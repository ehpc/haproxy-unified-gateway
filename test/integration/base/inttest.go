//
// Copyright 2025 HAProxy Technologies LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package base

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/haproxytech/client-native/v6/runtime"
	v3 "github.com/haproxytech/haproxy-unified-gateway/api/gate/v3"
	"github.com/haproxytech/haproxy-unified-gateway/cmd/start"
	defaults "github.com/haproxytech/haproxy-unified-gateway/fs/usr/local/hug"
	haproxymgr "github.com/haproxytech/haproxy-unified-gateway/hug/haproxy"
	hapapi "github.com/haproxytech/haproxy-unified-gateway/hug/haproxy/api"
	haproxyparams "github.com/haproxytech/haproxy-unified-gateway/hug/haproxy/params"
	"github.com/haproxytech/haproxy-unified-gateway/hug/haproxy/process"
	gate "github.com/haproxytech/haproxy-unified-gateway/k8s/gate"
	"github.com/haproxytech/haproxy-unified-gateway/k8s/gate/config"
	"github.com/haproxytech/haproxy-unified-gateway/test/integration/utils"

	"github.com/go-logr/logr"
	"github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	discoveryV1 "k8s.io/api/discovery/v1"
	apiext "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/kubectl/pkg/scheme"
	ctrlruntime "sigs.k8s.io/controller-runtime"
	ctrlruntimeclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

func init() {
	utilruntime.Must(v3.AddToScheme(scheme.Scheme))
	utilruntime.Must(v1.AddToScheme(scheme.Scheme))
	utilruntime.Must(discoveryV1.AddToScheme(scheme.Scheme))
	utilruntime.Must(apiext.AddToScheme(scheme.Scheme))
	utilruntime.Must(appsv1.AddToScheme(scheme.Scheme))
	utilruntime.Must(gatewayv1.Install(scheme.Scheme))
}

const (
	controllerNs = "haproxy-controller"
)

var hugConfNsName = types.NamespacedName{
	Namespace: "test",
	Name:      "hugconf",
}

type IntTest struct {
	Ctx           context.Context
	Client        ctrlruntimeclient.Client
	RuntimeClient runtime.Runtime
	HaproxyClient hapapi.HAProxyClient
	TestEnv       *envtest.Environment
	cancel        context.CancelFunc
	Namespace     string
}

func NewIntTest(t *testing.T) (test IntTest, err error) {
	ctx, cancel := context.WithCancel(context.Background())
	g := gomega.NewWithT(t)

	// Namespace
	namespace, err := utils.GetIntTestNamespace()
	g.Expect(err).ToNot(gomega.HaveOccurred())

	testEnvVersion := os.Getenv("ENVTEST_VERSION")
	installPath := os.Getenv("KUBEBUILDER_ASSETS")

	testEnv := &envtest.Environment{
		CRDDirectoryPaths: []string{
			"../../../api/definition",
			"../api",
		},
		ErrorIfCRDPathMissing:       true,
		DownloadBinaryAssets:        true,
		DownloadBinaryAssetsVersion: testEnvVersion,
		BinaryAssetsDirectory:       installPath,
	}

	test = IntTest{
		Ctx:       ctx,
		cancel:    cancel,
		TestEnv:   testEnv,
		Namespace: namespace,
	}

	return test, nil
}

func (test *IntTest) StartTestEnv(t *testing.T) { //revive:disable:function-length
	// Bootstrapping test environment.
	cfg, err := test.TestEnv.Start()
	g := gomega.NewWithT(t)

	g.Expect(err).ToNot(gomega.HaveOccurred())
	g.Expect(cfg).ToNot(gomega.BeNil())
	// metricsConfig := config.MetricsConfig{
	// 	Port:    6062,
	// 	Enabled: false,
	// 	Secure:  false,
	// }
	kubeconfigPath, err := WriteKubeconfig(cfg)
	g.Expect(err).ToNot(gomega.HaveOccurred())
	t.Logf("kubeconfig path: %s", kubeconfigPath)

	// Controller HUGConfig
	hugConfig := hugConfig(test)

	// Ensure route.lua is present in the test HAProxy cfg dir so HAProxy can load it when
	// the controller emits `lua-load-per-thread route.lua` / `http-request lua.route`.
	// We embed the repository copy of route.lua into the test binary so tests don't
	// rely on the process working directory or external files. In CI the HAProxy process
	// may be started with a different working directory, so we also rewrite the
	// embedded haproxy.cfg to reference the absolute path to the copied route.lua file.
	dstRoute := filepath.Join(hugConfig.HaproxyDirs.CfgDir, "route.lua")
	if _, err := os.Stat(dstRoute); os.IsNotExist(err) {
		err := os.WriteFile(dstRoute, []byte(defaults.RouteLua), 0o644)
		g.Expect(err).ToNot(gomega.HaveOccurred())
	}

	// Rewrite the embedded initial HAProxy config so lua-load-per-thread references
	// the absolute path to the copied route.lua file. This guarantees HAProxy can
	// open the file regardless of the process working directory in CI.
	modifiedCfg := defaults.HaproxyCfg
	// Replace the simple filename directive if present.
	if strings.Contains(modifiedCfg, "lua-load-per-thread route.lua") {
		modifiedCfg = strings.ReplaceAll(modifiedCfg, "lua-load-per-thread route.lua", fmt.Sprintf("lua-load-per-thread %s", dstRoute))
	}
	if strings.Contains(modifiedCfg, "/var/run/haproxy-runtime-api.sock") {
		modifiedCfg = strings.ReplaceAll(modifiedCfg, "/var/run/haproxy-runtime-api.sock", hugConfig.HaproxyDirs.RuntimeSocket)
	}
	if strings.Contains(modifiedCfg, "/var/run/haproxy.pid") {
		modifiedCfg = strings.ReplaceAll(modifiedCfg, "/var/run/haproxy.pid", hugConfig.HaproxyDirs.PIDFile)
	}
	if strings.Contains(modifiedCfg, "/var/run/haproxy/health.sock") {
		dir := path.Join(os.TempDir(), "hug")
		err = os.MkdirAll(dir, 0o755)
		g.Expect(err).ToNot(gomega.HaveOccurred())
		modifiedCfg = strings.ReplaceAll(modifiedCfg, "/var/run/haproxy/health.sock", path.Join(dir, "health.sock"))
	}
	err = writeInitialHaproxyCfg(hugConfig.HaproxyDirs.MainCfgFile, modifiedCfg)
	g.Expect(err).ToNot(gomega.HaveOccurred())

	// Setup Gate lib configuration from HUG binary configuration
	opts := start.SetupGateConfig(hugConfig)

	mgr, err := ctrlruntime.NewManager(cfg, ctrlruntime.Options{
		Scheme:  scheme.Scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
	})
	g.Expect(err).ToNot(gomega.HaveOccurred())

	client, err := ctrlruntimeclient.New(cfg, ctrlruntimeclient.Options{Scheme: scheme.Scheme})
	test.Client = client
	g.Expect(err).ToNot(gomega.HaveOccurred())

	// Create the test Namespace
	err = test.createNamespace(test.Namespace)
	g.Expect(err).ToNot(gomega.HaveOccurred())

	// Create controller namespace.
	err = test.createNamespace(controllerNs)
	g.Expect(err).ToNot(gomega.HaveOccurred())

	gateconfig := config.Configuration{}
	gateconfig.ApplyDefaults()

	for _, o := range opts {
		_ = o(&gateconfig)
	}
	logrLoggerFromSlog := logr.FromSlogHandler(gateconfig.LogHandler)
	ctrlruntime.SetLogger(logrLoggerFromSlog)

	// find and kill any running haproxy
	test.killAnyRunningHaproxy(t, gateconfig.HaproxyParams.HaproxyBinary)
	// // ----------------
	// // Start Haproxy App manager
	var wg sync.WaitGroup

	haproxyClient, err := hapapi.New(gateconfig.Logger, gateconfig.HaproxyParams.CfgDir,
		gateconfig.HaproxyParams.MainCfgFile, gateconfig.HaproxyParams.HaproxyBinary, gateconfig.HaproxyParams.RuntimeSocket)
	if err != nil {
		err = fmt.Errorf("failed to initialize haproxy API client: %w", err)
		panic(err)
	}
	params := haproxyparams.Params{
		Test:             hugConfig.Test,
		UseWiths6Overlay: hugConfig.UseWiths6Overlay,
		UseWithPebble:    hugConfig.UseWithPebble,
		HaproxyDirs:      hugConfig.HaproxyDirs,
	}
	p := process.New(params, haproxyClient, gateconfig.Logger)
	p.SetAPI(haproxyClient)

	// // ----------------
	// // Start Haproxy App manager
	haproxyAppManager, err := haproxymgr.NewAppManager(test.Ctx, &wg,
		gateconfig.TransferHaproxyConfChannel,
		// runtimeClientCh,
		haproxyClient, p,
		params,
		gateconfig.Logger)
	if err != nil {
		panic(err)
	}

	// haproxyAppManager, runtimeClient := start.NewAppManager(test.Ctx, &wg, gatecontrollercfg, hugConfig)
	test.RuntimeClient = haproxyClient.RuntimeClient()

	haproxyAppManager.Run()
	test.HaproxyClient = haproxyAppManager.HaproxyClient()

	err = gate.Add(test.Ctx, gateconfig, haproxyClient, mgr)
	g.Expect(err).ToNot(gomega.HaveOccurred())

	go func() {
		if err := mgr.Start(test.Ctx); err != nil {
			t.Errorf("failed to start manager: %s", err)
			return
		}
	}()
}

func (test *IntTest) StopTestEnv(t *testing.T) {
	g := gomega.NewWithT(t)

	// delete test Namespace
	err := test.cleanupNamespace(test.Namespace)
	g.Expect(err).ToNot(gomega.HaveOccurred())

	// delete the controller namespace
	err = test.cleanupNamespace(controllerNs)
	g.Expect(err).ToNot(gomega.HaveOccurred())

	// Clean up and stop controller.
	test.cancel()

	// Tearing down the test environment.
	if err := test.TestEnv.Stop(); err != nil {
		t.Fatalf("failed to stop testEnv: %s", err)
	}
}

func (test *IntTest) killAnyRunningHaproxy(t *testing.T, haproxyBinary string) {
	g := gomega.NewWithT(t)
	if !utils.WaitFor(test.Ctx, interval, timeout, func() bool {
		pid, err := findPID(haproxyBinary, "tmp/hug")
		if err == nil {
			p, errF := os.FindProcess(pid)
			g.Expect(errF).ToNot(gomega.HaveOccurred())

			errS := p.Signal(syscall.SIGKILL)
			g.Expect(errS).ToNot(gomega.HaveOccurred())
			return false
		} else {
			return true
		}
	}) {
		t.Fatal("could not stop haproxy")
	}
}

func (test *IntTest) createNamespace(ns string) error {
	err := utils.CreateRuntimeObject(test.Ctx, test.Client, &v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: ns,
		},
	}, true)
	return err
}

func (test *IntTest) cleanupNamespace(ns string) error {
	err := utils.DeleteRuntimeObject(test.Ctx, test.Client, &v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: ns,
		},
	}, true)
	return err
}

// WriteKubeconfig writes the given rest.Config to a kubeconfig file.
// It returns the path to the file and an error if it fails.
func WriteKubeconfig(cfg *rest.Config) (string, error) {
	// Create a clientcmdapi.Config object from the rest.Config
	clusters := make(map[string]*api.Cluster)
	clusters["envtest-cluster"] = &api.Cluster{
		Server:                   cfg.Host,
		CertificateAuthorityData: cfg.CAData,
	}

	authInfos := make(map[string]*api.AuthInfo)
	authInfos["envtest-user"] = &api.AuthInfo{
		ClientCertificateData: cfg.CertData,
		ClientKeyData:         cfg.KeyData,
	}

	contexts := make(map[string]*api.Context)
	contexts["envtest-context"] = &api.Context{
		Cluster:   "envtest-cluster",
		AuthInfo:  "envtest-user",
		Namespace: "default", // or whatever namespace you're testing in
	}

	kubeconfig := &api.Config{
		Kind:           "Config",
		APIVersion:     "v1",
		Clusters:       clusters,
		AuthInfos:      authInfos,
		Contexts:       contexts,
		CurrentContext: "envtest-context",
	}

	// Create a temporary file to write the kubeconfig to
	kubeconfigPath := filepath.Join(os.TempDir(), "kubeconfig")
	if err := clientcmd.WriteToFile(*kubeconfig, kubeconfigPath); err != nil {
		return "", fmt.Errorf("failed to write kubeconfig file: %w", err)
	}
	return kubeconfigPath, nil
}

// writeInitialHaproxyCfg writes a string to a file at the specified path.
func writeInitialHaproxyCfg(dstFile, content string) error {
	// Use os.WriteFile which is a convenience function
	// to write a byte slice to a file. It handles opening, writing, and closing.
	// We convert the string to a byte slice.
	err := os.WriteFile(dstFile, []byte(content), 0o644)
	if err != nil {
		return fmt.Errorf("could not write string to file: %w", err)
	}
	return nil
}

// findPID finds the PID of a process that matches the given filters.
func findPID(processName, filterArg string) (int, error) {
	// Use pgrep with the -a flag to list the full command line of processes.
	cmd := exec.Command("pgrep", "-af", processName)
	var out bytes.Buffer
	cmd.Stdout = &out

	if err := cmd.Run(); err != nil {
		// pgrep returns an error if no process is found, which is a normal case.
		// We'll return a more specific message if the command itself fails.
		if _, ok := err.(*exec.ExitError); ok {
			return 0, fmt.Errorf("no process found matching '%s'", processName)
		}
		return 0, fmt.Errorf("failed to run pgrep: %w", err)
	}

	// Split the output into lines to process each process entry.
	lines := strings.Split(out.String(), "\n")

	// Iterate over each line and apply the additional filter.
	for _, line := range lines {
		if strings.Contains(line, filterArg) {
			// Found a matching line. Now, extract the PID (the first word).
			fields := strings.Fields(line)
			if len(fields) > 0 {
				pid, err := strconv.Atoi(fields[0])
				if err != nil {
					return 0, fmt.Errorf("failed to parse PID from line '%s': %w", line, err)
				}
				// Return the first matching PID found.
				return pid, nil
			}
		}
	}

	return 0, fmt.Errorf("no process found matching both '%s' and '%s'", processName, filterArg)
}
