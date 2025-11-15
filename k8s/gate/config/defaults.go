// Copyright 2025 HAProxy Technologies LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package config

import (
	"time"

	"github.com/haproxytech/haproxy-unified-gateway/k8s/gate/haproxy/storage"
	"github.com/haproxytech/haproxy-unified-gateway/k8s/gate/logging"
)

const (
	// defaultControllerName is the default name of the controller.
	defaultControllerName         = "gate.haproxy.org/hug"
	defaultLeaderElectionLockName = "hug-leader-election-lock"
	defaultSyncPeriod             = 5 * time.Second
	defaultFrontendNameTemplate   = "{{ .LINK_ID }}_{{ .GATEWAY_NAMESPACE}}_{{ .GATEWAY_NAME }}_{{ .LISTENER_NAME }}"
	defaultBackendNameTemplate    = "{{ .LINK_ID }}_{{ .SERVICE_NAMESPACE}}_{{ .SERVICE_NAME }}_{{ .SERVICE_PORT}}_{{ .FILTER_HASH}}"
	defaultServerNameTemplate     = "SRV_{{ .POD_IP_PORT_HASH}}"
	DefaultsSectionName           = "haproxytech"
	DefaultWaitForRuntimeTimeout  = 10 * time.Second
	DefaultCertsDirName           = "certs"
	DefaultCertFilesDirName       = "certlists"
	DefaultMapsDirName            = "maps"
	DefaultErrFilesDirName        = "errorfiles"
	DefaultPatternDirName         = "patterns"
)

func (cfg *Configuration) ApplyDefaults() {
	if cfg.LogHandlerType == "" {
		cfg.LogHandlerType = logging.LogHandlerTypeJSON
	}
	// Logging Defaults
	slogger, logHandler := NewBaseLogger(cfg.LogHandlerType, logging.DefaultLevel, logging.DefaultLogLevelPerCategory)
	cfg.Logger = slogger
	cfg.LogHandler = logHandler

	cfg.LeaderElectionConfig.LockName = defaultLeaderElectionLockName
	if cfg.ControllerName == "" {
		cfg.ControllerName = defaultControllerName
	}
	if cfg.SyncPeriod == 0 {
		cfg.SyncPeriod = defaultSyncPeriod
	}
	// StartupSyncPeriod if not defined is equal to SyncPeriod
	if cfg.StartupSyncPeriod == 0 {
		cfg.StartupSyncPeriod = cfg.SyncPeriod
	}
	if cfg.HaproxyParams.FrontendNameTemplate == "" {
		cfg.HaproxyParams.FrontendNameTemplate = defaultFrontendNameTemplate
	}
	if cfg.HaproxyParams.BackendNameTemplate == "" {
		cfg.HaproxyParams.BackendNameTemplate = defaultBackendNameTemplate
	}
	if cfg.HaproxyParams.ServerNameTemplate == "" {
		cfg.HaproxyParams.ServerNameTemplate = defaultServerNameTemplate
	}
	if cfg.HaproxyParams.LinkID == "" {
		cfg.HaproxyParams.LinkID = "linkid"
	}
	if cfg.HaproxyParams.TimeoutWaitForRuntime == 0 {
		cfg.HaproxyParams.TimeoutWaitForRuntime = DefaultWaitForRuntimeTimeout
	}
	if cfg.HaproxyParams.StoreCertificateStructureType == "" {
		cfg.HaproxyParams.StoreCertificateStructureType = storage.StructureTypeCertDefault
	}
	if cfg.HaproxyParams.StoreMapsStructureType == "" {
		cfg.HaproxyParams.StoreMapsStructureType = storage.StructureTypeMapsDefault
	}
	if cfg.HaproxyParams.HaproxyDirs.CertsDir == "" {
		cfg.HaproxyParams.HaproxyDirs.CertsDir = DefaultCertsDirName
	}
	if cfg.HaproxyParams.HaproxyDirs.CertListDir == "" {
		cfg.HaproxyParams.HaproxyDirs.CertListDir = DefaultCertFilesDirName
	}
	if cfg.HaproxyParams.HaproxyDirs.MapsDir == "" {
		cfg.HaproxyParams.HaproxyDirs.MapsDir = DefaultMapsDirName
	}
	if cfg.HaproxyParams.HaproxyDirs.ErrFileDir == "" {
		cfg.HaproxyParams.HaproxyDirs.ErrFileDir = DefaultErrFilesDirName
	}
	if cfg.HaproxyParams.HaproxyDirs.PatternDir == "" {
		cfg.HaproxyParams.HaproxyDirs.PatternDir = DefaultPatternDirName
	}
}
