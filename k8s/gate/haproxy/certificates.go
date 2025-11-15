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
package haproxy

import (
	"context"
	"log/slog"
	"strings"

	"github.com/haproxytech/client-native/v6/models"
	"github.com/haproxytech/haproxy-unified-gateway/hug/reload"
	"github.com/haproxytech/haproxy-unified-gateway/k8s/gate/haproxy/certificate"
	"github.com/haproxytech/haproxy-unified-gateway/k8s/gate/logging"
	"github.com/haproxytech/haproxy-unified-gateway/k8s/gate/utils"
)

func (b *HaproxyConfMgrImpl) processCertificates() error {
	if !b.params.RuntimeUpdateHaproxy {
		return nil
	}
	b.runtimeCertificatesPrechecks()

	err := b.executeRuntimeCertCommands()
	return err
}

// runtimeCertificatesPrechecks performs needed pre-checks for runtime update
// For example, if crt-list need to be created or deleted, it can not be done through runtime socket
func (b *HaproxyConfMgrImpl) runtimeCertificatesPrechecks() {
	crtListUpdates := b.controllerStore.CrtListUpdates
	// Crt-list can not be created or deleted through runtime
	// So, if there are any created or deleted crt-lists, we need to reload.
	// No further runtime update needed.
	if len(crtListUpdates.Created) > 0 {
		buffc := strings.Builder{}
		for c := range crtListUpdates.Created {
			buffc.WriteString(c)
			buffc.WriteString(",")
		}
		reload.Instance().SetReload("crt-list are created %s", buffc.String())
	}
	if len(crtListUpdates.Deleted) > 0 {
		buffd := strings.Builder{}
		for d := range crtListUpdates.Deleted {
			buffd.WriteString(d)
			buffd.WriteString(",")
		}
		reload.Instance().SetReload("crt-list are deleted %s", buffd.String())
	}
}

func (b *HaproxyConfMgrImpl) executeRuntimeCertCommands() error {
	if reload.Instance().NeedReload() {
		return nil
	}
	certUpdates := b.controllerStore.CertUpdates

	// 1- Process Cert create and update
	for _, cert := range certUpdates.Created {
		if err := b.runtimeCreateCert(cert); err != nil {
			reload.Instance().SetReload("runtime cert create failed")
			return err
		}
	}
	for _, cert := range certUpdates.Updated {
		if err := b.runtimeUpdateCert(cert); err != nil {
			reload.Instance().SetReload("runtime cert update failed")
			return err
		}
	}

	// 2- Process crt-list create + delete: no, this can not be done dynamically

	// 3- Process crt-list update - This must be before cert delete as we can not delete from crt-list a cert that is still used in a crt-list
	for _, crtList := range b.controllerStore.CrtListUpdates.Updated {
		if err := b.runtimeUpdateCrtList(crtList); err != nil {
			reload.Instance().SetReload("runtime crt-list failed")
			return err
		}
	}
	// 4 - Finally cert delete
	for _, cert := range certUpdates.Deleted {
		if err := b.runtimeDeleteCert(cert); err != nil {
			reload.Instance().SetReload("runtime cert delete failed")
			return err
		}
	}

	return nil
}

func (b *HaproxyConfMgrImpl) runtimeCreateCert(cert certificate.CertificateData) error {
	// Only 1 transaction in parallel is possible for now in haproxy
	// Keep this mutex for now to ensure that we perform 1 transaction at a time
	b.mu.Lock()
	defer b.mu.Unlock()

	var err error
	runtimeClient := b.haproxyClient.RuntimeClient()

	certName := cert.Path.FullPath()
	err = runtimeClient.NewCertEntry(certName)
	// If already exists
	if err != nil {
		// if !strings.Contains(err.Error(), "already exists") {
		return err
	}
	b.logger.LogAttrs(context.Background(), slog.LevelDebug, "ok: `new ssl cert`", slog.String("cert", certName))

	err = runtimeClient.SetCertEntry(certName, string(cert.Data))
	if err != nil {
		return err
	}
	b.logger.LogAttrs(context.Background(), slog.LevelDebug, "ok: `set ssl cert`", slog.String("cert", certName))

	err = runtimeClient.CommitCertEntry(certName)
	if err != nil {
		// Abort transaction
		errAbort := runtimeClient.AbortCertEntry(certName)
		// If error, just log it
		// a Reload will follow, transaction will be gone no matter what
		if errAbort != nil {
			b.logger.LogAttrs(context.Background(), slog.LevelError, "failed to abort transaction", logging.LogAttrError(errAbort))
		}
		return err
	}
	b.logger.LogAttrs(context.Background(), slog.LevelDebug, "ok: `commit ssl cert`", slog.String("cert", certName))

	return nil
}

func (b *HaproxyConfMgrImpl) runtimeUpdateCert(cert certificate.CertificateData) error {
	// Only 1 transaction in parallel is possible for now in haproxy
	// Keep this mutex for now to ensure that we perform 1 transaction at a time
	b.mu.Lock()
	defer b.mu.Unlock()

	var err error
	runtimeClient := b.haproxyClient.RuntimeClient()

	certName := cert.Path.FullPath()

	err = runtimeClient.SetCertEntry(certName, string(cert.Data))
	if err != nil {
		return err
	}
	b.logger.LogAttrs(context.Background(), slog.LevelDebug, "ok: `set ssl cert`", slog.String("cert", certName))

	err = runtimeClient.CommitCertEntry(certName)
	if err != nil {
		// Abort transaction
		errAbort := runtimeClient.AbortCertEntry(certName)
		// If error, just log it
		// a Reload will follow, transaction will be gone no matter what
		if errAbort != nil {
			b.logger.LogAttrs(context.Background(), slog.LevelError, "failed to abort transaction", logging.LogAttrError(errAbort))
		}
		return err
	}
	b.logger.LogAttrs(context.Background(), slog.LevelDebug, "ok: `commit ssl cert`", slog.String("cert", certName))

	return nil
}

func (b *HaproxyConfMgrImpl) runtimeDeleteCert(cert certificate.CertificateData) error {
	// Only 1 transaction in parallel is possible for now in haproxy
	// Keep this mutex for now to ensure that we perform 1 transaction at a time
	b.mu.Lock()
	defer b.mu.Unlock()

	var err error
	runtimeClient := b.haproxyClient.RuntimeClient()

	certName := cert.Path.FullPath()

	err = runtimeClient.DeleteCertEntry(certName)
	if err != nil {
		return err
	}
	b.logger.LogAttrs(context.Background(), slog.LevelDebug, "ok: `del ssl cert`", slog.String("cert", certName))

	return nil
}

func (b *HaproxyConfMgrImpl) runtimeUpdateCrtList(crtList certificate.CrtListData) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	var err error
	runtimeClient := b.haproxyClient.RuntimeClient()

	// First read the content of the existing crt-list
	crtListEntries, err := runtimeClient.ShowCrtListEntries(crtList.Path.FullPath())
	if err != nil {
		return err
	}

	currentCerts := make(map[string]struct{})
	for _, crtListEntry := range crtListEntries {
		currentCerts[crtListEntry.File] = struct{}{}
	}

	newContent := make(map[string]struct{})
	for _, cert := range crtList.Content {
		newContent[cert] = struct{}{}
	}

	addedCerts := utils.SetDifference(newContent, currentCerts)
	deletedCerts := utils.SetDifference(currentCerts, newContent)

	for cert := range addedCerts {
		err = runtimeClient.AddCrtListEntry(crtList.Path.FullPath(), models.SslCrtListEntry{
			File: cert,
		})
		if err != nil {
			return err
		}
		b.logger.LogAttrs(context.Background(), slog.LevelDebug, "ok: `add ssl crt-list`",
			slog.String("crt-list", crtList.Path.FullPath()), slog.String("cert", cert))
	}
	for cert := range deletedCerts {
		err = runtimeClient.DeleteCrtListEntry(crtList.Path.FullPath(), cert, nil)
		if err != nil {
			return err
		}
		b.logger.LogAttrs(context.Background(), slog.LevelDebug, "ok: `del ssl crt-list`",
			slog.String("crt-list", crtList.Path.FullPath()), slog.String("cert", cert))
	}
	return nil
}
