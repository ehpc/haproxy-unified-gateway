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
package api

import (
	"context"
	"log/slog"

	parser "github.com/haproxytech/client-native/v6/config-parser"
	"github.com/haproxytech/client-native/v6/models"
	"github.com/haproxytech/haproxy-unified-gateway/hug/reload"
	"github.com/haproxytech/haproxy-unified-gateway/k8s/gate/logging"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func (c *clientNative) BackendCreate(backend models.Backend) error {
	configuration, err := c.nativeAPI.Configuration()
	if err != nil {
		return err
	}
	b := &models.Backend{BackendBase: backend.BackendBase}
	errCreate := configuration.CreateBackend(b, c.activeTransaction, 0)
	if errCreate != nil {
		// ... maybe it's already existing, so just edit it.
		if err := configuration.EditBackend(backend.Name, &backend, c.activeTransaction, 0); err != nil {
			c.logger.LogAttrs(context.Background(), slog.LevelError, "failed to edit backend",
				logging.LogAttrError(err),
				slog.String("backend", backend.Name),
			)
			return err
		}
	}
	reload.Instance().SetReload("Backend upserted %s", backend.Name)

	// ACLs
	err = c.ACLReplaceAll(parser.Backends, backend.Name, backend.ACLList)
	if err != nil {
		return err
	}

	// Http Requests
	err = c.HTTPRequestReplaceAll(parser.Backends, backend.Name, backend.HTTPRequestRuleList)
	if err != nil {
		return err
	}

	// Servers
	err = c.ServerReplaceAll(parser.Backends, backend.Name, backend.Servers)
	return err
}

func (c *clientNative) BackendDelete(backendName string) error {
	configuration, err := c.nativeAPI.Configuration()
	if err != nil {
		return err
	}
	reload.Instance().SetReload("Backend deleted %s", backendName)
	return configuration.DeleteBackend(backendName, c.activeTransaction, 0)
}

func (c *clientNative) BackendsGet() (models.Backends, error) {
	configuration, err := c.nativeAPI.Configuration()
	if err != nil {
		return nil, err
	}
	// TODO: complete with children
	_, backends, err := configuration.GetBackends(c.activeTransaction)

	// ACLS
	for _, backend := range backends {
		_, acls, err := configuration.GetACLs(string(parser.Backends), backend.Name, c.activeTransaction)
		if err != nil {
			return nil, err
		}
		backend.ACLList = append(backend.ACLList, acls...)
	}

	// HTTPRequest
	for _, backend := range backends {
		_, httpRequests, err := configuration.GetHTTPRequestRules(string(parser.Backends), backend.Name, c.activeTransaction)
		if err != nil {
			return nil, err
		}
		backend.HTTPRequestRuleList = append(backend.HTTPRequestRuleList, httpRequests...)
	}

	// Servers
	for _, backend := range backends {
		_, servers, err := configuration.GetServers(string(parser.Backends), backend.Name, c.activeTransaction)
		if err != nil {
			return nil, err
		}
		if len(servers) != 0 {
			backend.Servers = make(map[string]models.Server)
		}
		for _, server := range servers {
			if server != nil {
				backend.Servers[server.Name] = *server
			}
		}
	}

	return backends, err
}

func (c *clientNative) BackendGet(backendName string) (models.Backend, error) {
	configuration, err := c.nativeAPI.Configuration()
	if err != nil {
		return models.Backend{}, err
	}
	// TODO: complete with children
	_, backend, err := configuration.GetBackend(backendName, c.activeTransaction)
	if err != nil {
		return models.Backend{}, err
	}

	// ACLS
	_, acls, err := configuration.GetACLs(string(parser.Backends), backend.Name, c.activeTransaction)
	if err != nil {
		return models.Backend{}, err
	}
	backend.ACLList = append(backend.ACLList, acls...)

	// HTTPRequest
	_, httpRequests, err := configuration.GetHTTPRequestRules(string(parser.Backends), backend.Name, c.activeTransaction)
	if err != nil {
		return models.Backend{}, err
	}
	backend.HTTPRequestRuleList = append(backend.HTTPRequestRuleList, httpRequests...)

	// Servers
	_, servers, err := configuration.GetServers(string(parser.Backends), backend.Name, c.activeTransaction)
	if err != nil {
		return models.Backend{}, err
	}
	if len(servers) != 0 {
		backend.Servers = make(map[string]models.Server)
	}
	for _, server := range servers {
		if server != nil {
			backend.Servers[server.Name] = *server
		}
	}

	return *backend, err
}

func (c *clientNative) BackendEdit(backend models.Backend) error {
	configuration, err := c.nativeAPI.Configuration()
	if err != nil {
		return err
	}
	b := &models.Backend{BackendBase: backend.BackendBase}
	previousBackend, err := c.BackendGet(backend.Name)
	if err != nil {
		return err
	}

	// Check if only Servers were updated
	onlyServersUpdated := false
	if cmp.Equal(previousBackend, backend, cmpopts.IgnoreFields(models.Backend{}, "Servers")) {
		c.logger.LogAttrs(context.Background(), slog.LevelInfo, "Only Servers are updated",
			slog.String("backend", backend.Name),
		)
		onlyServersUpdated = true
	}

	if err := configuration.EditBackend(backend.Name, b, c.activeTransaction, 0); err != nil {
		c.logger.LogAttrs(context.Background(), slog.LevelError, "Failed to edit backend",
			logging.LogAttrError(err),
			slog.String("backend", backend.Name),
		)
		return err
	}

	// ACLs
	err = c.ACLReplaceAll(parser.Backends, backend.Name, backend.ACLList)
	if err != nil {
		return err
	}

	// Http Requests
	err = c.HTTPRequestReplaceAll(parser.Backends, backend.Name, backend.HTTPRequestRuleList)
	if err != nil {
		return err
	}

	// Servers only updated
	// Did we try runtime updates ? (server state update)
	if onlyServersUpdated {
		if reload.Instance().DynamicUpdateServerStateAttempted() {
			// Yes we did perform runtime update of server states
			if reload.Instance().DynamicUpdateServerStateFailed() {
				// If failed, we need to reload
				reload.Instance().SetReload("[onlyServersUpdated] [runtime] servers state update failure - backend %s", backend.Name)
			} else {
				c.logger.LogAttrs(context.Background(), slog.LevelDebug, "[onlyServersUpdated] [runtime] success",
					slog.String("backend", backend.Name),
				)
			}
		} else {
			// We did not try runtime update (server create, for now is NOT done through runtime, it needs a reload)
			reload.Instance().SetReload("[onlyServersUpdated] [reload needed] - backend %s", backend.Name)
		}
	} else {
		reload.Instance().SetReload("Backend upserted %s", backend.Name)
	}
	// Servers
	err = c.ServerReplaceAll(parser.Backends, backend.Name, backend.Servers)
	return err
}
