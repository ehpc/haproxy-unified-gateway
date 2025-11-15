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
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/haproxytech/client-native/v6/models"
	"github.com/haproxytech/haproxy-unified-gateway/k8s/gate/haproxy/diffs"
	"github.com/haproxytech/haproxy-unified-gateway/k8s/gate/haproxy/metadata"
	"github.com/haproxytech/haproxy-unified-gateway/k8s/gate/haproxy/structured"
	"github.com/haproxytech/haproxy-unified-gateway/k8s/gate/logging"
	"github.com/haproxytech/haproxy-unified-gateway/k8s/gate/utils"
)

type Configuration struct {
	// structured contains the complete Structured configuration
	structured structured.Structured
	diffs      diffs.HaproxyConfDiffs
}

func (c *Configuration) resetDiffs() {
	c.diffs = diffs.HaproxyConfDiffs{
		Created: structured.NewStructuredConf(),
		Updated: structured.NewStructuredConf(),
		Deleted: structured.NewStructuredConf(),
	}
}

func (c *Configuration) upsertFrontend(logger *slog.Logger, fe *models.Frontend) error {
	if fe == nil {
		logger.LogAttrs(context.Background(), slog.LevelError, "nil frontend")
		return errors.New("nil frontend")
	}

	if previousFe, ok := c.structured.Frontends[fe.Name]; ok {
		// Check if they are the same
		if previousFe.Equal(*fe) {
			logger.LogAttrs(context.Background(), slog.LevelDebug, "Frontend [same]",
				logging.LogAttrFrontendName(fe.Name),
			)
			return nil
		}

		// Update existing frontend
		logger.LogAttrs(context.Background(), slog.LevelInfo, "Frontend [UPDATE]",
			logging.LogAttrFrontendName(fe.Name),
		)

		// We need to deep copy the frontend to avoid modifying the original
		// as the diffs will be sent on a channel and used at the same time we continue to update the haproxy cfg store.
		deepCopied, err := DeepCopyFrontend(fe)
		if err != nil {
			return err
		}

		c.diffs.Updated.Frontends[fe.Name] = deepCopied
		c.structured.Frontends[fe.Name] = fe
	} else {
		// Create new frontend
		logger.LogAttrs(context.Background(), slog.LevelInfo, "Frontend [CREATE]",
			logging.LogAttrFrontendName(fe.Name),
		)

		// We need to deep copy the frontend to avoid modifying the original
		// as the diffs will be sent on a channel and used at the same time we continue to update the haproxy cfg store.
		deepCopied, err := DeepCopyFrontend(fe)
		if err != nil {
			return err
		}

		c.structured.Frontends[fe.Name] = deepCopied
		c.diffs.Created.Frontends[fe.Name] = deepCopied
	}
	return nil
}

func (c *Configuration) deleteFrontend(logger *slog.Logger, feName string) error {
	// Retrieve the frontend from the store
	fe, ok := c.structured.Frontends[feName]
	if !ok {
		// It could happen that the frontend was already deleted
		// like gateway is:
		// - first unmanaged: Frontend is not created, not in store
		// - then deleted: Frontend is deleted, not in store
		return nil
	}
	// delete the frontend from the store
	logger.LogAttrs(context.Background(), slog.LevelInfo, "Frontend [DELETE]",
		logging.LogAttrFrontendName(feName),
	)
	// We need to deep copy the frontend to avoid modifying the original
	// as the diffs will be sent on a channel and used at the same time we continue to update the haproxy cfg store.

	c.diffs.Deleted.Frontends[fe.Name] = nil
	delete(c.structured.Frontends, feName)
	return nil
}

func (c *Configuration) upsertBackend(logger *slog.Logger, be *models.Backend) error {
	if be == nil {
		logger.LogAttrs(context.Background(), slog.LevelError, "nil backend")
		return errors.New("nil backend")
	}

	if previousBe, ok := c.structured.Backends[be.Name]; ok {
		// Check if they are the same
		// Here, the input parameter be has no Servers, it's only the Backend without Servers that we want to compare
		// Even though Servers are part of the structured Backend, they are managed differently from Backend fields.
		if cmp.Equal(*previousBe, *be, cmpopts.IgnoreFields(models.Backend{}, "Servers")) {
			logger.LogAttrs(context.Background(), slog.LevelDebug, "Backend [same]",
				logging.LogAttrBackendName(be.Name),
			)
			return nil
		}
		be.Servers = previousBe.Servers

		// Update existing backend
		logger.LogAttrs(context.Background(), slog.LevelInfo, "Backend [UPDATE]",
			logging.LogAttrBackendName(be.Name),
		)

		// We need to deep copy the backend to avoid modifying the original
		// as the diffs will be sent on a channel and used at the same time we continue to update the haproxy cfg store.
		deepCopied, err := DeepCopyBackend(be)
		if err != nil {
			return err
		}

		c.diffs.Updated.Backends[be.Name] = deepCopied
		c.structured.Backends[be.Name] = be
	} else {
		// Create new backend
		logger.LogAttrs(context.Background(), slog.LevelInfo, "Backend [CREATE]",
			logging.LogAttrBackendName(be.Name),
		)

		// We need to deep copy the backend to avoid modifying the original
		// as the diffs will be sent on a channel and used at the same time we continue to update the haproxy cfg store.
		deepCopied, err := DeepCopyBackend(be)
		if err != nil {
			return err
		}

		c.structured.Backends[be.Name] = deepCopied
		c.diffs.Created.Backends[be.Name] = deepCopied
	}
	return nil
}

func (c *Configuration) upsertBackendWithServers(logger *slog.Logger, beName string, servers map[string]models.Server) ([]RuntimeServerStateData, error) {
	be, ok := c.structured.Backends[beName]
	if !ok {
		return nil, fmt.Errorf("could not find backend %s", beName)
	}

	beBackup, err := DeepCopyBackend(be)
	if err != nil {
		return nil, err
	}
	be.Servers = servers
	// 1- Same servers
	// TODO : use models.EqualMapStringServer
	if cmp.Equal(beBackup.Servers, be.Servers) {
		// if beBackup.Equal(*be) {
		logger.LogAttrs(context.Background(), slog.LevelDebug, "Backend [servers][same]",
			logging.LogAttrBackendName(be.Name),
		)
		return nil, nil
	}

	// 2- Servers differ
	// Update existing backend
	logger.LogAttrs(context.Background(), slog.LevelInfo, "Backend [servers][UPDATE]",
		logging.LogAttrBackendName(be.Name),
	)

	// Compute list of delete servers to update them through runtime = to set to MAINT
	deletedServerNames := utils.SetDifference(beBackup.Servers, be.Servers)

	// 2.1- Update the new Servers in Backend, so it will be written in the configuration
	c.diffs.Updated.Backends[be.Name] = be
	c.structured.Backends[be.Name] = be

	// 2.2- If some servers were deleted, set the server to Maintenance through runtime
	// TODO: we also need to check in HUG when we update the configuration that if BE differ only by deleted servers...
	runtimeServerStateData := make([]RuntimeServerStateData, 0)
	for serverName := range deletedServerNames {
		runtimeServerStateData = append(runtimeServerStateData, RuntimeServerStateData{
			BackendName: be.Name,
			ServerName:  serverName,
			IP:          "127.0.0.1",
			Port:        1,
			State:       "maint",
		})
	}

	return runtimeServerStateData, nil
}

func (c *Configuration) upsertBackendMetadata(logger *slog.Logger, beName string, md metadata.MetaData) error {
	if previousBe, ok := c.structured.Backends[beName]; ok {
		// Update existing backend
		logger.LogAttrs(context.Background(), slog.LevelInfo, "Backend [UPDATE_METADATA]",
			logging.LogAttrBackendName(beName),
		)

		previousBe.Metadata = md

		// We need to deep copy the backend to avoid modifying the original
		// as the diffs will be sent on a channel and used at the same time we continue to update the haproxy cfg store.
		deepCopied, err := DeepCopyBackend(previousBe)
		if err != nil {
			return err
		}

		c.diffs.Updated.Backends[beName] = deepCopied
		c.structured.Backends[beName] = previousBe
	}
	return nil
}

func (c *Configuration) deleteBackend(logger *slog.Logger, beName string) error {
	// Retrieve the backend from the store
	be, ok := c.structured.Backends[beName]
	if !ok {
		// It could happen that the backend was already deleted
		return nil
	}
	// delete the backend from the store
	logger.LogAttrs(context.Background(), slog.LevelInfo, "Backend [DELETE]",
		logging.LogAttrBackendName(beName),
	)
	// We need to deep copy the backend to avoid modifying the original
	// as the diffs will be sent on a channel and used at the same time we continue to update the haproxy cfg store.

	c.diffs.Deleted.Backends[be.Name] = nil
	delete(c.structured.Backends, beName)
	return nil
}
