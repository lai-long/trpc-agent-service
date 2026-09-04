package wecomws

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// Binding is the slice of a channel_binding row the wecomws channel serves.
// RoutesProvider projects tenant.Route down to it so this package stays
// decoupled from the tenant store.
type Binding struct {
	ID          string
	WebhookPath string
	Config      json.RawMessage
}

// RoutesProvider enumerates the bindings of one channel type that are
// currently servable (binding active + tenant active + app published), with
// the resolver's usual TTL / pub-sub invalidation semantics.
type RoutesProvider interface {
	RoutesByChannel(ctx context.Context, channel string) ([]Binding, error)
}

// bindingConfig is channel_binding.config for wecomws bindings — the first
// use of the jsonb config column: one bot per binding. The secret stays as a
// reference and is resolved through the SecretResolver at every (re)connect,
// so a rotation takes effect on the next reconnect.
type bindingConfig struct {
	BotID     string `json:"bot_id"`
	SecretRef string `json:"secret_ref"`
}

// parseBindingConfig enforces the two required fields; the admin API already
// refuses such rows up front, so a malformed row here means it was written
// out of band.
func parseBindingConfig(raw json.RawMessage) (bindingConfig, error) {
	var cfg bindingConfig
	if len(raw) == 0 {
		return cfg, errors.New("wecomws: binding config is empty")
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return cfg, fmt.Errorf("wecomws: parse binding config: %w", err)
	}
	if cfg.BotID == "" {
		return cfg, errors.New("wecomws: binding config misses bot_id")
	}
	if cfg.SecretRef == "" {
		return cfg, errors.New("wecomws: binding config misses secret_ref")
	}
	return cfg, nil
}
