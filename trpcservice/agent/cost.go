package agent

import "sync/atomic"

// Model pricing for cost accounting (audit_log.cost). The table maps a model
// name to USD per 1M tokens, [input, output]; it is process-wide static
// configuration set once at startup (env TRPC_MODEL_PRICES). Unknown models
// cost-track as 0 — token counts are always recorded, so cost can be
// re-derived offline when a price is missing.
var pricing atomic.Value // map[string][2]float64

// SetModelPricing installs the pricing table; call once at startup.
func SetModelPricing(table map[string][2]float64) {
	pricing.Store(table)
}

// CostUSD computes the cost of one run in USD.
func CostUSD(model string, promptTokens, completionTokens int) float64 {
	table, _ := pricing.Load().(map[string][2]float64)
	price, ok := table[model]
	if !ok {
		return 0
	}
	return (float64(promptTokens)*price[0] + float64(completionTokens)*price[1]) / 1e6
}
