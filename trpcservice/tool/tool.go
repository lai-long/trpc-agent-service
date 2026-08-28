// Package tool registers platform tools and tenant-scoped MCP / function tools.
package tool

import (
	"context"
	"fmt"

	ttool "trpc.group/trpc-go/trpc-agent-go/tool"
	"trpc.group/trpc-go/trpc-agent-go/tool/function"
)

// Tool wraps a framework tool with platform metadata.
type Tool struct {
	Tool ttool.Tool // the framework tool
	// Dangerous marks tools whose execution requires in-band user approval
	// (design 5.3.3): the guardrail intercepts the call and only releases it
	// after the user confirms in the same conversation.
	Dangerous bool
}

// Registry is the set of platform tools an agent may use. Tenant-level
// whitelisting (per tenant.tool_policy) filters this list per tenant once the
// Admin API lands; until then all tenants share the registry.
type Registry struct {
	tools map[string]Tool
}

// NewRegistry creates a Registry from the given tools.
func NewRegistry(tools ...Tool) *Registry {
	r := &Registry{tools: make(map[string]Tool, len(tools))}
	for _, t := range tools {
		r.tools[t.Tool.Declaration().Name] = t
	}
	return r
}

// All returns the framework tools, for llmagent.WithTools.
func (r *Registry) All() []ttool.Tool {
	out := make([]ttool.Tool, 0, len(r.tools))
	for _, t := range r.tools {
		out = append(out, t.Tool)
	}
	return out
}

// IsDangerous reports whether the named tool requires user approval.
func (r *Registry) IsDangerous(name string) bool {
	t, ok := r.tools[name]
	return ok && t.Dangerous
}

// Call executes a tool directly with JSON arguments. It is used to release
// the original tool call after the user confirms a pending approval.
func (r *Registry) Call(ctx context.Context, name string, jsonArgs []byte) (any, error) {
	t, ok := r.tools[name]
	if !ok {
		return nil, fmt.Errorf("unknown tool %q", name)
	}
	callable, ok := t.Tool.(ttool.CallableTool)
	if !ok {
		return nil, fmt.Errorf("tool %q is not callable", name)
	}
	return callable.Call(ctx, jsonArgs)
}

// DemoTools returns a demo registry: one safe tool plus one dangerous tool so
// the approval chain is exercisable end to end. Both tool bodies are stubs —
// the dangerous one performs no real destructive action.
func DemoTools() *Registry {
	weather := function.NewFunctionTool(getWeather,
		function.WithName("get_weather"),
		function.WithDescription("查询指定城市的天气"))
	del := function.NewFunctionTool(deleteUserData,
		function.WithName("delete_user_data"),
		function.WithDescription("删除指定用户的全部数据，不可恢复（危险操作，执行前需用户确认）"))
	return NewRegistry(
		Tool{Tool: weather},
		Tool{Tool: del, Dangerous: true},
	)
}

type weatherArgs struct {
	City string `json:"city" jsonschema:"description=要查询的城市"`
}

type weatherResult struct {
	City    string `json:"city"`
	Weather string `json:"weather"`
}

func getWeather(_ context.Context, in weatherArgs) (weatherResult, error) {
	return weatherResult{City: in.City, Weather: "晴，26℃（演示数据）"}, nil
}

type deleteArgs struct {
	UserID string `json:"user_id" jsonschema:"description=要删除数据的用户 ID"`
}

type deleteResult struct {
	UserID  string `json:"user_id"`
	Deleted bool   `json:"deleted"`
	Note    string `json:"note"`
}

func deleteUserData(_ context.Context, in deleteArgs) (deleteResult, error) {
	return deleteResult{UserID: in.UserID, Deleted: true, Note: "演示工具，未删除任何真实数据"}, nil
}
