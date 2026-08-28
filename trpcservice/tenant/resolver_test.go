package tenant_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// fakeStore implements tenant.Store with a load counter for cache assertions.
type fakeStore struct {
	mu    sync.Mutex
	data  tenant.Data
	err   error
	loads int
}

func (f *fakeStore) LoadAll(context.Context) (tenant.Data, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.loads++
	return f.data, f.err
}

func (f *fakeStore) loadCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.loads
}

func testData() tenant.Data {
	return tenant.Data{
		Tenants: []tenant.Tenant{
			{ID: "t1", Name: "demo", Status: tenant.StatusActive},
			{ID: "t2", Name: "off", Status: tenant.StatusDisabled},
		},
		Apps: []tenant.AgentApp{
			{ID: "a1", TenantID: "t1", Name: "assistant", Version: 1, Status: "published"},
			{ID: "a2", TenantID: "t2", Name: "assistant", Version: 1, Status: "published"},
		},
		Bindings: []tenant.ChannelBinding{
			{ID: "b1", TenantID: "t1", Channel: "mock", AppID: "a1",
				WebhookPath: "/mock/callback", Status: tenant.StatusActive},
			{ID: "b2", TenantID: "t2", Channel: "mock", AppID: "a2",
				WebhookPath: "/off/callback", Status: tenant.StatusActive},
		},
	}
}

func TestResolveKnownPath(t *testing.T) {
	r := tenant.NewResolver(&fakeStore{data: testData()})
	route, err := r.Resolve(context.Background(), "/mock/callback")
	if err != nil {
		t.Fatal(err)
	}
	if route.Tenant.ID != "t1" || route.App.ID != "a1" || route.Binding.ID != "b1" {
		t.Fatalf("unexpected route: %+v", route)
	}
}

func TestResolveUnknownPath(t *testing.T) {
	r := tenant.NewResolver(&fakeStore{data: testData()})
	_, err := r.Resolve(context.Background(), "/nope")
	if !errors.Is(err, tenant.ErrUnknownBinding) {
		t.Fatalf("want ErrUnknownBinding, got %v", err)
	}
}

func TestResolveDisabledTenant(t *testing.T) {
	r := tenant.NewResolver(&fakeStore{data: testData()})
	_, err := r.Resolve(context.Background(), "/off/callback")
	if !errors.Is(err, tenant.ErrInactive) {
		t.Fatalf("want ErrInactive, got %v", err)
	}
}

func TestCacheTTL(t *testing.T) {
	store := &fakeStore{data: testData()}
	r := tenant.NewResolverWithTTL(store, 50*time.Millisecond)
	ctx := context.Background()

	if _, err := r.Resolve(ctx, "/mock/callback"); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Resolve(ctx, "/mock/callback"); err != nil {
		t.Fatal(err)
	}
	if n := store.loadCount(); n != 1 {
		t.Fatalf("want 1 load within TTL, got %d", n)
	}

	time.Sleep(60 * time.Millisecond)
	if _, err := r.Resolve(ctx, "/mock/callback"); err != nil {
		t.Fatal(err)
	}
	if n := store.loadCount(); n != 2 {
		t.Fatalf("want reload after TTL, got %d loads", n)
	}
}

func TestReloadFailureServesStale(t *testing.T) {
	store := &fakeStore{data: testData()}
	r := tenant.NewResolverWithTTL(store, 50*time.Millisecond)
	ctx := context.Background()

	if _, err := r.Resolve(ctx, "/mock/callback"); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.err = errors.New("pg down")
	store.mu.Unlock()

	time.Sleep(60 * time.Millisecond)
	route, err := r.Resolve(ctx, "/mock/callback")
	if err != nil {
		t.Fatalf("stale snapshot should keep serving, got %v", err)
	}
	if route.Tenant.ID != "t1" {
		t.Fatalf("unexpected route: %+v", route)
	}
}

func TestFirstLoadFailure(t *testing.T) {
	store := &fakeStore{err: errors.New("pg down")}
	r := tenant.NewResolver(store)
	if _, err := r.Resolve(context.Background(), "/mock/callback"); err == nil {
		t.Fatal("want error when the first load fails")
	}
}
