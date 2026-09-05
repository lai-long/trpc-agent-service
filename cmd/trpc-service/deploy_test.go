package main

import (
	"os"
	"strings"
	"testing"
)

// TestK8sManifestsProvideKMSToken pins the contract between deploy/k8s and
// buildSecretResolver's fail-closed KMS branch: with resolver=kms the process
// refuses to start unless the bootstrap token file exists under SecretsDir, so
// manifests that ask for kms must also deliver that file — the ConfigMap names
// the dir and the token ref, and every role Deployment mounts a Secret volume
// at the dir carrying the ref's file. Nothing in the toolchain checks YAML
// against Go: without this test the manifests can declare kms and starve every
// pod of its token while every build stays green, which is three CrashLooping
// Deployments on release day (the exact gap this test was added to close).
func TestK8sManifestsProvideKMSToken(t *testing.T) {
	cfg := readManifest(t, "../../deploy/k8s/config.yaml")
	if !strings.Contains(cfg, `TRPC_SECRET_RESOLVER: "kms"`) {
		t.Fatalf(`config.yaml must deploy the production resolver (TRPC_SECRET_RESOLVER: "kms"); if it moved off kms on purpose, update this test with it`)
	}
	dir := manifestEnv(t, cfg, "TRPC_SECRETS_DIR")
	ref := manifestEnv(t, cfg, "TRPC_KMS_TOKEN_REF")

	for _, role := range []string{"gateway", "worker", "admin"} {
		manifest := readManifest(t, "../../deploy/k8s/"+role+".yaml")
		if !strings.Contains(manifest, "mountPath: "+dir) {
			t.Errorf("%s.yaml: nothing mounted at TRPC_SECRETS_DIR %q — without the volume the bootstrap token never reaches the pod and it CrashLoops at startup", role, dir)
		}
		if !strings.Contains(manifest, "path: "+ref) {
			t.Errorf("%s.yaml: no Secret item files %q into TRPC_SECRETS_DIR — the resolver reads the token by that name", role, ref)
		}
	}
}

// readManifest reads one repo manifest; a missing file is a broken checkout,
// not a skip.
func readManifest(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}

// manifestEnv reads one `KEY: "value"` line out of a manifest, failing the test
// when the key is absent or empty — a missing declaration is itself the drift
// this suite exists to catch.
func manifestEnv(t *testing.T, manifest, key string) string {
	t.Helper()
	for _, line := range strings.Split(manifest, "\n") {
		after, ok := strings.CutPrefix(strings.TrimSpace(line), key+":")
		if !ok {
			continue
		}
		v := strings.TrimSpace(after)
		if i := strings.Index(v, "#"); i >= 0 {
			v = strings.TrimSpace(v[:i])
		}
		v = strings.Trim(v, `"`)
		if v == "" {
			t.Fatalf("deploy/k8s/config.yaml declares %s empty", key)
		}
		return v
	}
	t.Fatalf("deploy/k8s/config.yaml does not declare %s", key)
	return ""
}
