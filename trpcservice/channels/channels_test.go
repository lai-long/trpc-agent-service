package channels

import (
	"errors"
	"net/url"
	"strings"
	"testing"
)

func TestRenderPlain(t *testing.T) {
	in := "## 标题\n**加粗** 和 `代码`，见 [文档](https://example.com)。\n- 列表项"
	want := "标题\n加粗 和 代码，见 文档 (https://example.com)。\n- 列表项"
	if got := RenderPlain(in); got != want {
		t.Fatalf("RenderPlain:\n got %q\nwant %q", got, want)
	}
	// Plain text passes through untouched.
	if got := RenderPlain("纯文本，没有标记"); got != "纯文本，没有标记" {
		t.Fatalf("plain text must pass through: %q", got)
	}
}

func TestLooksMarkdown(t *testing.T) {
	if !LooksMarkdown("**加粗**") || !LooksMarkdown("## 标题") || !LooksMarkdown("[a](b)") {
		t.Fatal("markdown markers must be detected")
	}
	if LooksMarkdown("纯文本") {
		t.Fatal("plain text must not be detected as markdown")
	}
}

func TestScrubError(t *testing.T) {
	// A url.Error from net/http embeds the full URL incl. query secrets.
	raw := &url.Error{Op: "Get", URL: "https://api.example.com/cgi-bin/gettoken?corpid=ww1&corpsecret=SUPERSECRET", Err: errors.New("dial refused")}
	scrubbed := ScrubError(raw)
	if strings.Contains(scrubbed.Error(), "SUPERSECRET") {
		t.Fatalf("corpsecret leaked in error: %v", scrubbed)
	}
	if !strings.Contains(scrubbed.Error(), "corpsecret=%2A%2A%2A") && !strings.Contains(scrubbed.Error(), "corpsecret=***") {
		t.Fatalf("scrubbed marker missing: %v", scrubbed)
	}
	// Non-url errors pass through.
	plain := errors.New("plain")
	if ScrubError(plain) != plain {
		t.Fatal("non-url error must pass through unchanged")
	}
}
