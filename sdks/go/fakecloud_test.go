package fakecloud

import (
	"testing"
)

// ── Unit tests for actual logic (not mocked HTTP) ─────────────────

func TestNewTrimsTrailingSlash(t *testing.T) {
	fc := New("http://localhost:4566/")
	if fc.BaseURL != "http://localhost:4566" {
		t.Errorf("expected trailing slash trimmed, got %s", fc.BaseURL)
	}
}

func TestAPIErrorFormat(t *testing.T) {
	e := &APIError{StatusCode: 500, Body: "internal error"}
	expected := "fakecloud: HTTP 500: internal error"
	if e.Error() != expected {
		t.Errorf("expected %q, got %q", expected, e.Error())
	}
}
