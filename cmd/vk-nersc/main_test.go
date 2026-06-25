package main

import (
	"net/url"
	"testing"
	"time"
)

func TestParsePodLogOptionsAcceptsAirflowStyleQuery(t *testing.T) {
	values := url.Values{
		"container":        []string{"base"},
		"follow":           []string{"True"},
		"timestamps":       []string{"False"},
		"tail_lines":       []string{"10"},
		"since_seconds":    []string{"5"},
		"limit_bytes":      []string{"1024"},
		"_preload_content": []string{"false"},
	}

	opts, err := parsePodLogOptions(values)
	if err != nil {
		t.Fatalf("parsePodLogOptions returned error: %v", err)
	}
	if opts.Container != "base" {
		t.Fatalf("container = %q, want base", opts.Container)
	}
	if !opts.Follow {
		t.Fatal("follow = false, want true")
	}
	if opts.Timestamps {
		t.Fatal("timestamps = true, want false")
	}
	if opts.TailLines == nil || *opts.TailLines != 10 {
		t.Fatalf("tailLines = %v, want 10", opts.TailLines)
	}
	if opts.SinceSeconds == nil || *opts.SinceSeconds != 5 {
		t.Fatalf("sinceSeconds = %v, want 5", opts.SinceSeconds)
	}
	if opts.LimitBytes == nil || *opts.LimitBytes != 1024 {
		t.Fatalf("limitBytes = %v, want 1024", opts.LimitBytes)
	}
}

func TestParsePodLogOptionsAcceptsKubernetesStyleQuery(t *testing.T) {
	values := url.Values{
		"container":                    []string{"base"},
		"follow":                       []string{"true"},
		"previous":                     []string{"false"},
		"timestamps":                   []string{"true"},
		"tailLines":                    []string{"20"},
		"sinceSeconds":                 []string{"30"},
		"sinceTime":                    []string{"2026-06-25T01:09:31Z"},
		"insecureSkipTLSVerifyBackend": []string{"true"},
	}

	opts, err := parsePodLogOptions(values)
	if err != nil {
		t.Fatalf("parsePodLogOptions returned error: %v", err)
	}
	if !opts.Follow {
		t.Fatal("follow = false, want true")
	}
	if opts.Previous {
		t.Fatal("previous = true, want false")
	}
	if !opts.Timestamps {
		t.Fatal("timestamps = false, want true")
	}
	if opts.TailLines == nil || *opts.TailLines != 20 {
		t.Fatalf("tailLines = %v, want 20", opts.TailLines)
	}
	if opts.SinceSeconds == nil || *opts.SinceSeconds != 30 {
		t.Fatalf("sinceSeconds = %v, want 30", opts.SinceSeconds)
	}
	if opts.SinceTime == nil || !opts.SinceTime.Time.Equal(time.Date(2026, 6, 25, 1, 9, 31, 0, time.UTC)) {
		t.Fatalf("sinceTime = %v, want 2026-06-25T01:09:31Z", opts.SinceTime)
	}
	if !opts.InsecureSkipTLSVerifyBackend {
		t.Fatal("insecureSkipTLSVerifyBackend = false, want true")
	}
}

func TestParsePodLogOptionsRejectsMalformedQuery(t *testing.T) {
	_, err := parsePodLogOptions(url.Values{"follow": []string{"maybe"}})
	if err == nil {
		t.Fatal("parsePodLogOptions returned nil error")
	}
}
