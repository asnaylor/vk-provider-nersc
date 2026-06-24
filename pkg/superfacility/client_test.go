package superfacility

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestSubmitJobSendsRequestAndDecodesJobID(t *testing.T) {
	client := newTestClient(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/api/v1.2/compute/jobs/perlmutter" {
			t.Fatalf("path = %s, want compute job submit path", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer token" {
			t.Fatalf("authorization header = %q", got)
		}
		if got := r.Header.Get("Content-Type"); got != "application/x-www-form-urlencoded" {
			t.Fatalf("content type = %q", got)
		}
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}
		if got := r.PostForm.Get("job"); got != "#!/bin/bash" {
			t.Fatalf("job form = %q", got)
		}
		if got := r.PostForm.Get("isPath"); got != "false" {
			t.Fatalf("isPath form = %q", got)
		}

		return response(http.StatusOK, `{"task_id":"task-123","status":"OK"}`), nil
	})

	jobID, err := client.SubmitJob(context.Background(), JobSubmissionRequest{
		Script:  "#!/bin/bash",
		System:  "perlmutter",
		Project: "m1234",
	})
	if err != nil {
		t.Fatalf("SubmitJob returned error: %v", err)
	}
	if jobID != makeTaskJobRef("perlmutter", "task-123") {
		t.Fatalf("jobID = %q, want task-backed ref", jobID)
	}
}

func TestGetJobStatusPollsTaskAndSlurmJob(t *testing.T) {
	requests := 0
	client := newTestClient(func(r *http.Request) (*http.Response, error) {
		requests++
		switch requests {
		case 1:
			if r.URL.EscapedPath() != "/api/v1.2/tasks/task%2F123" {
				t.Fatalf("escaped task path = %s", r.URL.EscapedPath())
			}
			return response(http.StatusOK, `{"id":"task/123","status":"completed","result":"Submitted batch job 98765"}`), nil
		case 2:
			if r.URL.EscapedPath() != "/api/v1.2/compute/jobs/perlmutter/98765" {
				t.Fatalf("escaped job path = %s", r.URL.EscapedPath())
			}
			if r.URL.Query().Get("sacct") != "true" || r.URL.Query().Get("cached") != "false" {
				t.Fatalf("query = %s, want sacct=true cached=false", r.URL.RawQuery)
			}
			return response(http.StatusOK, `{"status":"OK","output":[{"State":"RUNNING"}]}`), nil
		default:
			t.Fatalf("unexpected request %d: %s", requests, r.URL.String())
		}
		return nil, nil
	})

	status, err := client.GetJobStatus(context.Background(), makeTaskJobRef("perlmutter", "task/123"))
	if err != nil {
		t.Fatalf("GetJobStatus returned error: %v", err)
	}
	if status != "running" {
		t.Fatalf("status = %q, want running", status)
	}
}

func TestFetchJobLogsDownloadsTaskBackedSlurmStdout(t *testing.T) {
	requests := 0
	client := newTestClient(func(r *http.Request) (*http.Response, error) {
		requests++
		switch requests {
		case 1:
			if r.URL.EscapedPath() != "/api/v1.2/tasks/task%2F123" {
				t.Fatalf("escaped task path = %s", r.URL.EscapedPath())
			}
			return response(http.StatusOK, `{"id":"task/123","status":"completed","result":"{\"status\":\"ok\",\"jobid\":\"98765\",\"error\":null}"}`), nil
		case 2:
			if r.URL.EscapedPath() != "/api/v1.2/compute/jobs/perlmutter/98765" {
				t.Fatalf("escaped job path = %s", r.URL.EscapedPath())
			}
			body := `{"status":"OK","output":[{"state":"COMPLETED","admincomment":"{\"stdoutPath\":\"/global/u2/a/asnaylor/perlmutter-smoke.out\"}"}]}`
			return response(http.StatusOK, body), nil
		case 3:
			if r.URL.EscapedPath() != "/api/v1.2/utilities/download/perlmutter//global/u2/a/asnaylor/perlmutter-smoke.out" {
				t.Fatalf("escaped download path = %s", r.URL.EscapedPath())
			}
			return response(http.StatusOK, `{"status":"OK","file":"hello from perlmutter\n","error":null,"is_binary":false}`), nil
		default:
			t.Fatalf("unexpected request %d: %s", requests, r.URL.String())
		}
		return nil, nil
	})

	logs, err := client.FetchJobLogs(context.Background(), makeTaskJobRef("perlmutter", "task/123"))
	if err != nil {
		t.Fatalf("FetchJobLogs returned error: %v", err)
	}
	if logs != "hello from perlmutter\n" {
		t.Fatalf("logs = %q", logs)
	}
}

func TestFetchJobLogsFallsBackToWorkdirAndJobName(t *testing.T) {
	requests := 0
	client := newTestClient(func(r *http.Request) (*http.Response, error) {
		requests++
		switch requests {
		case 1:
			return response(http.StatusOK, `{"status":"OK","output":[{"state":"COMPLETED","workdir":"/global/u2/a/asnaylor","jobname":"demo"}]}`), nil
		case 2:
			if r.URL.EscapedPath() != "/api/v1.2/utilities/download/perlmutter//global/u2/a/asnaylor/demo.out" {
				t.Fatalf("escaped download path = %s", r.URL.EscapedPath())
			}
			return response(http.StatusOK, `{"status":"OK","file":"demo logs\n","is_binary":false}`), nil
		default:
			t.Fatalf("unexpected request %d: %s", requests, r.URL.String())
		}
		return nil, nil
	})

	logs, err := client.FetchJobLogs(context.Background(), "98765")
	if err != nil {
		t.Fatalf("FetchJobLogs returned error: %v", err)
	}
	if logs != "demo logs\n" {
		t.Fatalf("logs = %q", logs)
	}
}

func TestClientErrorIncludesStatusAndBody(t *testing.T) {
	client := newTestClient(func(r *http.Request) (*http.Response, error) {
		return response(http.StatusUnauthorized, "bad token\n"), nil
	})

	_, err := client.GetJobStatus(context.Background(), "123")
	if err == nil {
		t.Fatal("GetJobStatus returned nil error")
	}
	if !strings.Contains(err.Error(), "401 Unauthorized") || !strings.Contains(err.Error(), "bad token") {
		t.Fatalf("error = %q, want status and body", err.Error())
	}
}

func TestSubmitJobRequiresJobID(t *testing.T) {
	client := newTestClient(func(r *http.Request) (*http.Response, error) {
		return response(http.StatusOK, `{}`), nil
	})

	_, err := client.SubmitJob(context.Background(), JobSubmissionRequest{Script: "script", System: "perlmutter"})
	if err == nil || !strings.Contains(err.Error(), "missing task_id") {
		t.Fatalf("error = %v, want missing task_id", err)
	}
}

func TestStartGlobusTransferUsesFormEndpoint(t *testing.T) {
	client := newTestClient(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/api/v1.2/storage/globus/transfer" {
			t.Fatalf("path = %s, want globus transfer path", r.URL.Path)
		}
		if got := r.Header.Get("Content-Type"); got != "application/x-www-form-urlencoded" {
			t.Fatalf("content type = %q", got)
		}
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}
		expected := map[string]string{
			"source_uuid": "dtn",
			"target_uuid": "perlmutter",
			"source_dir":  "/input",
			"target_dir":  "/scratch/input",
			"username":    "alice",
		}
		for key, want := range expected {
			if got := r.PostForm.Get(key); got != want {
				t.Fatalf("%s = %q, want %q", key, got, want)
			}
		}
		return response(http.StatusOK, `{"globus_uuid":"transfer-123"}`), nil
	})

	transfer, err := client.StartGlobusTransfer(context.Background(), GlobusTransferRequest{
		SourceUUID: "dtn",
		TargetUUID: "perlmutter",
		SourceDir:  "/input",
		TargetDir:  "/scratch/input",
		Username:   "alice",
	})
	if err != nil {
		t.Fatalf("StartGlobusTransfer returned error: %v", err)
	}
	if transfer.TransferID() != "transfer-123" {
		t.Fatalf("transfer id = %q, want transfer-123", transfer.TransferID())
	}
}

func TestCheckGlobusTransferEscapesIDAndDecodesStatus(t *testing.T) {
	client := newTestClient(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodGet {
			t.Fatalf("method = %s, want GET", r.Method)
		}
		if r.URL.EscapedPath() != "/api/v1.2/storage/globus/transfer/transfer%2F123" {
			t.Fatalf("escaped path = %s", r.URL.EscapedPath())
		}
		return response(http.StatusOK, `{"globus_uuid":"transfer/123","status":"SUCCEEDED"}`), nil
	})

	result, err := client.CheckGlobusTransfer(context.Background(), "transfer/123")
	if err != nil {
		t.Fatalf("CheckGlobusTransfer returned error: %v", err)
	}
	done, failed := result.IsComplete()
	if !done || failed {
		t.Fatalf("completion = done %t failed %t, want done true failed false", done, failed)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

func newTestClient(fn roundTripFunc) *Client {
	client := New("https://api.nersc.gov/api/v1.2/", " token ")
	client.http = &http.Client{Transport: fn}
	return client
}

func response(statusCode int, body string) *http.Response {
	return &http.Response{
		StatusCode: statusCode,
		Status:     fmt.Sprintf("%d %s", statusCode, http.StatusText(statusCode)),
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}
