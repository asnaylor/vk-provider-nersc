package superfacility

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

const maxErrorBodyBytes = 4096

const taskJobRefPrefix = "sfapi-task:"

var slurmJobIDPattern = regexp.MustCompile(`\b[0-9]+(?:_[0-9]+)?\b`)

type Client struct {
	Endpoint string
	Token    string
	http     *http.Client
}

func New(endpoint, token string) *Client {
	return &Client{
		Endpoint: strings.TrimRight(strings.TrimSpace(endpoint), "/"),
		Token:    strings.TrimSpace(token),
		http:     &http.Client{Timeout: 30 * time.Second},
	}
}

type JobSubmissionRequest struct {
	Script  string `json:"script"`
	System  string `json:"system"`
	Project string `json:"project,omitempty"`
	Queue   string `json:"queue,omitempty"`
}

type JobSubmissionResponse struct {
	JobID  string `json:"jobid"`
	TaskID string `json:"task_id"`
	Status string `json:"status"`
	Error  string `json:"error"`
}

type taskResponse struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	Result string `json:"result"`
}

type jobOutputResponse struct {
	Status string              `json:"status"`
	Output []map[string]string `json:"output"`
	Error  string              `json:"error"`
}

type GlobusTransferRequest struct {
	SourceUUID string
	TargetUUID string
	SourceDir  string
	TargetDir  string
	Username   string
}

type GlobusTransfer struct {
	GlobusUUID string `json:"globus_uuid"`
	TaskID     string `json:"task_id"`
	UUID       string `json:"uuid"`
	ID         string `json:"id"`
	Message    string `json:"message"`
}

func (t GlobusTransfer) TransferID() string {
	for _, id := range []string{t.GlobusUUID, t.TaskID, t.UUID, t.ID} {
		if id != "" {
			return id
		}
	}
	return ""
}

type GlobusTransferResult struct {
	GlobusUUID       string `json:"globus_uuid"`
	TaskID           string `json:"task_id"`
	UUID             string `json:"uuid"`
	ID               string `json:"id"`
	Status           string `json:"status"`
	State            string `json:"state"`
	CompletionStatus string `json:"completion_status"`
	Message          string `json:"message"`
	Error            string `json:"error"`
	Successful       *bool  `json:"successful"`
	Done             *bool  `json:"done"`
}

func (r GlobusTransferResult) TransferID() string {
	for _, id := range []string{r.GlobusUUID, r.TaskID, r.UUID, r.ID} {
		if id != "" {
			return id
		}
	}
	return ""
}

func (r GlobusTransferResult) Summary() string {
	for _, value := range []string{r.Message, r.Error, r.Status, r.State, r.CompletionStatus} {
		if value != "" {
			return value
		}
	}
	return "unknown transfer status"
}

func (r GlobusTransferResult) IsComplete() (bool, bool) {
	if r.Successful != nil {
		return true, !*r.Successful
	}
	if r.Done != nil && !*r.Done {
		return false, false
	}

	status := strings.ToLower(strings.TrimSpace(firstNonEmpty(r.Status, r.State, r.CompletionStatus)))
	if r.Done != nil && *r.Done && status == "" {
		return true, false
	}
	switch status {
	case "succeeded", "success", "successful", "done", "completed", "complete":
		return true, false
	case "failed", "failure", "error", "cancelled", "canceled":
		return true, true
	case "", "active", "inactive", "pending", "queued", "running", "submitted":
		return false, false
	default:
		return false, false
	}
}

func (c *Client) SubmitJob(ctx context.Context, req JobSubmissionRequest) (string, error) {
	if strings.TrimSpace(req.System) == "" {
		return "", fmt.Errorf("job submission system is required")
	}

	form := url.Values{}
	form.Set("job", req.Script)
	form.Set("isPath", "false")

	httpReq, err := c.newRequest(ctx, http.MethodPost, fmt.Sprintf("compute/jobs/%s", url.PathEscape(req.System)), strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	httpReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return "", fmt.Errorf("submit job request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return "", fmt.Errorf("submit failed: %s", responseError(resp))
	}

	var out JobSubmissionResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", fmt.Errorf("decode submit response: %w", err)
	}
	if strings.EqualFold(out.Status, "ERROR") || out.Error != "" {
		return "", fmt.Errorf("submit failed: %s", firstNonEmpty(out.Error, out.Status))
	}
	if out.JobID != "" {
		return out.JobID, nil
	}
	if out.TaskID == "" {
		return "", fmt.Errorf("submit response missing task_id")
	}
	return makeTaskJobRef(req.System, out.TaskID), nil
}

func (c *Client) GetJobStatus(ctx context.Context, jobID string) (string, error) {
	if machine, taskID, ok := parseTaskJobRef(jobID); ok {
		return c.getTaskBackedJobStatus(ctx, machine, taskID)
	}
	return c.getComputeJobStatus(ctx, "perlmutter", jobID)
}

func (c *Client) getTaskBackedJobStatus(ctx context.Context, machine, taskID string) (string, error) {
	task, err := c.getTask(ctx, taskID)
	if err != nil {
		return "", err
	}
	switch strings.ToLower(strings.TrimSpace(task.Status)) {
	case "new", "":
		return "pending", nil
	case "failed", "cancelled", "canceled":
		return "failed", nil
	case "completed":
		slurmJobID := extractSlurmJobID(task.Result)
		if slurmJobID == "" {
			return "", fmt.Errorf("task %s completed but result did not contain a Slurm job id: %q", taskID, task.Result)
		}
		return c.getComputeJobStatus(ctx, machine, slurmJobID)
	default:
		return strings.ToLower(task.Status), nil
	}
}

func (c *Client) getTask(ctx context.Context, taskID string) (taskResponse, error) {
	req, err := c.newRequest(ctx, http.MethodGet, fmt.Sprintf("tasks/%s", url.PathEscape(taskID)), nil)
	if err != nil {
		return taskResponse{}, err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return taskResponse{}, fmt.Errorf("get task request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return taskResponse{}, fmt.Errorf("task status failed: %s", responseError(resp))
	}

	var out taskResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return taskResponse{}, fmt.Errorf("decode task response: %w", err)
	}
	return out, nil
}

func (c *Client) getComputeJobStatus(ctx context.Context, machine, jobID string) (string, error) {
	req, err := c.newRequest(ctx, http.MethodGet, fmt.Sprintf("compute/jobs/%s/%s?sacct=true&cached=false", url.PathEscape(machine), url.PathEscape(jobID)), nil)
	if err != nil {
		return "", err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("get job status request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("status failed: %s", responseError(resp))
	}

	var out jobOutputResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", fmt.Errorf("decode status response: %w", err)
	}
	if strings.EqualFold(out.Status, "ERROR") || out.Error != "" {
		return "", fmt.Errorf("status failed: %s", firstNonEmpty(out.Error, out.Status))
	}
	if status := statusFromJobOutput(out.Output); status != "" {
		return status, nil
	}
	return "pending", nil
}

func (c *Client) CancelJob(ctx context.Context, jobID string) error {
	if _, taskID, ok := parseTaskJobRef(jobID); ok {
		return c.cancelTask(ctx, taskID)
	}

	req, err := c.newRequest(ctx, http.MethodDelete, fmt.Sprintf("compute/jobs/perlmutter/%s", url.PathEscape(jobID)), nil)
	if err != nil {
		return err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("cancel job request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("cancel failed: %s", responseError(resp))
	}
	return nil
}

func (c *Client) cancelTask(ctx context.Context, taskID string) error {
	req, err := c.newRequest(ctx, http.MethodDelete, fmt.Sprintf("tasks/%s", url.PathEscape(taskID)), nil)
	if err != nil {
		return err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("cancel task request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("cancel failed: %s", responseError(resp))
	}
	return nil
}

func (c *Client) FetchJobLogs(ctx context.Context, jobID string) (string, error) {
	req, err := c.newRequest(ctx, http.MethodGet, fmt.Sprintf("jobs/%s/logs", url.PathEscape(jobID)), nil)
	if err != nil {
		return "", err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("fetch job logs request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("logs failed: %s", responseError(resp))
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read logs response: %w", err)
	}
	return string(data), nil
}

func (c *Client) StartGlobusTransfer(ctx context.Context, req GlobusTransferRequest) (GlobusTransfer, error) {
	if req.SourceUUID == "" {
		return GlobusTransfer{}, fmt.Errorf("source_uuid is required")
	}
	if req.TargetUUID == "" {
		return GlobusTransfer{}, fmt.Errorf("target_uuid is required")
	}
	if req.SourceDir == "" {
		return GlobusTransfer{}, fmt.Errorf("source_dir is required")
	}
	if req.TargetDir == "" {
		return GlobusTransfer{}, fmt.Errorf("target_dir is required")
	}

	form := url.Values{}
	form.Set("source_uuid", req.SourceUUID)
	form.Set("target_uuid", req.TargetUUID)
	form.Set("source_dir", req.SourceDir)
	form.Set("target_dir", req.TargetDir)
	if req.Username != "" {
		form.Set("username", req.Username)
	}

	httpReq, err := c.newRequest(ctx, http.MethodPost, "storage/globus/transfer", strings.NewReader(form.Encode()))
	if err != nil {
		return GlobusTransfer{}, err
	}
	httpReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return GlobusTransfer{}, fmt.Errorf("start globus transfer request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return GlobusTransfer{}, fmt.Errorf("start globus transfer failed: %s", responseError(resp))
	}

	var out GlobusTransfer
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return GlobusTransfer{}, fmt.Errorf("decode globus transfer response: %w", err)
	}
	if out.TransferID() == "" {
		return GlobusTransfer{}, fmt.Errorf("globus transfer response missing transfer id")
	}
	return out, nil
}

func (c *Client) CheckGlobusTransfer(ctx context.Context, globusUUID string) (GlobusTransferResult, error) {
	if globusUUID == "" {
		return GlobusTransferResult{}, fmt.Errorf("globus transfer id is required")
	}

	req, err := c.newRequest(ctx, http.MethodGet, fmt.Sprintf("storage/globus/transfer/%s", url.PathEscape(globusUUID)), nil)
	if err != nil {
		return GlobusTransferResult{}, err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return GlobusTransferResult{}, fmt.Errorf("check globus transfer request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return GlobusTransferResult{}, fmt.Errorf("check globus transfer failed: %s", responseError(resp))
	}

	var out GlobusTransferResult
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return GlobusTransferResult{}, fmt.Errorf("decode globus transfer status response: %w", err)
	}
	return out, nil
}

func (c *Client) newRequest(ctx context.Context, method, path string, body io.Reader) (*http.Request, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if c.Endpoint == "" {
		return nil, fmt.Errorf("superfacility endpoint is required")
	}

	endpoint := fmt.Sprintf("%s/%s", strings.TrimRight(c.Endpoint, "/"), strings.TrimLeft(path, "/"))
	req, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		return nil, fmt.Errorf("create %s request for %s: %w", method, endpoint, err)
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	return req, nil
}

func responseError(resp *http.Response) string {
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodyBytes))
	if err != nil {
		return fmt.Sprintf("%s (failed to read response body: %v)", resp.Status, err)
	}
	bodyText := strings.TrimSpace(string(body))
	if bodyText == "" {
		return resp.Status
	}
	return fmt.Sprintf("%s: %s", resp.Status, bodyText)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func makeTaskJobRef(machine, taskID string) string {
	return taskJobRefPrefix + url.QueryEscape(machine) + ":" + url.QueryEscape(taskID)
}

func parseTaskJobRef(ref string) (string, string, bool) {
	if !strings.HasPrefix(ref, taskJobRefPrefix) {
		return "", "", false
	}
	parts := strings.SplitN(strings.TrimPrefix(ref, taskJobRefPrefix), ":", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	machine, err := url.QueryUnescape(parts[0])
	if err != nil {
		return "", "", false
	}
	taskID, err := url.QueryUnescape(parts[1])
	if err != nil {
		return "", "", false
	}
	return machine, taskID, machine != "" && taskID != ""
}

func extractSlurmJobID(result string) string {
	return slurmJobIDPattern.FindString(result)
}

func statusFromJobOutput(rows []map[string]string) string {
	for _, row := range rows {
		for _, key := range []string{"state", "State", "STATE", "job_state", "JobState", "ST"} {
			if status := normalizeSlurmStatus(row[key]); status != "" {
				return status
			}
		}
	}
	return ""
}

func normalizeSlurmStatus(status string) string {
	status = strings.ToUpper(strings.TrimSpace(status))
	switch status {
	case "PD", "PENDING", "CONFIGURING":
		return "pending"
	case "R", "RUNNING", "CG", "COMPLETING", "S", "SUSPENDED":
		return "running"
	case "CD", "COMPLETED", "COMPLETED+":
		return "completed"
	case "F", "FAILED", "CA", "CANCELLED", "CANCELED", "TO", "TIMEOUT", "NF", "NODE_FAIL", "OOM", "OUT_OF_MEMORY":
		return "failed"
	default:
		return strings.ToLower(status)
	}
}
