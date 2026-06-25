package provider

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"

	"vk-provider-nersc/pkg/superfacility"
)

func buildStagingState(pod *corev1.Pod, jobScratchBase string, volumeScratchPaths map[string]string) (*podStagingState, error) {
	inputSource := getAnnotation(pod, annotationInputSource)
	outputDest := getAnnotation(pod, annotationOutputDest)
	transferMode, err := getTransferModeAnnotation(pod)
	if err != nil {
		return nil, err
	}
	stageOut, err := getBoolAnnotation(pod, annotationStageOut)
	if err != nil {
		return nil, err
	}

	if inputSource == "" && !stageOut {
		return nil, nil
	}

	if transferMode == stagingTransferModeSFAPI && strings.Contains(jobScratchBase, "$") {
		return nil, fmt.Errorf("%s=%s requires %s to be a concrete absolute path, not %q", annotationTransferMode, transferMode, annotationScratchBase, jobScratchBase)
	}

	state := &podStagingState{transferMode: transferMode}
	if inputSource != "" {
		inputStagePath, err := resolveStagePath(pod, jobScratchBase, volumeScratchPaths, annotationInputVolume)
		if err != nil {
			return nil, err
		}
		switch transferMode {
		case stagingTransferModeGlobus:
			input, err := parseGlobusLocation(inputSource)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", annotationInputSource, err)
			}
			state.inputSource = input
			state.inputTargetDir = inputStagePath
		case stagingTransferModeSFAPI:
			inputPath, err := parseLocalTransferPath(inputSource)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", annotationInputSource, err)
			}
			state.inputLocalPath = inputPath
			state.inputTargetDir = inputStagePath
			state.inputTargetPath = path.Join(inputStagePath, filepath.Base(inputPath))
		}
	}

	if stageOut {
		outputStagePath, err := resolveStagePath(pod, jobScratchBase, volumeScratchPaths, annotationOutputVolume)
		if err != nil {
			return nil, err
		}
		if outputDest == "" {
			return nil, fmt.Errorf("%s must be set when %s is true", annotationOutputDest, annotationStageOut)
		}
		switch transferMode {
		case stagingTransferModeGlobus:
			output, err := parseGlobusLocation(outputDest)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", annotationOutputDest, err)
			}
			state.outputDest = output
			state.outputSourceDir = outputStagePath
			state.outputRequest = &superfacility.GlobusTransferRequest{
				SourceUUID: "perlmutter",
				TargetUUID: output.Endpoint,
				SourceDir:  outputStagePath,
				TargetDir:  output.Path,
				Username:   getAnnotation(pod, annotationGlobusUsername),
			}
		case stagingTransferModeSFAPI:
			outputPath, err := parseLocalTransferPath(outputDest)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", annotationOutputDest, err)
			}
			state.outputLocalPath = outputPath
			state.outputSourceDir = outputStagePath
			state.outputSourcePath = path.Join(outputStagePath, filepath.Base(outputPath))
		}
	}

	return state, nil
}

func (s *podStagingState) hasStageOut() bool {
	return s != nil && (s.outputRequest != nil || s.outputLocalPath != "")
}

func getTransferModeAnnotation(pod *corev1.Pod) (stagingTransferMode, error) {
	mode := strings.ToLower(getAnnotation(pod, annotationTransferMode))
	switch mode {
	case "", string(stagingTransferModeGlobus):
		return stagingTransferModeGlobus, nil
	case string(stagingTransferModeSFAPI):
		return stagingTransferModeSFAPI, nil
	default:
		return "", fmt.Errorf("%s must be one of %q or %q", annotationTransferMode, stagingTransferModeGlobus, stagingTransferModeSFAPI)
	}
}

func parseGlobusLocation(raw string) (*globusLocation, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return nil, fmt.Errorf("invalid Globus URI: %w", err)
	}
	if parsed.Scheme != "globus" {
		return nil, fmt.Errorf("expected globus:// endpoint URI")
	}
	if parsed.Host == "" {
		return nil, fmt.Errorf("missing Globus endpoint")
	}
	if parsed.Path == "" || parsed.Path == "/" {
		return nil, fmt.Errorf("missing Globus path")
	}
	return &globusLocation{
		Endpoint: parsed.Host,
		Path:     parsed.Path,
	}, nil
}

func parseLocalTransferPath(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("missing local file path")
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("invalid local file path: %w", err)
	}
	switch parsed.Scheme {
	case "":
		return raw, nil
	case "file":
		if parsed.Host != "" {
			return "", fmt.Errorf("file:// URIs must not include a host")
		}
		if parsed.Path == "" {
			return "", fmt.Errorf("missing file path")
		}
		return parsed.Path, nil
	default:
		return "", fmt.Errorf("expected a provider-local path or file:// URI")
	}
}

func resolveStagePath(pod *corev1.Pod, jobScratchBase string, volumeScratchPaths map[string]string, specificAnnotation string) (string, error) {
	annotationUsed := specificAnnotation
	stageVolume := getAnnotation(pod, specificAnnotation)
	if stageVolume == "" {
		stageVolume = getAnnotation(pod, annotationStageVolume)
		if stageVolume != "" {
			annotationUsed = annotationStageVolume
		}
	}
	if stageVolume != "" {
		path, ok := volumeScratchPaths[stageVolume]
		if !ok {
			return "", fmt.Errorf("%s references unknown volume %q", annotationUsed, stageVolume)
		}
		return path, nil
	}

	if len(volumeScratchPaths) == 0 {
		return jobScratchBase, nil
	}
	if len(volumeScratchPaths) == 1 {
		for _, path := range volumeScratchPaths {
			return path, nil
		}
	}

	volumeNames := make([]string, 0, len(volumeScratchPaths))
	for name := range volumeScratchPaths {
		volumeNames = append(volumeNames, name)
	}
	sort.Strings(volumeNames)
	return "", fmt.Errorf("%s or %s is required when staging with multiple volumes: %s", specificAnnotation, annotationStageVolume, strings.Join(volumeNames, ", "))
}

func getAnnotation(pod *corev1.Pod, key string) string {
	if pod == nil || pod.Annotations == nil {
		return ""
	}
	return strings.TrimSpace(pod.Annotations[key])
}

func getBoolAnnotation(pod *corev1.Pod, key string) (bool, error) {
	value := getAnnotation(pod, key)
	if value == "" {
		return false, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("%s must be a boolean: %w", key, err)
	}
	return parsed, nil
}

func (p *NerscProvider) stageInput(ctx context.Context, client jobClient, key string, staging *podStagingState, pod *corev1.Pod) error {
	switch {
	case staging == nil:
		return nil
	case staging.inputSource != nil:
		transferID, err := p.startAndWaitForTransfer(ctx, client, superfacility.GlobusTransferRequest{
			SourceUUID: staging.inputSource.Endpoint,
			TargetUUID: "perlmutter",
			SourceDir:  staging.inputSource.Path,
			TargetDir:  staging.inputTargetDir,
			Username:   getAnnotation(pod, annotationGlobusUsername),
		})
		if err != nil {
			return fmt.Errorf("stage input for pod %s: %w", key, err)
		}
		staging.inputTransferID = transferID
		log.Printf("Pod %s input staged with Globus transfer %s", key, transferID)
	case staging.inputLocalPath != "":
		localPath, err := p.resolveLocalTransferPath(staging.inputLocalPath)
		if err != nil {
			return fmt.Errorf("stage input for pod %s: %w", key, err)
		}
		file, err := os.Open(localPath)
		if err != nil {
			return fmt.Errorf("stage input for pod %s: open %s: %w", key, localPath, err)
		}
		defer file.Close()

		info, err := file.Stat()
		if err != nil {
			return fmt.Errorf("stage input for pod %s: stat %s: %w", key, localPath, err)
		}
		if info.IsDir() {
			return fmt.Errorf("stage input for pod %s: %s is a directory; SFAPI transfer mode supports single files", key, localPath)
		}
		targetDir := path.Dir(staging.inputTargetPath)
		command := "bash -c " + remoteShellQuote("mkdir -p -- "+remoteShellQuote(targetDir))
		if _, err := client.RunCommand(ctx, "perlmutter", command); err != nil {
			return fmt.Errorf("stage input for pod %s: create remote directory %s: %w", key, targetDir, err)
		}
		if err := client.UploadFile(ctx, "perlmutter", staging.inputTargetPath, filepath.Base(localPath), file); err != nil {
			return fmt.Errorf("stage input for pod %s: upload %s to %s: %w", key, localPath, staging.inputTargetPath, err)
		}
		log.Printf("Pod %s input file %s uploaded to %s via SFAPI utilities", key, localPath, staging.inputTargetPath)
	}
	return nil
}

func (p *NerscProvider) startAndWaitForTransfer(ctx context.Context, client jobClient, req superfacility.GlobusTransferRequest) (string, error) {
	transfer, err := client.StartGlobusTransfer(ctx, req)
	if err != nil {
		return "", err
	}

	transferID := transfer.TransferID()
	timeout := p.transferTimeout
	if timeout <= 0 {
		timeout = defaultTransferTimeout
	}
	pollInterval := p.transferPollInterval
	if pollInterval <= 0 {
		pollInterval = defaultTransferPollInterval
	}

	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	for {
		result, err := client.CheckGlobusTransfer(waitCtx, transferID)
		if err != nil {
			return transferID, err
		}
		done, failed := result.IsComplete()
		if done && failed {
			return transferID, fmt.Errorf("globus transfer %s failed: %s", transferID, result.Summary())
		}
		if done {
			return transferID, nil
		}

		timer := time.NewTimer(pollInterval)
		select {
		case <-waitCtx.Done():
			timer.Stop()
			return transferID, fmt.Errorf("globus transfer %s timed out after %s", transferID, timeout)
		case <-timer.C:
		}
	}
}

func (p *NerscProvider) reconcileStageOut(ctx context.Context, key, token string) corev1.PodStatus {
	client, err := p.clientForToken(token)
	if err != nil {
		msg := fmt.Sprintf("create Superfacility client: %v", err)
		p.setStageOutStatus(key, transferFailed, "", msg)
		return podStatus(corev1.PodFailed, "StageOutFailed", msg)
	}

	staging, status, outputErr := p.stageOutSnapshot(key)
	switch status {
	case transferSucceeded:
		return podStatus(corev1.PodSucceeded, "StageOutComplete", "Output data staged out")
	case transferFailed:
		return podStatus(corev1.PodFailed, "StageOutFailed", outputErr)
	case transferStarting:
		return podStatus(corev1.PodRunning, "StageOutStarting", "Starting output data transfer")
	}

	if staging.transferMode == stagingTransferModeSFAPI {
		return p.reconcileSFAPIStageOut(ctx, key, client, staging, status)
	}

	req := *staging.outputRequest
	transferID := staging.outputTransferID
	if status == transferNotStarted {
		p.setStageOutStatus(key, transferStarting, "", "")
		transfer, err := client.StartGlobusTransfer(ctx, req)
		if err != nil {
			msg := fmt.Sprintf("start output transfer: %v", err)
			p.setStageOutStatus(key, transferFailed, "", msg)
			return podStatus(corev1.PodFailed, "StageOutFailed", msg)
		}
		transferID = transfer.TransferID()
		p.setStageOutStatus(key, transferRunning, transferID, "")
		log.Printf("Pod %s output stage-out started as Globus transfer %s", key, transferID)
	}

	result, err := client.CheckGlobusTransfer(ctx, transferID)
	if err != nil {
		msg := fmt.Sprintf("check output transfer %s: %v", transferID, err)
		p.setStageOutStatus(key, transferFailed, transferID, msg)
		return podStatus(corev1.PodFailed, "StageOutFailed", msg)
	}
	done, failed := result.IsComplete()
	if done && failed {
		msg := fmt.Sprintf("globus transfer %s failed: %s", transferID, result.Summary())
		p.setStageOutStatus(key, transferFailed, transferID, msg)
		return podStatus(corev1.PodFailed, "StageOutFailed", msg)
	}
	if done {
		p.setStageOutStatus(key, transferSucceeded, transferID, "")
		return podStatus(corev1.PodSucceeded, "StageOutComplete", "Output data staged out")
	}

	return podStatus(corev1.PodRunning, "StageOutRunning", fmt.Sprintf("Output transfer %s is still running", transferID))
}

func (p *NerscProvider) reconcileSFAPIStageOut(ctx context.Context, key string, client jobClient, staging podStagingState, status transferStatus) corev1.PodStatus {
	if status == transferNotStarted {
		p.setStageOutStatus(key, transferStarting, "", "")
		localPath, err := p.resolveLocalTransferPath(staging.outputLocalPath)
		if err != nil {
			msg := fmt.Sprintf("resolve output destination: %v", err)
			p.setStageOutStatus(key, transferFailed, "", msg)
			return podStatus(corev1.PodFailed, "StageOutFailed", msg)
		}
		data, err := client.DownloadFile(ctx, "perlmutter", staging.outputSourcePath)
		if err != nil {
			msg := fmt.Sprintf("download output file %s: %v", staging.outputSourcePath, err)
			p.setStageOutStatus(key, transferFailed, "", msg)
			return podStatus(corev1.PodFailed, "StageOutFailed", msg)
		}
		if err := os.MkdirAll(filepath.Dir(localPath), 0o700); err != nil {
			msg := fmt.Sprintf("create output directory %s: %v", filepath.Dir(localPath), err)
			p.setStageOutStatus(key, transferFailed, "", msg)
			return podStatus(corev1.PodFailed, "StageOutFailed", msg)
		}
		if err := os.WriteFile(localPath, data, 0o600); err != nil {
			msg := fmt.Sprintf("write output file %s: %v", localPath, err)
			p.setStageOutStatus(key, transferFailed, "", msg)
			return podStatus(corev1.PodFailed, "StageOutFailed", msg)
		}
		p.setStageOutStatus(key, transferSucceeded, "", "")
		log.Printf("Pod %s output file %s downloaded to %s via SFAPI utilities", key, staging.outputSourcePath, localPath)
	}
	return podStatus(corev1.PodSucceeded, "StageOutComplete", "Output data staged out")
}

func (p *NerscProvider) stageOutSnapshot(key string) (podStagingState, transferStatus, string) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	staging := p.stagingMap[key]
	if staging == nil || !staging.hasStageOut() {
		return podStagingState{}, transferSucceeded, ""
	}
	return *staging, staging.outputStatus, staging.outputError
}

func (p *NerscProvider) setStageOutStatus(key string, status transferStatus, transferID, outputErr string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	staging := p.stagingMap[key]
	if staging == nil {
		return
	}
	staging.outputStatus = status
	if transferID != "" {
		staging.outputTransferID = transferID
	}
	staging.outputError = outputErr
}

func (p *NerscProvider) resolveLocalTransferPath(raw string) (string, error) {
	root := strings.TrimSpace(p.localTransferRoot)
	if root == "" {
		return "", fmt.Errorf("SFAPI transfer mode requires SFAPI_TRANSFER_LOCAL_ROOT")
	}
	if !filepath.IsAbs(root) {
		return "", fmt.Errorf("SFAPI_TRANSFER_LOCAL_ROOT must be an absolute path")
	}
	root = filepath.Clean(root)

	candidate := strings.TrimSpace(raw)
	if candidate == "" {
		return "", fmt.Errorf("local transfer path is empty")
	}
	if !filepath.IsAbs(candidate) {
		candidate = filepath.Join(root, candidate)
	}
	candidate = filepath.Clean(candidate)

	rel, err := filepath.Rel(root, candidate)
	if err != nil {
		return "", err
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) || filepath.IsAbs(rel) {
		return "", fmt.Errorf("local transfer path %q escapes SFAPI_TRANSFER_LOCAL_ROOT", raw)
	}
	return candidate, nil
}

func remoteShellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", `'"'"'`) + "'"
}

func podStatus(phase corev1.PodPhase, reason, message string) corev1.PodStatus {
	return corev1.PodStatus{
		Phase:   phase,
		Reason:  reason,
		Message: message,
	}
}
