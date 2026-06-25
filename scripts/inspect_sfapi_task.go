package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"vk-provider-nersc/pkg/superfacility"
)

type credentialFile struct {
	ClientID string          `json:"client_id"`
	Secret   json.RawMessage `json:"secret"`
	JWK      json.RawMessage `json:"jwk"`
}

func main() {
	if len(os.Args) < 2 || len(os.Args) > 3 {
		fatalf("usage: go run ./scripts/inspect_sfapi_task.go <task-id-or-api-path> [credential-json]")
	}
	target := strings.TrimSpace(os.Args[1])
	if target == "" {
		fatalf("task id or API path is required")
	}
	credentialPath := "sf_api.json"
	if len(os.Args) == 3 {
		credentialPath = os.Args[2]
	}
	if credentialPath != "-" && !filepath.IsAbs(credentialPath) {
		if cwd, err := os.Getwd(); err == nil {
			credentialPath = filepath.Join(cwd, credentialPath)
		}
	}

	credential, err := readCredential(credentialPath)
	if err != nil {
		fatalf("%v", err)
	}
	source, err := superfacility.NewPrivateKeyJWTTokenSource(credential.ClientID, credential.JWK, superfacility.DefaultTokenURL)
	if err != nil {
		fatalf("%v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	token, err := source.Token(ctx)
	if err != nil {
		fatalf("%v", err)
	}

	endpoint := "https://api.nersc.gov/api/v1.2/" + apiPath(target)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		fatalf("%v", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fatalf("%v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fatalf("%v", err)
	}
	if resp.StatusCode != http.StatusOK {
		fatalf("SFAPI task request failed: %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}

	var pretty any
	if err := json.Unmarshal(body, &pretty); err != nil {
		fmt.Println(string(body))
		return
	}
	out, err := json.MarshalIndent(pretty, "", "  ")
	if err != nil {
		fatalf("%v", err)
	}
	fmt.Println(string(out))
}

func apiPath(target string) string {
	target = strings.TrimSpace(target)
	if strings.HasPrefix(target, "utilities/download/") {
		return target
	}
	target = strings.TrimLeft(target, "/")
	if strings.Contains(target, "/") || strings.Contains(target, "?") {
		return target
	}
	return "tasks/" + url.PathEscape(target)
}

func readCredential(path string) (struct {
	ClientID string
	JWK      []byte
}, error) {
	var data []byte
	var err error
	if path == "-" {
		data, err = io.ReadAll(os.Stdin)
		if err != nil {
			return struct {
				ClientID string
				JWK      []byte
			}{}, fmt.Errorf("read credential from stdin: %w", err)
		}
	} else {
		data, err = os.ReadFile(path)
		if err != nil {
			return struct {
				ClientID string
				JWK      []byte
			}{}, fmt.Errorf("read credential file %s: %w", path, err)
		}
	}

	var file credentialFile
	if err := json.Unmarshal(data, &file); err != nil {
		return struct {
			ClientID string
			JWK      []byte
		}{}, fmt.Errorf("decode credential file %s: %w", path, err)
	}
	clientID := strings.TrimSpace(file.ClientID)
	if clientID == "" {
		return struct {
			ClientID string
			JWK      []byte
		}{}, fmt.Errorf("credential file %s missing client_id", path)
	}

	jwk := file.JWK
	if len(jwk) == 0 {
		jwk = file.Secret
	}
	if len(jwk) == 0 {
		return struct {
			ClientID string
			JWK      []byte
		}{}, fmt.Errorf("credential file %s missing secret or jwk", path)
	}

	var jwkString string
	if err := json.Unmarshal(jwk, &jwkString); err == nil {
		jwkString = strings.TrimSpace(jwkString)
		if jwkString == "" {
			return struct {
				ClientID string
				JWK      []byte
			}{}, fmt.Errorf("credential file %s has empty jwk", path)
		}
		jwk = []byte(jwkString)
	}
	if !json.Valid(jwk) {
		return struct {
			ClientID string
			JWK      []byte
		}{}, fmt.Errorf("credential file %s has invalid jwk json", path)
	}

	return struct {
		ClientID string
		JWK      []byte
	}{ClientID: clientID, JWK: jwk}, nil
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
