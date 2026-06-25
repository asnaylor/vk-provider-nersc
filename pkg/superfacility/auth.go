package superfacility

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	DefaultTokenURL     = "https://oidc.nersc.gov/c2id/token"
	clientAssertionType = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer"
	tokenRefreshLeeway  = time.Minute
)

type PrivateKeyJWTTokenSource struct {
	ClientID  string
	TokenURL  string
	Key       *rsa.PrivateKey
	HTTP      *http.Client
	Now       func() time.Time
	assertion func(time.Time) (string, error)

	mu     sync.Mutex
	cached cachedToken
}

type cachedToken struct {
	AccessToken string
	ExpiresAt   time.Time
}

type tokenResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int    `json:"expires_in"`
}

type rsaPrivateJWK struct {
	KeyType string `json:"kty"`
	KeyID   string `json:"kid"`
	N       string `json:"n"`
	E       string `json:"e"`
	D       string `json:"d"`
	P       string `json:"p"`
	Q       string `json:"q"`
	DP      string `json:"dp"`
	DQ      string `json:"dq"`
	QI      string `json:"qi"`
}

func NewPrivateKeyJWTTokenSource(clientID string, jwkJSON []byte, tokenURL string) (*PrivateKeyJWTTokenSource, error) {
	clientID = strings.TrimSpace(clientID)
	if clientID == "" {
		return nil, fmt.Errorf("SFAPI client_id is required")
	}
	if strings.TrimSpace(tokenURL) == "" {
		tokenURL = DefaultTokenURL
	}
	parsedURL, err := url.Parse(tokenURL)
	if err != nil {
		return nil, fmt.Errorf("invalid SFAPI token URL: %w", err)
	}
	if parsedURL.Scheme == "" || parsedURL.Host == "" {
		return nil, fmt.Errorf("invalid SFAPI token URL: must include scheme and host")
	}

	key, _, err := ParseRSAPrivateJWK(jwkJSON)
	if err != nil {
		return nil, err
	}

	return &PrivateKeyJWTTokenSource{
		ClientID: clientID,
		TokenURL: strings.TrimSpace(tokenURL),
		Key:      key,
		HTTP:     &http.Client{Timeout: 30 * time.Second},
		Now:      time.Now,
	}, nil
}

func ParseRSAPrivateJWK(raw []byte) (*rsa.PrivateKey, string, error) {
	var jwk rsaPrivateJWK
	if err := json.Unmarshal(raw, &jwk); err != nil {
		return nil, "", fmt.Errorf("decode RSA JWK: %w", err)
	}
	if jwk.KeyType != "" && jwk.KeyType != "RSA" {
		return nil, "", fmt.Errorf("unsupported JWK key type %q", jwk.KeyType)
	}

	required := map[string]string{
		"n": jwk.N,
		"e": jwk.E,
		"d": jwk.D,
		"p": jwk.P,
		"q": jwk.Q,
	}
	for name, value := range required {
		if strings.TrimSpace(value) == "" {
			return nil, "", fmt.Errorf("RSA JWK missing %q", name)
		}
	}

	n, err := decodeJWKBigInt(jwk.N)
	if err != nil {
		return nil, "", fmt.Errorf("decode JWK n: %w", err)
	}
	e, err := decodeJWKBigInt(jwk.E)
	if err != nil {
		return nil, "", fmt.Errorf("decode JWK e: %w", err)
	}
	if !e.IsInt64() || e.Int64() <= 1 || e.Int64() > math.MaxInt32 {
		return nil, "", fmt.Errorf("JWK e is not a supported public exponent")
	}
	d, err := decodeJWKBigInt(jwk.D)
	if err != nil {
		return nil, "", fmt.Errorf("decode JWK d: %w", err)
	}
	p, err := decodeJWKBigInt(jwk.P)
	if err != nil {
		return nil, "", fmt.Errorf("decode JWK p: %w", err)
	}
	q, err := decodeJWKBigInt(jwk.Q)
	if err != nil {
		return nil, "", fmt.Errorf("decode JWK q: %w", err)
	}

	key := &rsa.PrivateKey{
		PublicKey: rsa.PublicKey{
			N: n,
			E: int(e.Int64()),
		},
		D:      d,
		Primes: []*big.Int{p, q},
	}
	if err := key.Validate(); err != nil {
		return nil, "", fmt.Errorf("validate RSA JWK: %w", err)
	}
	key.Precompute()
	return key, jwk.KeyID, nil
}

func (s *PrivateKeyJWTTokenSource) Token(ctx context.Context) (string, error) {
	if s == nil {
		return "", fmt.Errorf("SFAPI token source is not configured")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	if s.cached.AccessToken != "" && now.Add(tokenRefreshLeeway).Before(s.cached.ExpiresAt) {
		return s.cached.AccessToken, nil
	}

	token, err := s.fetchToken(ctx, now)
	if err != nil {
		return "", err
	}
	s.cached = token
	return token.AccessToken, nil
}

func (s *PrivateKeyJWTTokenSource) fetchToken(ctx context.Context, now time.Time) (cachedToken, error) {
	assertion, err := s.clientAssertion(now)
	if err != nil {
		return cachedToken{}, err
	}

	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	form.Set("client_id", s.ClientID)
	form.Set("client_assertion_type", clientAssertionType)
	form.Set("client_assertion", assertion)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.TokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return cachedToken{}, fmt.Errorf("create SFAPI token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := s.httpClient().Do(req)
	if err != nil {
		return cachedToken{}, fmt.Errorf("fetch SFAPI token: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodyBytes))
		if readErr != nil {
			return cachedToken{}, fmt.Errorf("SFAPI token request failed: %s", resp.Status)
		}
		return cachedToken{}, fmt.Errorf("SFAPI token request failed: %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}

	var out tokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return cachedToken{}, fmt.Errorf("decode SFAPI token response: %w", err)
	}
	out.AccessToken = strings.TrimSpace(out.AccessToken)
	if out.AccessToken == "" {
		return cachedToken{}, fmt.Errorf("SFAPI token response missing access_token")
	}
	if out.ExpiresIn <= 0 {
		out.ExpiresIn = int((10 * time.Minute).Seconds())
	}

	return cachedToken{
		AccessToken: out.AccessToken,
		ExpiresAt:   now.Add(time.Duration(out.ExpiresIn) * time.Second),
	}, nil
}

func (s *PrivateKeyJWTTokenSource) clientAssertion(now time.Time) (string, error) {
	if s.assertion != nil {
		return s.assertion(now)
	}
	if s.Key == nil {
		return "", fmt.Errorf("SFAPI RSA private key is not configured")
	}

	header := map[string]string{
		"alg": "RS256",
		"typ": "JWT",
	}
	claims := map[string]any{
		"iss": s.ClientID,
		"sub": s.ClientID,
		"aud": s.TokenURL,
		"iat": now.Unix(),
		"exp": now.Add(5 * time.Minute).Unix(),
		"jti": randomID(),
	}

	encodedHeader, err := encodeJWTPart(header)
	if err != nil {
		return "", err
	}
	encodedClaims, err := encodeJWTPart(claims)
	if err != nil {
		return "", err
	}

	signingInput := encodedHeader + "." + encodedClaims
	digest := sha256.Sum256([]byte(signingInput))
	signature, err := rsa.SignPKCS1v15(rand.Reader, s.Key, crypto.SHA256, digest[:])
	if err != nil {
		return "", fmt.Errorf("sign SFAPI client assertion: %w", err)
	}
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}

func (s *PrivateKeyJWTTokenSource) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

func (s *PrivateKeyJWTTokenSource) httpClient() *http.Client {
	if s.HTTP != nil {
		return s.HTTP
	}
	return &http.Client{Timeout: 30 * time.Second}
}

func encodeJWTPart(v any) (string, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return "", fmt.Errorf("marshal JWT part: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(data), nil
}

func randomID() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return base64.RawURLEncoding.EncodeToString(buf)
}

func decodeJWKBigInt(raw string) (*big.Int, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return nil, fmt.Errorf("empty value")
	}
	data, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		data, err = base64.URLEncoding.DecodeString(value)
	}
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("empty decoded value")
	}
	return new(big.Int).SetBytes(data), nil
}
