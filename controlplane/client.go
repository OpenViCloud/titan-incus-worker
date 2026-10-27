// Package controlplane calls the control plane: the /internal routes with X-Internal-Token, and the anonymous catalog
// routes without it. Any 2xx is success.
package controlplane

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const maxErrorBody = 4 << 10 // 4 KiB

// HTTPError: the control plane answered and said no.
type HTTPError struct {
	Method     string
	Path       string
	StatusCode int
	Body       string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("control plane %s %s returned HTTP %d: %s", e.Method, e.Path, e.StatusCode, e.Body)
}

// TransportError: the request never got an answer (connection refused, timeout, a body cut off mid-read).
type TransportError struct {
	Err error
}

func (e *TransportError) Error() string {
	return e.Err.Error()
}

func (e *TransportError) Unwrap() error {
	return e.Err
}

type Client struct {
	baseUrl string
	token   string
	http    *http.Client
}

func New(baseUrl string, token string) *Client {
	return &Client{
		baseUrl: strings.TrimRight(baseUrl, "/"),
		token:   token,
		http:    &http.Client{Timeout: 30 * time.Second},
	}
}

// NewWithHTTPClient is for tests that point the client at an httptest server.
func NewWithHTTPClient(baseUrl string, token string, httpClient *http.Client) *Client {
	return &Client{baseUrl: strings.TrimRight(baseUrl, "/"), token: token, http: httpClient}
}

// Put, Patch: payload nil sends no body.
func (c *Client) Put(ctx context.Context, path string, payload any) error {
	_, err := c.send(ctx, http.MethodPut, path, payload)
	return err
}

func (c *Client) Patch(ctx context.Context, path string, payload any) error {
	_, err := c.send(ctx, http.MethodPatch, path, payload)
	return err
}

// Get returns the whole body of a 2xx.
func (c *Client) Get(ctx context.Context, path string) ([]byte, error) {
	return c.send(ctx, http.MethodGet, path, nil)
}

// GetJSON decodes a 2xx body into target.
func (c *Client) GetJSON(ctx context.Context, path string, target any) error {
	body, err := c.Get(ctx, path)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(body, target); err != nil {
		return fmt.Errorf("control plane GET %s: decode: %w", path, err)
	}
	return nil
}

func (c *Client) send(ctx context.Context, method string, path string, payload any) ([]byte, error) {
	var requestBody io.Reader
	hasPayload := payload != nil
	if hasPayload {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("control plane %s %s: encode: %w", method, path, err)
		}
		requestBody = bytes.NewReader(encoded)
	}

	request, err := http.NewRequestWithContext(ctx, method, c.baseUrl+path, requestBody)
	if err != nil {
		return nil, fmt.Errorf("control plane %s %s: build request: %w", method, path, err)
	}
	if hasPayload {
		request.Header.Set("Content-Type", "application/json")
	}
	request.Header.Set("X-Internal-Token", c.token)

	response, err := c.http.Do(request)
	if err != nil {
		return nil, &TransportError{Err: fmt.Errorf("control plane %s %s: %w", method, path, err)}
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, &TransportError{Err: fmt.Errorf("control plane %s %s: read body: %w", method, path, err)}
	}
	isSuccess := response.StatusCode >= 200 && response.StatusCode < 300
	if !isSuccess {
		if len(body) > maxErrorBody {
			body = body[:maxErrorBody]
		}
		return nil, &HTTPError{Method: method, Path: path, StatusCode: response.StatusCode, Body: string(body)}
	}
	return body, nil
}

// StatusCodeOf returns the control plane's HTTP status for an *HTTPError, 0 for anything else.
func StatusCodeOf(err error) int {
	var httpErr *HTTPError
	if errors.As(err, &httpErr) {
		return httpErr.StatusCode
	}
	return 0
}

// Permanent: a 4xx is the control plane refusing this request for good, except 409 and 429, which are about timing.
// A 5xx may pass on a later try. A TransportError has no Permanent, so it is retried too.
func (e *HTTPError) Permanent() bool {
	isClientError := e.StatusCode >= 400 && e.StatusCode < 500
	if !isClientError {
		return false
	}
	isAboutTiming := e.StatusCode == http.StatusConflict || e.StatusCode == http.StatusTooManyRequests
	return !isAboutTiming
}
