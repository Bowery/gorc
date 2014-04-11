// Copyright 2014, Orchestrate.IO, Inc.

// A client for use with Orchestrate.io: http://orchestrate.io/
//
// Orchestrate unifies multiple databases through one simple REST API.
// Orchestrate runs as a service and supports queries like full-text
// search, events, graph, and key/value.
//
// You can sign up for an Orchestrate account here:
// http://dashboard.orchestrate.io
package client

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

// The root path for all API endpoints.
const rootUri = "https://api.orchestrate.io/v0/"

var (
	Timeout                        = 3 * time.Second
	MaxIdleConns                   = 40
	Transport    http.RoundTripper = &http.Transport{
		MaxIdleConnsPerHost:   MaxIdleConns,
		ResponseHeaderTimeout: Timeout,
		Dial: func(network, addr string) (net.Conn, error) {
			return net.DialTimeout(network, addr, Timeout)
		},
	}
)

type Client struct {
	httpClient *http.Client
	authToken  string
}

// An implementation of 'error' that exposes all the orchestrate specific
// error details.
type OrchestrateError struct {
	Status     string `json:"-"`
	StatusCode int    `json:"-"`
	Message    string `json:"message"`
	Code       string `json:"code"`
}

type Path struct {
	Collection string `json:"collection"`
	Key        string `json:"key"`
	Ref        string `json:"ref"`
}

// Returns a new Client object that will use the given authToken for
// authorization against Orchestrate. This token can be obtained
// at http://dashboard.orchestrate.io
func NewClient(authToken string) *Client {
	return &Client{
		httpClient: &http.Client{Transport: Transport},
		authToken:  authToken,
	}
}

// Creates a new OrchestrateError from a given http.Response object.
func newError(resp *http.Response) error {
	decoder := json.NewDecoder(resp.Body)
	orchestrateError := new(OrchestrateError)
	decoder.Decode(orchestrateError)

	orchestrateError.Status = resp.Status
	orchestrateError.StatusCode = resp.StatusCode

	return orchestrateError
}

func (e OrchestrateError) Error() string {
	return fmt.Sprintf(`%v (%v): %v`, e.Status, e.StatusCode, e.Message)
}

func (client *Client) doRequest(method, trailing string, headers map[string]string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest(method, rootUri+trailing, body)
	if err != nil {
		return nil, err
	}

	req.SetBasicAuth(client.authToken, "")

	for k, v := range headers {
		req.Header.Add(k, v)
	}

	if method == "PUT" {
		req.Header.Add("Content-Type", "application/json")
	}

	return client.httpClient.Do(req)
}
