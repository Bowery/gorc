// Copyright 2014, Orchestrate.IO, Inc.

package client

import (
	"net/http"
	"testing"
)

func TestNewClient(t *testing.T) {
	c := NewClient("")
	httpClient := c.httpClient

	if trans, ok := httpClient.Transport.(*http.Transport); ok {
		if trans.ResponseHeaderTimeout != Timeout {
			t.Error("Timeout not being set.")
		}

		if trans.MaxIdleConnsPerHost != MaxIdleConns {
			t.Error("MaxIdleConns not being set.")
		}
	}
}
