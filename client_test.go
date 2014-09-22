// Copyright 2014, Orchestrate.IO, Inc.

package gorc

import (
	"net/http"
	"testing"

	"github.com/liquidgecka/testlib"
)

func TestNewClient(t *testing.T) {
	T := testlib.NewT(t)
	client := NewClient("XXX")
	T.Equal(client.authToken, "XXX")
	T.Equal(client.httpClient.Transport, DefaultTransport)
}

func TestNewClientWithTransport(t *testing.T) {
	T := testlib.NewT(t)
	client := NewClientWithTransport("XXX", http.DefaultTransport)
	T.Equal(client.authToken, "XXX")
	T.Equal(client.httpClient.Transport, http.DefaultTransport)
}

func TestPing(t *testing.T) {
	T := testlib.NewT(t)
	client := cleanTestingClient(T)

	// Change the root URI to ensure that the method always returns an error.
	// Also schedule a defer to ensure that it gets cleaned up.
	func() {
		defer func(s string) {
			rootUri = s
		}(rootUri)
		rootUri = "://bad/url/base"
		T.ExpectError(client.Ping())
	}()

	// Next we change the root uri to ensure that the client returns a 404
	// error so the StatusCode check fails.
	func() {
		defer func(s string) {
			rootUri = s
		}(rootUri)
		rootUri = "https://api.orchestrate.io/golang_unittest"
		T.ExpectError(client.Ping())
	}()

	// And lastly we try the check without any fooling around to ensure that
	// it returns successfully.
	T.ExpectSuccess(client.Ping())
}
