// Copyright 2014 Orchestrate, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// A client for use with Orchestrate.io: http://orchestrate.io/
//
// Orchestrate unifies multiple databases through one simple REST API.
// Orchestrate runs as a service and supports queries like full-text
// search, events, graph, and key/value.
//
// You can sign up for an Orchestrate account here:
// http://dashboard.orchestrate.io
package gorc

import (
	"compress/flate"
	"compress/gzip"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"sync/atomic"
)

//
// gorc2
//

// All changes past this point were added in order to make conversion from
// gorc to gorc2 easier. Most of these calls backports gorc2 functionality and
// API into the gorc library.

//
// Collection
//

// Represents a Collection in Orchestrate.
//
// This type is gorc2 compatible.
type Collection struct {
	// The unique name of this collection.
	Name string

	// A reference back to the Client that created this Collection.
	client *Client
}

//
// Client
//

// Returns a Collection object for a collection with the given name. Note that
// this call does not verify that the collection exists however most operations
// will create it automatically. To ensure the collection exists use
// CreateCollection().
//
// This func is gorc2 compatible.
func (c *Client) Collection(name string) *Collection {
	return &Collection{
		client: c,
		Name:   name,
	}
}

// Creates a new collection (if it doesn't already exist) in the data store.
//
// This function is gorc2 compatible.
func (c *Client) CreateCollection(name string) (*Collection, error) {
	// To create a collection we just list it with a limit of zero.
	path := name + "?limit=0"
	if _, err := c.emptyReply("GET", path, nil, nil, 200); err != nil {
		return nil, err
	}
	return &Collection{
		client: c,
		Name:   name,
	}, nil
}

// Deletes a collection. This returns without error even if the collection
// didn't already exist.
//
// This function is gorc2 compatible.
func (c *Client) DeleteCollection(name string) error {
    path := name + "?force=true"
	_, err := c.emptyReply("DELETE", path, nil, nil, 204)
	return err
}

// Check that Orchestrate is reachable.
//
// This function is gorc2 compatible.
func (c *Client) Ping() error {
	_, err := c.emptyReply("HEAD", "", nil, nil, 200)
	return err
}

//
// Private
//

// Executes an HTTP request.
func (c *Client) doRequest(
	method, trailing string, headers map[string]string, body io.Reader,
) (*http.Response, error) {
	// Get the URL that we should be talking too.
	host := c.APIHost
	if host == "" {
		host = DefaultAPIHost
	}
	url := "http://" + host + "/v0/" + trailing

	// Create the new Request.
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}

	// Ensure that the query gets the authToken as username.
	req.SetBasicAuth(c.authToken, "")

	// Add any headers that the client provided.
	for k, v := range headers {
		req.Header.Add(k, v)
	}
	if atomic.LoadInt32(&c.deprecated) == 0 {
		req.Header.Add("User-Agent", userAgent)
	} else {
		req.Header.Add("User-Agent", userAgentDeprecated)
	}

	// If the client request has a body then we need to set a Content-Type
	// header.
	if body != nil {
		req.Header.Add("Content-Type", "application/json")
	}

	// If the HTTPClient is nil we use the DefaultTransport provided in this
	// package, otherwise we use the specific HTTPClient that the caller set
	// in the client object.
	client := c.HTTPClient
	if client == nil {
		client = &http.Client{Transport: DefaultTransport}
	}
	return client.Do(req)
}

// This call will perform a simple request which expects no body to be
// returned. These are typically sued with POST/PUT/DELETE type calls which
// expect no response from the server.
//
// Any status return other than 'status' will cause an error to be returned
// from this function.
func (c *Client) emptyReply(
	method, path string, headers map[string]string, body io.Reader, status int,
) (*http.Response, error) {
	resp, err := c.doRequest(method, path, headers, body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// Check the status code.
	if resp.StatusCode != status {
		return nil, newError(resp)
	}

	// Read the whole body to ensure that the connections can be reused. Note
	// that we don't bother checking errors here since an error will not impact
	// the code path at all.
	io.Copy(ioutil.Discard, resp.Body)

	// Success!
	return resp, nil
}

// This call will perform a request which expects a JSON body to be returned.
// The contents of the body will be decoded into the value given.
//
// Any status return other than 'status' will cause an error to be returned
// from this function.
func (c *Client) jsonReply(
	method, path string, body io.Reader, status int, value interface{},
) (*http.Response, error) {
	resp, err := c.doRequest(method, path, nil, body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// Ensure that the returned status was expected.
	if resp.StatusCode != status {
		return nil, newError(resp)
	}

	// See what kind of encoding the server is replying with.
	var decoder *json.Decoder
	switch resp.Header.Get("Content-Encoding") {
	case "gzip":
		gzipReader, err := gzip.NewReader(resp.Body)
		if err != nil {
			return nil, err
		}
		decoder = json.NewDecoder(gzipReader)
	case "deflate":
		decoder = json.NewDecoder(flate.NewReader(resp.Body))
	default:
		decoder = json.NewDecoder(resp.Body)
	}

	// Decode the body into a json object.
	if err := decoder.Decode(value); err != nil {
		return nil, err
	}

	// Success!
	return resp, nil
}
