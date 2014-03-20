// Copyright 2014, Orchestrate.IO, Inc.

package client

import (
	"bytes"
	"io"
)

func (client *Client) Get(collection, key string) (*bytes.Buffer, error) {
	resp, err := client.doRequest("GET", collection+"/"+key, nil)

	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, newError(resp)
	}

	// TODO: Check for a content-length header so we can pre-allocate buffer
	// space.
	buf := bytes.NewBuffer(nil)
	if _, err := buf.ReadFrom(resp.Body); err != nil {
		return nil, err
	}

	return buf, nil
}

func (client *Client) Put(collection, key string, value io.Reader) error {
	resp, err := client.doRequest("PUT", collection+"/"+key, value)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 201 {
		return newError(resp)
	}

	return nil
}

func (client *Client) Delete(collection, key string) error {
	resp, err := client.doRequest("DELETE", collection+"/"+key, nil)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 204 {
		return newError(resp)
	}

	return nil
}
