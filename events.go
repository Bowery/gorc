// Copyright 2014, Orchestrate.IO, Inc.

package client

import (
	"bytes"
	"io"
)

func (client *Client) GetEvents(collection, key, kind string) (*bytes.Buffer, error) {
	resp, err := client.doRequest("GET", collection+"/"+key+"/events/"+kind, nil)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, newError(resp)
	}

	// TODO: See if there is a content-length header so we can pre-allocate
	// space to fit the contents.
	buf := bytes.NewBuffer(nil)
	_, err = buf.ReadFrom(resp.Body)

	return buf, err
}

func (client *Client) PutEvent(collection, key, kind string, value io.Reader) error {
	resp, err := client.doRequest("PUT", collection+"/"+key+"/events/"+kind, value)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 204 {
		return newError(resp)
	}
	return nil
}
