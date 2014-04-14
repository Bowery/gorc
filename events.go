// Copyright 2014, Orchestrate.IO, Inc.

package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"strconv"
)

// Holds results returned from an Events query.
type EventResults struct {
	Count   uint64  `json:"count"`
	Results []Event `json:"results"`
}

// An individual event.
type Event struct {
	Ordinal   uint64          `json:"ordinal"`
	Timestamp uint64          `json:"timestamp"`
	RawValue  json.RawMessage `json:"value"`
}

// Get all events of a particular type in specified collection-key pair in a range.
func (client *Client) GetEvents(collection string, key string, kind string, start int64, end int64) (*EventResults, error) {
	queryVariables := url.Values{
		"start": []string{strconv.FormatInt(start, 10)},
		"end":   []string{strconv.FormatInt(end, 10)},
	}

	trailingUri := fmt.Sprintf("%s/%s/events/%s?%s", collection, key, kind, queryVariables.Encode())

	resp, err := client.doRequest("GET", trailingUri, nil, nil)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, newError(resp)
	}

	decoder := json.NewDecoder(resp.Body)
	results := new(EventResults)
	err = decoder.Decode(results)

	if err != nil {
		return nil, err
	}

	return results, err
}

// Put an event of the specified type to provided collection-key pair.
func (client *Client) PutEvent(collection, key, kind string, value interface{}) error {
	buf := new(bytes.Buffer)
	encoder := json.NewEncoder(buf)

	if err := encoder.Encode(value); err != nil {
		return err
	}

	return client.PutEventRaw(collection, key, kind, buf)
}

// Put an event of the specified type to provided collection-key pair.
func (client *Client) PutEventRaw(collection, key, kind string, value io.Reader) error {
	resp, err := client.doRequest("PUT", collection+"/"+key+"/events/"+kind, nil, value)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 204 {
		return newError(resp)
	}
	return nil
}

// Marshall the value of an event into the provided object.
func (result *Event) Value(value interface{}) error {
	return json.Unmarshal(result.RawValue, value)
}
