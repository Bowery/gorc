// Copyright 2014, Orchestrate.IO, Inc.

package gorc

import (
	"encoding/json"
	"io"
	"net/url"
	"strconv"
	"strings"
)

// Holds results returned from an Events query.
type EventResults struct {
	Count   uint64  `json:"count"`
	Results []Event `json:"results"`
	Next    string  `json:"next,omitempty"`
}

type EventPath struct {
	Collection string  `json:"collection"`
	Key        string  `json:"key"`
	Kind       string  `json:"type"`
	Timestamp  *int64  `json:"timestamp"`
	Ordinal    *uint64 `json:"ordinal"`
	Ref        *string `json:"ref"`
}

// An individual event.
type Event struct {
	Path     EventPath       `json:"path"`
	RawValue json.RawMessage `json:"value"`
}

// Get a particular event.
func (c *Client) GetEvent(path *EventPath) (*Event, error) {
	return c.doGetEvent(path)
}

// Get latest events of a particular type from specified collection-key pair.
func (c *Client) GetEvents(collection, key, kind string) (*EventResults, error) {
	trailingUri := collection + "/" + key + "/events/" + kind

	return c.doGetEvents(trailingUri)
}

// Get all events of a particular type from specified collection-key pair in a
// range.
func (c *Client) GetEventsInRange(collection, key, kind string, start int64, end int64) (*EventResults, error) {
	queryVariables := url.Values{
		"start": []string{strconv.FormatInt(start, 10)},
		"end":   []string{strconv.FormatInt(end, 10)},
	}

	trailingUri := collection + "/" + key + "/events/" + kind + "?" + queryVariables.Encode()

	return c.doGetEvents(trailingUri)
}

// Post an event of the specified type to provided collection-key pair.
func (c *Client) PostEvent(collection, key, kind string, value interface{}) (*EventPath, error) {
	reader, writer := io.Pipe()
	encoder := json.NewEncoder(writer)

	go func() { writer.CloseWithError(encoder.Encode(value)) }()
	return c.PostEventRaw(collection, key, kind, reader)
}

// Post an event of the specified type to provided collection-key pair.
func (c *Client) PostEventRaw(collection, key, kind string, value io.Reader) (*EventPath, error) {
	path := EventPath{Collection: collection, Key: key, Kind: kind}

	return c.doPostEvent(&path, value)

}

// Post an event of the specified type to provided collection-key pair and time.
func (c *Client) PostEventWithTime(collection, key, kind string, time int64, value interface{}) (*EventPath, error) {
	reader, writer := io.Pipe()
	encoder := json.NewEncoder(writer)

	go func() { writer.CloseWithError(encoder.Encode(value)) }()
	return c.PostEventWithTimeRaw(collection, key, kind, time, reader)
}

// Post an event of the specified type to provided collection-key pair and time.
func (c *Client) PostEventWithTimeRaw(collection, key, kind string, time int64, value io.Reader) (*EventPath, error) {
	path := EventPath{Collection: collection, Key: key, Kind: kind, Timestamp: &time}

	return c.doPostEvent(&path, value)
}

// Execute event get.
func (c *Client) doGetEvent(path *EventPath) (*Event, error) {
	resp, err := c.doRequest("GET", path.trailingUri(), nil, nil)

	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, newError(resp)
	}

	decoder := json.NewDecoder(resp.Body)
	event := new(Event)
	if err = decoder.Decode(event); err != nil {
		return nil, err
	}

	return event, err
}

// Execute events get.
func (c *Client) doGetEvents(trailingUri string) (*EventResults, error) {
	resp, err := c.doRequest("GET", trailingUri, nil, nil)

	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, newError(resp)
	}

	decoder := json.NewDecoder(resp.Body)
	results := new(EventResults)
	if err = decoder.Decode(results); err != nil {
		return nil, err
	}

	return results, err
}

// Execute event post.
func (c *Client) doPostEvent(path *EventPath, value io.Reader) (*EventPath, error) {
	resp, err := c.doRequest("POST", path.trailingUri(), nil, value)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 201 {
		return nil, newError(resp)
	}

	location := strings.SplitAfter(resp.Header.Get("Location"), "/")
	timestamp, _ := strconv.ParseInt(location[5], 10, 64)
	ordinal, _ := strconv.ParseUint(location[6], 10, 64)
	etag := resp.Header.Get("Etag")
	ref := etag[1 : len(etag)-1]

	return &EventPath{
		Collection: path.Collection,
		Key:        path.Key,
		Kind:       path.Kind,
		Timestamp:  &timestamp,
		Ordinal:    &ordinal,
		Ref:        &ref,
	}, nil
}

func (ep *EventPath) trailingUri() string {
	if ep.Timestamp == nil {
		return ep.Collection + "/" + ep.Key + "/events/" + ep.Kind
	}

	if ep.Ordinal != nil {
		return ep.Collection + "/" + ep.Key + "/events/" + ep.Kind + "/" + strconv.FormatInt(*ep.Timestamp, 10) + "/" + strconv.FormatUint(*ep.Ordinal, 10)
	}

	return ep.Collection + "/" + ep.Key + "/events/" + ep.Kind + "/" + strconv.FormatInt(*ep.Timestamp, 10)
}

// Marshall the value of an event into the provided object.
func (r *Event) Value(value interface{}) error {
	return json.Unmarshal(r.RawValue, value)
}
