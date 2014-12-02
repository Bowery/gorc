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

package gorc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// ----------------------------------------------------------------------------
// ---------------------------------- gorc2 -----------------------------------
// ----------------------------------------------------------------------------

// All changes past this point were added in order to make conversion from
// gorc to gorc2 easier. Most of these calls backports gorc2 functionality and
// API into the gorc library.

// Internal type that represents the reply form a JSON event fetch.
type jsonEvent struct {
	Ordinal   int64           `json:"ordinal"`
	Path      jsonPath        `json:"path"`
	Timestamp int64           `json:"timestamp"`
	Value     json.RawMessage `json:"value"`
}

//
// Event
//

// Represents a single Event in a Collection.
//
// In gorc2 this type is "Event", however that name was already used in gorc.
type Event2 struct {
	// The collection that this Event is attached too.
	Collection *Collection

	// The Item Key that this Event is attached too.
	Key string

	// The update Ordinal for this event.
	Ordinal int64

	// The Reference number for this specific event.
	Ref string

	// The user supplied Timestamp associated with this event.
	Timestamp time.Time

	// The user supplied Type associated with this event.
	Type string

	// The raw JSON value.
	Value json.RawMessage
}

// Deletes the Event2 if it is the most recent event for the given key, time
// stamp, and ordinal pairing. This will return an error if the event has
// been updated via a prior call to Update() or Delete().
//
// This call is gorc2 compatible other than changing the type from Event2 to
// Event.
func (e *Event2) Delete() error {
	headers := map[string]string{"If-Match": `"` + e.Ref + `"`}
	path := fmt.Sprintf("%s/%s/events/%s/%d/%d?purge=true",
		e.Collection.Name, e.Key, e.Type, e.Timestamp.UnixNano()/1000000,
		e.Ordinal)
	_, err := e.Collection.client.emptyReply("DELETE", path, headers, nil, 204)
	if err != nil {
		if _, ok := err.(PreconditionFailedError); ok {
			err = NotMostRecentError(e.Ref)
		}
	}
	return err
}

// Unmarshal's the data from 'Value' into the given item.
//
// This call is gorc2 compatible other than changing the type from Event2 to
// Event.
func (e *Event2) Unmarshal(value interface{}) error {
	return json.Unmarshal(e.Value, value)
}

// Updates this event if it represents the most recent event for the key,
// timestamp, and ordinal pairing. This will return an error if the event has
// already been updated via a prior call to Event2.Update().
//
// This call is gorc2 compatible other than changing the type from Event2 to
// Event.
func (e *Event2) Update(value interface{}) (*Event2, error) {
	headers := map[string]string{
		"If-Match":     `"` + e.Ref + `"`,
		"Content-Type": "application/json",
	}
	event, err := e.Collection.innerUpdateEvent(e.Key, e.Type, e.Timestamp,
		e.Ordinal, value, headers)
	if err != nil {
		if _, ok := err.(PreconditionFailedError); ok {
			err = NotMostRecentError(e.Ref)
		}
	}
	return event, err
}

//
// AddEvent
//

// Adds a new event to the collection with the given key, and type. The
// timestamp of the new event will be set by the Orchestrate server to the
// time that the request was processed. Unlike Create this function will
// created an event even if an event already exists with that tuple. The
// new event will be given a new Ordinal value. To update and existing
// Event2 use UpdateEvent() instead.
//
// Note that the key should exist otherwise this call will have unpredictable
// results.
//
// This call is gorc2 compatible other than changing the type from Event2 to
// Event.
func (c *Collection) AddEvent(
	key, typ string, value interface{},
) (*Event2, error) {
	return c.innerAddEvent(key, typ, nil, value)
}

// Like AddEvent() except this lets you specify the timestamp that will be
// attached to the event.
func (c *Collection) AddEventWithTimestamp(
	key, typ string, ts time.Time, value interface{},
) (*Event2, error) {
	return c.innerAddEvent(key, typ, &ts, value)
}

// Inner implementation of AddEvent*
func (c *Collection) innerAddEvent(
	key, typ string, ts *time.Time, value interface{},
) (*Event2, error) {
	event := &Event2{
		Collection: c,
		Key:        key,
		Type:       typ,
	}

	// Encode the JSON message into a raw value that we can return to the
	// client if necessary.
	if rawMsg, err := json.Marshal(value); err != nil {
		return nil, err
	} else {
		event.Value = json.RawMessage(rawMsg)
	}

	// Perform the actual POST
	headers := map[string]string{"Content-Type": "application/json"}
	var path string
	if ts != nil {
		path = fmt.Sprintf("%s/%s/events/%s/%d", c.Name, key, typ,
			ts.UnixNano()/1000000)
	} else {
		path = fmt.Sprintf("%s/%s/events/%s", c.Name, key, typ)
	}
	resp, err := c.client.emptyReply("POST", path, headers,
		bytes.NewBuffer(event.Value), 201)
	if err != nil {
		return nil, err
	}

	// Get the Location header and parse it. The Header will give us the
	// Ordinal.
	location := resp.Header.Get("Location")
	if location == "" {
		return nil, fmt.Errorf("Missing Location header.")
	} else if parts := strings.Split(location, "/"); len(parts) != 8 {
		return nil, fmt.Errorf("Malformed Location header.")
	} else if ts, err := strconv.ParseInt(parts[6], 10, 64); err != nil {
		return nil, fmt.Errorf("Malformed Timestamp in the Location header.")
	} else if ord, err := strconv.ParseInt(parts[7], 10, 64); err != nil {
		return nil, fmt.Errorf("Malformed Ordinal in the Location header.")
	} else {
		secs := ts / 1000
		nsecs := (ts % 1000) * 1000000
		event.Timestamp = time.Unix(secs, nsecs)
		event.Ordinal = ord
	}

	// Get the Ref via the Etag header.
	if etag := resp.Header.Get("Etag"); etag == "" {
		return nil, fmt.Errorf("Missing ETag header.")
	} else if parts := strings.Split(etag, `"`); len(parts) != 3 {
		return nil, fmt.Errorf("Malformed ETag header.")
	} else {
		event.Ref = parts[1]
	}

	// Success
	return event, nil
}

//
// DeleteEvent
//

// Removes an event from the collection. This succeeds even if the event did
// not exist prior to this call. Note that all event deletes are Final and can
// not be undone.
//
// This call is gorc2 compatible other than changing the type from Event2 to
// Event.
func (c *Collection) DeleteEvent(
	key, typ string, ts time.Time, ordinal int64,
) error {
	path := fmt.Sprintf("%s/%s/events/%s/%d/%d?purge=true",
		c.Name, key, typ, ts.UnixNano()/1000000, ordinal)
	_, err := c.client.emptyReply("DELETE", path, nil, nil, 204)
	return err
}

//
// GetEvent
//

// Returns an individual event with the given details.
//
// This call is gorc2 compatible other than changing the type from Event2 to
// Event.
func (c *Collection) GetEvent(
	key, typ string, ts time.Time, ordinal int64, value interface{},
) (*Event2, error) {
	event := &Event2{
		Collection: c,
		Key:        key,
		Ordinal:    ordinal,
		Timestamp:  ts,
		Type:       typ,
	}

	// Perform the actual GET
	path := fmt.Sprintf("%s/%s/events/%s/%d/%d", c.Name, key, typ,
		ts.UnixNano()/1000000, ordinal)
	var responseData jsonEvent
	_, err := c.client.jsonReply("GET", path, nil, 200, &responseData)
	if err != nil {
		return nil, err
	}

	// Move the data from the returned values into the Event2 object.
	event.Value = responseData.Value
	event.Ref = responseData.Path.Ref
	secs := responseData.Timestamp / 1000
	nsecs := (responseData.Timestamp % 1000) * 1000000
	event.Timestamp = time.Unix(secs, nsecs)
	event.Ordinal = responseData.Ordinal

	// If the user provided us a place to unmarshal the 'value' field into
	// we do that here.
	if value != nil {
		return event, event.Unmarshal(value)
	}

	// Success
	return event, nil
}

//
// UpdateEvent
//

// Updates an event at the given location. In order for this to work the Event
// must exist prior to this call.
//
// This call is gorc2 compatible other than changing the type from Event2 to
// Event.
func (c *Collection) UpdateEvent(
	key, typ string, ts time.Time, ordinal int64, value interface{},
) (*Event2, error) {
	headers := map[string]string{"Content-Type": "application/json"}
	return c.innerUpdateEvent(key, typ, ts, ordinal, value, headers)
}

// Inner implementation used in both UpdateEvent and Event2.Update.
func (c *Collection) innerUpdateEvent(
	key, typ string, ts time.Time, ordinal int64, value interface{},
	headers map[string]string,
) (*Event2, error) {
	event := &Event2{
		Collection: c,
		Key:        key,
		Ordinal:    ordinal,
		Timestamp:  ts,
		Type:       typ,
	}

	// Encode the JSON message into a raw value that we can return to the
	// client if necessary.
	if rawMsg, err := json.Marshal(value); err != nil {
		return nil, err
	} else {
		event.Value = json.RawMessage(rawMsg)
	}

	// Perform the actual PUT
	path := fmt.Sprintf("%s/%s/events/%s/%d/%d", c.Name, key, typ,
		ts.UnixNano()/1000000, ordinal)
	resp, err := c.client.emptyReply("PUT", path, headers,
		bytes.NewBuffer(event.Value), 204)
	if err != nil {
		return nil, err
	}

	// Get the Location header and parse it. The Header will give us the
	// Ordinal.
	location := resp.Header.Get("Location")
	if location == "" {
		return nil, fmt.Errorf("Missing Location header.")
	} else if parts := strings.Split(location, "/"); len(parts) != 8 {
		return nil, fmt.Errorf("Malformed Location header.")
	} else if ts, err := strconv.ParseInt(parts[6], 10, 64); err != nil {
		return nil, fmt.Errorf("Malformed Timestamp in the Location header.")
	} else if ord, err := strconv.ParseInt(parts[7], 10, 64); err != nil {
		return nil, fmt.Errorf("Malformed Ordinal in the Location header.")
	} else {
		secs := ts / 1000
		nsecs := (ts % 1000) * 1000000
		event.Timestamp = time.Unix(secs, nsecs)
		event.Ordinal = ord
	}

	// Get the Ref via the Etag header.
	if etag := resp.Header.Get("Etag"); etag == "" {
		return nil, fmt.Errorf("Missing ETag header.")
	} else if parts := strings.Split(etag, `"`); len(parts) != 3 {
		return nil, fmt.Errorf("Malformed ETag header.")
	} else {
		event.Ref = parts[1]
	}

	// Success
	return event, nil
}

//
// ListEvents
//

//
// Search
//

// Provides optional searching parameters to a cal to ListEvents()
//
// This type is gorc2 compatible.
type ListEventsQuery struct {
	// The number of results to return per call to Orchestrate. The default
	// if this is not set is to return 10 at a time, the maximum that can be
	// returned is 100.
	PageSize int

	// This is the timestamp and ordinal that should be the oldest item
	// included in the Event listing. Since Events a re listed newest to oldest
	// this will be the last item returned (if it exists). The precision of
	// the time value is miliseconds.
	Start        time.Time
	StartOrdinal int64

	// Events up to this timestamp will be included in the listing. Note that
	// if EndOrdinal is not set then End behaves the same as Before. The time
	// till be truncated to miliseconds.
	End        time.Time
	EndOrdinal int64

	// After the time/ordinal pairing which all events must be newer than in
	// order to be included in the results. Leaving Ordinal at zero has the
	// effect of including all events with the same timestamp (leaving after
	// to work like Start). The time will be truncated to miliseconds for
	// the search.
	After        time.Time
	AfterOrdinal int64

	// Only include listing before this time stamp. Optionally you can include
	// an ordinal as well which will be used if an event exists at the exact
	// same ms as Before. The precision of this time value is in miliseconds.
	Before        time.Time
	BeforeOrdinal int64
}

// Sets up a Events listing. This does not actually perform the query, that is
// done on the first call to Next() in the iterator. If opts is nil then
// default listing parameters are used, which will return all events and
// sets the PageSize to 10 items at a time.
//
// This function is gorc2 compatible.
func (c *Collection) ListEvents(
	key, typ string, opts *ListEventsQuery,
) *Iterator {
	var path string
	// Build a query from the user provided values.
	if opts != nil {
		query := make(url.Values, 10)

		if opts.PageSize != 0 {
			query.Add("limit", strconv.Itoa(opts.PageSize))
		}
		var defaultTime time.Time
		if opts.After != defaultTime {
			if opts.AfterOrdinal != 0 {
				query.Add("afterEvent", fmt.Sprintf("%d/%d",
					opts.After.UnixNano()/1000000, opts.AfterOrdinal))
			} else {
				query.Add("afterEvent",
					strconv.FormatInt(opts.After.UnixNano()/1000000, 10))
			}
		}
		if opts.Before != defaultTime {
			if opts.BeforeOrdinal != 0 {
				query.Add("beforeEvent", fmt.Sprintf("%d/%d",
					opts.Before.UnixNano()/1000000, opts.BeforeOrdinal))
			} else {
				query.Add("beforeEvent",
					strconv.FormatInt(opts.Before.UnixNano()/1000000, 10))
			}
		}
		if opts.End != defaultTime {
			if opts.EndOrdinal != 0 {
				query.Add("endEvent", fmt.Sprintf("%d/%d",
					opts.End.UnixNano()/1000000, opts.EndOrdinal))
			} else {
				query.Add("endEvent",
					strconv.FormatInt(opts.End.UnixNano()/1000000, 10))
			}
		}
		if opts.Start != defaultTime {
			if opts.StartOrdinal != 0 {
				query.Add("startEvent", fmt.Sprintf("%d/%d",
					opts.Start.UnixNano()/1000000, opts.StartOrdinal))
			} else {
				query.Add("startEvent",
					strconv.FormatInt(opts.Start.UnixNano()/1000000, 10))
			}
		}

		// Encode the path
		path = c.Name + "/" + key + "/events/" + typ + "?" + query.Encode()
	} else {
		path = c.Name + "/" + key + "/events/" + typ
	}

	return &Iterator{
		client:          c.client,
		iteratingEvents: true,
		next:            path,
	}
}
