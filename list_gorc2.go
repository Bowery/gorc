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
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// ----------------------------------------------------------------------------
// ---------------------------------- gorc2 -----------------------------------
// ----------------------------------------------------------------------------

// All changes past this point were added in order to make conversion from
// gorc to gorc2 easier. Most of these calls backports gorc2 functionality and
// API into the gorc library.

//
// List Iterator
//

// An internal only representation of the Path structure in queries.
type jsonPath struct {
	// The collection name that the item is from.
	Collection string `json:"collection"`

	// The Key of the item in the collection.
	Key string `json:"key"`

	// Used with Events
	Ordinal int64 `json:"ordinal"`

	// The Ref of this specific item.
	Ref string `json:"ref"`

	// Returned with Event listings.
	Timestamp int64 `json:"timestamp"`

	// For Ref history listing this tells us if this item is a delete marker.
	Tombstone bool `json:"tombstone"`

	// Used with Events
	Type string `json:"type"`
}

// JSON encoding type used with listing.
type jsonList struct {
	// Returned with List and Search operations.
	Count int `json:"count"`

	// Returned on Search operations.
	TotalCount int64 `json:"total_count"`

	// Returned with List and Search operations.
	Next string `json:"next"`
	Prev string `json:"prev"`

	// Returned with all operations.
	Results []*jsonListItem `json:"results"`
}

// JSON encoding type used with listing.
type jsonListItem struct {
	// Distance is used when searching.
	Distance float32

	// Used with Events
	Ordinal int64 `json:"ordinal"`

	// The raw path to the item including its ref identifier.
	Path jsonPath `json:"path"`

	// The time that an item was added to the system (Used in History calls).
	// This is in miliseconds since epoch.
	RefTime int64 `json:"reftime"`

	// The Score (Search) of the item.
	Score float32 `json:"score"`

	// Returned with Event listings.
	Timestamp int64 `json:"timestamp"`

	// The raw value in the item.
	Value json.RawMessage `json:"value"`
}

// Iterates through results from calls to List(), ListEvents(), Search() and
// History().
//
// This type is gorc2 compatible.
type Iterator struct {
	// Stores any error encountered during a call to Next(). This is split
	// out so that a for loop can easily iterate without having to have
	// complex semantics. See the Iteration Example for more details.
	Error error

	// Returns the total number of items that match a given search query.
	// This will only be populated after the first call to Next() and only
	// for Iterators returned from a call to Search().
	TotalCount int64

	// The client that this listing was run against.
	client *Client

	// Set to true when the last item has been returned.
	done bool

	// Keeps track of the next item that needs to be returned via a call to
	// Get().
	index int

	// These fields let us know what type of iterator we are.
	iteratingEvents bool
	iteratingItems  bool

	// The path to the "next" group of results for pagination.
	next string

	// The results returned from the raw JSON unmarshaling.
	results []*jsonListItem
}

// Inner functionality for fetching the next set of results.
func (i *Iterator) fetchResults() bool {
	// See if there is a next link, otherwise we are done.
	if i.next == "" {
		i.done = true
		return false
	}

	// We need to perform a list query. We do this by fetching the path given
	// to us in the 'next' field. After fetching we should get the replacement
	// URL from the server.
	var results jsonList
	_, err := i.client.jsonReply("GET", i.next, nil, 200, &results)
	if err != nil {
		i.Error = err
		return false
	}

	// Capture the Link header into the next field.
	i.next = strings.TrimPrefix(results.Next, "/v0/")
	i.results = results.Results

	// Make sure we set done if nothing was returned, otherwise reset our
	// index back to the start.
	if len(results.Results) == 0 {
		i.done = true
	} else {
		i.index = 0
	}

	// For search queries we copy the TotalCount field if it is non zero.
	if results.TotalCount != 0 {
		i.TotalCount = results.TotalCount
	}

	return !i.done
}

// Returns the Item for the current iteration index. This should be used if
// the Iterator was created via a call to List(), Search() or History(). If
// value is nil then no decoding will be done, but the Item will still be
// returned.
//
// This function is gorc2 compatible.
func (i *Iterator) Get(value interface{}) (*Item, error) {
	if i.iteratingItems != true {
		return nil, fmt.Errorf("Not an Item Iterator.")
	}
	r := i.results[i.index]
	secs := int64(r.RefTime / 1000)
	nsecs := int64((r.RefTime % 1000) * 1000000)
	item := &Item{
		Collection: i.client.Collection(r.Path.Collection),
		Distance:   r.Distance,
		Key:        r.Path.Key,
		Ref:        r.Path.Ref,
		Score:      r.Score,
		Tombstone:  r.Path.Tombstone,
		Updated:    time.Unix(secs, nsecs),
		Value:      r.Value,
	}

	// Decode value if necessary.
	if value != nil && len(item.Value) > 0 {
		return item, json.Unmarshal(r.Value, value)
	}

	// Success
	return item, nil
}

// Returns all of the unreturned items in the last fetch paged results
// returned from Orchestrate. This typically works best with calls to
// NextPage. See the Iterator examples for more information.
//
// This function is gorc2 compatible.
func (i *Iterator) GetPage() ([]*Item, error) {
	if i.iteratingItems != true {
		return nil, fmt.Errorf("Not an Item Iterator.")
	}
	items := make([]*Item, len(i.results)-i.index)
	for offset := range items {
		r := i.results[i.index]
		i.index++
		secs := int64(r.RefTime / 1000)
		nsecs := int64((r.RefTime % 1000) * 1000000)
		items[offset] = &Item{
			Collection: i.client.Collection(r.Path.Collection),
			Distance:   r.Distance,
			Key:        r.Path.Key,
			Ref:        r.Path.Ref,
			Score:      r.Score,
			Tombstone:  r.Path.Tombstone,
			Updated:    time.Unix(secs, nsecs),
			Value:      r.Value,
		}
	}

	// Success
	return items, nil
}

// Returns the Event for the current iteration. This should only be used if the
// call was made to ListEvents() otherwise this will return an error.
//
// In gorc2 the type returned by this function is Event rather than Event2.
func (i *Iterator) GetEvent(value interface{}) (event *Event2, err error) {
	if i.iteratingEvents != true {
		return nil, fmt.Errorf("Not an Event Iterator.")
	}
	r := i.results[i.index]
	secs := int64(r.Timestamp / 1000)
	nsecs := int64((r.Timestamp % 1000) * 1000000)
	event = &Event2{
		Collection: i.client.Collection(r.Path.Collection),
		Key:        r.Path.Key,
		Ordinal:    r.Path.Ordinal,
		Ref:        r.Path.Ref,
		Type:       r.Path.Type,
		Timestamp:  time.Unix(secs, nsecs),
		Value:      r.Value,
	}

	// Decode value if necessary.
	if value != nil {
		return event, json.Unmarshal(r.Value, value)
	}

	// Success
	return event, nil
}

// Like GetPage() except for Event iterators. This call can only be used with
// Iterators returned from ListEvents() otherwise it will return an error.
//
// In gorc2 the type returned by this function is Event rather than Event2.
func (i *Iterator) GetEventPage() ([]*Event2, error) {
	if i.iteratingEvents != true {
		return nil, fmt.Errorf("Not an Event Iterator.")
	}
	events := make([]*Event2, len(i.results)-i.index)
	for offset := range events {
		r := i.results[i.index]
		i.index++
		secs := int64(r.Timestamp / 1000)
		nsecs := int64((r.Timestamp % 1000) * 1000000)
		events[offset] = &Event2{
			Collection: i.client.Collection(r.Path.Collection),
			Key:        r.Path.Key,
			Ordinal:    r.Path.Ordinal,
			Ref:        r.Path.Ref,
			Type:       r.Path.Type,
			Timestamp:  time.Unix(secs, nsecs),
			Value:      r.Value,
		}
	}

	// Success
	return events, nil
}

// Moves the iterator component of the results to the next item. This will
// NOT return an error, rather it will store the error in Error. A return
// of true means that an item has been loaded and can be retrieved via a call
// to Get(), while a return of false means that iteration has finished.
//
// This function is gorc2 compatible.
func (i *Iterator) Next() bool {
	if i.done || i.Error != nil {
		return false
	}

	// See if we can just quickly iterate to the next item without performing
	// any remote calls at all.
	if i.index < len(i.results)-1 {
		i.index += 1
		return true
	}

	// We need to fetch the next page of results in order to be able to return
	// any items.
	return i.fetchResults()
}

// Moves the iterator to the next Page of results. If there are unread items
// or events in the current buffer then this will skip past them. See the
// Iterator examples for more information on how this call works.
//
// This function is gorc2 compatible.
func (i *Iterator) NextPage() bool {
	// Check to see if iteration has completed already.
	if i.done || i.Error != nil {
		return false
	}

	// Return the next results.
	return i.fetchResults()
}

// Like NextPage() except this returns the error as well.
//
// This function is gorc2 compatible.
func (i *Iterator) NextPageWithError() (bool, error) {
	return i.NextPage(), i.Error
}

// Like Next() except this returns the error as well.
//
// This function is gorc2 compatible.
func (i *Iterator) NextWithError() (bool, error) {
	return i.Next(), i.Error
}
