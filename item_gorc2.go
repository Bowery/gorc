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
// Item
//

// Stores information about a single Item from the Key Value part of a
// Collection.
//
// This type is gorc2 compatible.
type Item struct {
	// The Collection that houses this item.
	Collection *Collection

	// Distance is set on queries that include geospacial search. If
	// this field is non zero then Score will be zero.
	// See http://orchestrate.io/blog/2014/10/08/geospatial-search/
	Distance float32

	// The Key used to store this item within its collection.
	Key string

	// The Ref value for this item which uniquely identifies its version.
	Ref string

	// For Search results this will be populated with the score returned from
	// Orchestrate. Higher numbers mean better matches.
	Score float32

	// Set to true if this item represents a "Tombstone", or delete operation.
	// If this is set then other fields, like Value might not be set at all.
	// Only calls to History calls will set this field.
	Tombstone bool

	// The time that this item was created or updated in Orchestrate. This is
	// only populated on History calls at the moment.
	Updated time.Time

	// The raw JSON value returned by Orchestrate. To decode this value into
	// a structure use the Unmarshal() call.
	Value json.RawMessage
}

// Delete the Item from the collection if it represents the most recent
// 'Ref' associated with the key. If the key has been updated at some point
// after this item then this call will fail, reduring a NotMostRecentError
// object.
//
// This function is gorc2 compatible.
func (i *Item) Delete() error {
	headers := map[string]string{"If-Match": `"` + i.Ref + `"`}
	path := i.Collection.Name + "/" + i.Key
	_, err := i.Collection.client.emptyReply("DELETE", path, headers, nil, 204)
	if err != nil {
		if _, ok := err.(PreconditionFailedError); ok {
			err = NotMostRecentError(i.Ref)
		}
	}
	return err
}

// Patches the item via a series of operations. This is like Update() except
// that it only sends small fragments of the object rather than re-uploading
// the whole object. This call will only succeed if the object has not been
// updated since it was read. This will return the new Item with the ref
// updated, however the value will not be set.
//
// This function is gorc2 compatible.
func (i *Item) Patch(ops []UpdateOperation) (*Item, error) {
	headers := map[string]string{"If-Match": `"` + i.Ref + `"`}
	path := i.Collection.Name + "/" + i.Key
	body, err := json.Marshal(ops)
	if err != nil {
		return nil, err
	}
	resp, err := i.Collection.client.emptyReply("PATCH", path, headers,
		bytes.NewBuffer([]byte(body)), 201)
	if err != nil {
		if _, ok := err.(PreconditionFailedError); ok {
			err = NotMostRecentError(i.Ref)
		}
		return nil, err
	}
	item := &Item{
		Collection: i.Collection,
		Key:        i.Key,
	}
	if etag := resp.Header.Get("Etag"); etag == "" {
		return nil, fmt.Errorf("Missing ETag header.")
	} else if parts := strings.Split(etag, `"`); len(parts) != 3 {
		return nil, fmt.Errorf("Malformed ETag header.")
	} else {
		item.Ref = parts[1]
	}
	return item, err
}

// This will take the raw JSON data returned from Orchestrate and Unmarshal it
// into the given object.
//
// This function is gorc2 compatible.
func (i *Item) Unmarshal(value interface{}) error {
	return json.Unmarshal(i.Value, value)
}

// Updates this Item in the key value store if it is the most recent 'Ref'
// associated with the given key. If the given Item's Ref field does not match
// the most recently updated item then this call will return a
// NotMostRecentError type, and no change will be made in the data store.
//
// This function is gorc2 compatible.
func (i *Item) Update(value interface{}) (*Item, error) {
	headers := map[string]string{"If-Match": `"` + i.Ref + `"`}
	item, err := i.Collection.innerPutPost("PUT", i.Key, headers, value)
	if err != nil {
		if _, ok := err.(PreconditionFailedError); ok {
			err = NotMostRecentError(i.Key)
		}
	}
	return item, err
}
