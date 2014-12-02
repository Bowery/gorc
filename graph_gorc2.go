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
	"fmt"
	"strconv"
	"strings"
)

// ----------------------------------------------------------------------------
// ---------------------------------- gorc2 -----------------------------------
// ----------------------------------------------------------------------------

// All changes past this point were added in order to make conversion from
// gorc to gorc2 easier. Most of these calls backports gorc2 functionality and
// API into the gorc library.

//
// GetLinks
//

// Wraps the options from GetLinks into a structure so more fields can be
// added later if necessary.
//
// This type is gorc2 compatible.
type GetLinksQuery struct {
	// The number of items that should be returned per call to Orchestrate.
	// If unset this will be 10, and the maximum is 100.
	PageSize int
}

// Sets up an Iterator that will walk all of relations to the given key.
// The first kind is required, however optional other kinds may be added
// to traverse the graph further. If opts is null then default values will
// be used. For more information on how graphs work see this page:
//http://orchestrate.io/docs/graph
//
// This function is gorc2 compatible.
func (c *Collection) GetLinks(
	key string, opts *GetLinksQuery, kind string, kinds ...string,
) *Iterator {
	path := c.Name + "/" + key + "/relations/" + kind
	if len(kinds) > 0 {
		path = path + "/" + strings.Join(kinds, "/")
	}
	if opts != nil && opts.PageSize != 0 {
		path = path + "?limit=" + strconv.Itoa(opts.PageSize)
	}
	return &Iterator{
		client:         c.client,
		iteratingItems: true,
		next:           path,
	}
}

//
// Link
//

// Creates a one way graph link between two items. Graphs are used to build
// associations between two items in the data store. For examples of how
// graphs can be used see the graph documentation:
// http://orchestrate.io/docs/graph
//
// This function is gorc2 compatible.
func (c *Collection) Link(key, kind, toCollection, toKey string) error {
	path := fmt.Sprintf("%s/%s/relation/%s/%s/%s", c.Name, key, kind,
		toCollection, toKey)
	_, err := c.client.emptyReply("PUT", path, nil, nil, 204)
	return err
}

//
// Unlink
//

// Deletes a graph link between two items. If the link did not exist prior to
// this call then this still returns success. See the graph documentation
// (http://orchestrate.io/docs/graph) for more information.
//
// This function is gorc2 compatible.
func (c *Collection) Unlink(key, kind, toCollection, toKey string) error {
	path := fmt.Sprintf("%s/%s/relation/%s/%s/%s?purge=true", c.Name, key,
		kind, toCollection, toKey)
	_, err := c.client.emptyReply("DELETE", path, nil, nil, 204)
	return err
}
