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

//
// gorc2
//

// All changes past this point were added in order to make conversion from
// gorc to gorc2 easier. Most of these calls backports gorc2 functionality and
// API into the gorc library.

//
// UpdateOperationType
//

// Document update types that are supported by the Orchestrate API.
//
// This type is gorc2 compatible.
type UpdateOperationType string

const (
	// Adds the value of 'Value' to the 'Path' field.
	AddOp UpdateOperationType = "add"

	// Copies the value of the 'From' field to 'Path'.
	CopyOp UpdateOperationType = "copy"

	// Increments a integer or floating point value at 'Path' by the amount
	// defined in 'Value'.
	IncOp UpdateOperationType = "inc"

	// Moves the value of the 'From' field to 'Path'.
	MoveOp UpdateOperationType = "move"

	// Removes the value of the 'Path' field.
	RemoveOp UpdateOperationType = "remove"

	// Replaces the value of 'Path' with 'Value'.
	ReplaceOp UpdateOperationType = "replace"

	// Tests the value of 'Path' against 'Value' to ensure that they are
	// the same, if not then no update will be performed.
	TestOp UpdateOperationType = "test"
)

//
// UpdateSet
//

// Represents a set UpdateOperations that should be applied to a specific
// item. These will all be performed together, and if any test case
// fails then no mutations will happen at all.
//
// This type is gorc2 compatible.
type UpdateSet []UpdateOperation

// Appends an "Add" Operation to the UpdateSet. Add operations will add a new
// field to a dictionary, or append a new item to a list.
//
// See the Examples for the Update() call for information on how to use the
// 'path' value.
//
// This function is gorc2 compatible.
func (u *UpdateSet) Add(path string, value interface{}) {
	*u = append(*u, UpdateOperation{Op: AddOp, Path: path, Value: value})
}

// Appends a "Copy" Operation to the UdateSet. Copy operations will copy the
// value in one field to another.
//
// See the Examples for the Update() call for information on how to use the
// 'source'  and 'dest' values.
//
// This function is gorc2 compatible.
func (u *UpdateSet) Copy(source, dest string) {
	*u = append(*u, UpdateOperation{Op: CopyOp, From: source, Path: dest})
}

// Appends an "Inc" Operation to the UdateSet. Inc operations will increment a
// counter vie the given value. To decrement the value provide a negative
// value.
//
// See the Examples for the Update() call for information on how to use the
// 'path' value.
//
// This function is gorc2 compatible.
func (u *UpdateSet) Inc(path string, value float64) {
	*u = append(*u, UpdateOperation{Op: IncOp, Path: path, Value: value})
}

// Appends a "Move" Operation to the UdateSet. Move operations work like
// Copy except that the source is removed in the process.
//
// Move can move an value within a list, however the "dest" will be processed
// after the value has been removed, so the index needs to be calculated
// accordingly. If the value is:
//  { "field": [0, 1, 2, 3, 4] }
//
// And the call is:
//  Move("field[0]", "field[3]")
//
// Then the resulting object will be:
//  { "field": [1, 2, 3, 0, 4] }
//
// See the Examples for the Update() call for information on how to use the
// 'source'  and 'dest' values.
//
// This function is gorc2 compatible.
func (u *UpdateSet) Move(source, dest string) {
	*u = append(*u, UpdateOperation{Op: MoveOp, From: source, Path: dest})
}

// Appends a "Remove" Operation to the UdateSet. Remove operations will remove
// the entire contents of a given path. The path can be either a field in
// a dictionary, or a item in a list.
//
// See the Examples for the Update() call for information on how to use the
// 'path' value.
//
// This function is gorc2 compatible.
func (u *UpdateSet) Remove(path string) {
	*u = append(*u, UpdateOperation{Op: RemoveOp, Path: path})
}

// Appends a "Replace" Operation to the UdateSet. Replace operations work like
// add except that the value must exist prior to the call for the operation
// to succeed.
//
// See the Examples for the Update() call for information on how to use the
// 'path' value.
//
// This function is gorc2 compatible.
func (u *UpdateSet) Replace(path string, value interface{}) {
	*u = append(*u, UpdateOperation{Op: ReplaceOp, Path: path, Value: value})
}

// Appends a "Test" Operation to the UdateSet. Test operations provide an
// equality check to ensure that the data matches expected values. If any
// test operation fails at any point during the Update call then no mutations
// will be made, including those that completed prior to the TestOp.
//
// See the Examples for the Update() call for information on how to use the
// 'path' value.
//
// This function is gorc2 compatible.
func (u *UpdateSet) Test(path string, value interface{}) {
	*u = append(*u, UpdateOperation{Op: TestOp, Path: path, Value: value})
}

//
// UpdateOperation
//

// Represents a single operation to be performed when patching and existing
// object. Each operation can mutate the data in some way, or test that the
// data is in a specific state.
//
// This type is gorc2 compatible.
type UpdateOperation struct {
	// The operation to perform. This is required and must be a known value
	// from the list of Ops defined in this package.
	Op UpdateOperationType `json:"op"`

	// Used with MoveOp and CopyOp to specify the source of the data.
	From string `json:"from,omitempty"`

	// Used with all operations to specify the target of the test or
	// update.
	Path string `json:"path"`

	// The value to use when performing the operation. Each operation
	// will use the value differently so see the documentation on the
	// operations.
	Value interface{} `json:"value,omitempty"`
}
