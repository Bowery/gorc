// Copyright 2014, Orchestrate.IO, Inc.

package client

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
)

type SearchResults struct {
	Count      uint64         `json:"count"`
	TotalCount uint64         `json:"total_count"`
	Results    []SearchResult `json:"results"`
	Next       string         `json:"next,omitempty"`
	Prev       string         `json:"prev,omitempty"`
}

type SearchResult struct {
	Path     Path            `json:"path"`
	Score    float64         `json:"score"`
	RawValue json.RawMessage `json:"value"`
}

func (client *Client) Search(collection string, query string, limit int, offset int) (*SearchResults, error) {
	queryVariables := url.Values{
		"query":  []string{query},
		"limit":  []string{strconv.Itoa(limit)},
		"offset": []string{strconv.Itoa(offset)},
	}

	trailingUri := fmt.Sprintf("%s?%s", collection, queryVariables.Encode())

	return client.doSearch(trailingUri)
}

func (client *Client) SearchGetNext(results *SearchResults) (*SearchResults, error) {
	return client.doSearch(results.Next[4:])
}

func (client *Client) SearchGetPrev(results *SearchResults) (*SearchResults, error) {
	return client.doSearch(results.Prev[4:])
}

func (client *Client) doSearch(trailingUri string) (*SearchResults, error) {
	resp, err := client.doRequest("GET", trailingUri, nil, nil)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, newError(resp)
	}

	decoder := json.NewDecoder(resp.Body)
	result := new(SearchResults)
	if err := decoder.Decode(result); err != nil {
		return result, err
	}

	return result, nil
}

func (results *SearchResults) HasNext() bool {
	return results.Next != ""
}

func (results *SearchResults) HasPrev() bool {
	return results.Prev != ""
}

func (result *SearchResult) Value(value interface{}) error {
	return json.Unmarshal(result.RawValue, value)
}
