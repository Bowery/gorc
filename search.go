// Copyright 2014, Orchestrate.IO, Inc.

package client

import (
	"encoding/json"
	"net/url"
)

type SearchResults struct {
	Count    uint64         `json:"count"`
	Results  []SearchResult `json:"results"`
	MaxScore float64        `json:"max_score"`
}

type SearchResult struct {
	Path  ResultPath             `json:"path"`
	Score float64                `json:"score"`
	Value map[string]interface{} `json:"value"`
}

type ResultPath struct {
	Collection string `json:"collection"`
	Key        string `json:"key"`
	Ref        string `json:"ref"`
}

func (client *Client) Search(collection, query string) (*SearchResults, error) {
	queryVariables := url.Values{
		"query": []string{query},
	}

	resp, err := client.doRequest("GET", collection+"?"+queryVariables.Encode(), nil)
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
