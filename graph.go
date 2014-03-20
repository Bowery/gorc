// Copyright 2014, Orchestrate.IO, Inc.

package client

import (
	"encoding/json"
	"net/url"
)

type GraphResults struct {
	Count   uint64        `json:"count"`
	Results []GraphResult `json:"results"`
}

type GraphResult struct {
	Collection string                 `json:"collection"`
	Key        string                 `json:"key"`
	Ref        string                 `json:"ref"`
	Value      map[string]interface{} `json:"value"`
}

func (client *Client) GetRelations(collection, key string, hops []string) (*GraphResults, error) {
	queryVariables := url.Values{
		"hop": hops,
	}

	resp, err := client.doRequest("GET", collection+"/"+key+"/relations?"+queryVariables.Encode(), nil)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, newError(resp)
	}

	decoder := json.NewDecoder(resp.Body)
	result := new(GraphResults)
	if err := decoder.Decode(result); err != nil {
		return nil, err
	}

	return result, nil
}

func (client *Client) PutRelation(sourceCollection, sourceKey, kind, sinkCollection, sinkKey string) error {
	resp, err := client.doRequest("PUT", sourceCollection+"/"+sourceKey+"/relations/"+kind+"/"+sinkCollection+"/"+sinkKey, nil)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 204 {
		return newError(resp)
	}
	return nil
}
