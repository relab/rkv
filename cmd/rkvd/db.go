package main

import (
	"fmt"

	memdb "github.com/hashicorp/go-memdb"
)

func InsertKeyValue(txn *memdb.Txn, kv *KeyValue) {
	err := txn.Insert(KeyValueStore, kv)
	if err != nil {
		panic(fmt.Sprintf("tried to insert key-value %+v: %v", kv, err))
	}
}

func InsertSession(txn *memdb.Txn, session *Client) {
	err := txn.Insert(SessionStore, session)
	if err != nil {
		panic(fmt.Sprintf("tried to insert session %+v: %v", session, err))
	}
}

func LookupKeyValue(txn *memdb.Txn, key string) *KeyValue {
	raw, err := txn.First(KeyValueStore, Identifier, key)
	if err != nil {
		panic(fmt.Sprintf("tried to lookup key-value '%v': %v", key, err))
	}
	return raw.(*KeyValue)
}
