package main

import memdb "github.com/hashicorp/go-memdb"

var schema = &memdb.DBSchema{
	Tables: map[string]*memdb.TableSchema{
		"kvs": &memdb.TableSchema{
			Name: "kvs",
			Indexes: map[string]*memdb.IndexSchema{
				"id": &memdb.IndexSchema{
					Name:         "id",
					AllowMissing: false,
					Unique:       true,
					Indexer: &memdb.StringFieldIndex{
						Field:     "Key",
						Lowercase: false,
					},
				},
			},
		},
		"sessions": &memdb.TableSchema{
			Name: "sessions",
			Indexes: map[string]*memdb.IndexSchema{
				"id": &memdb.IndexSchema{
					Name:         "id",
					AllowMissing: false,
					Unique:       true,
					Indexer: &memdb.StringFieldIndex{
						Field:     "ClientID",
						Lowercase: false,
					},
				},
			},
		},
	},
}
