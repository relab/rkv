package main

import (
	"fmt"
	"reflect"
	"strconv"

	memdb "github.com/hashicorp/go-memdb"
)

// Uint64Index is used to extract a field from an object using reflection and
// builds an index on that field.
type Uint64Index struct {
	Field string
}

// FromObject implements the memdb.SingleIndexer interface.
func (s *Uint64Index) FromObject(obj interface{}) (bool, []byte, error) {
	v := reflect.ValueOf(obj)
	v = reflect.Indirect(v) // Dereference the pointer if any
	fv := v.FieldByName(s.Field)
	if !fv.IsValid() {
		return false, nil,
			fmt.Errorf("field '%s' for %#v is invalid", s.Field, obj)
	}
	val := strconv.FormatUint(fv.Uint(), 10)
	val += "\x00"
	return true, []byte(val), nil
}

// FromArgs implements the memdb.Indexer interface.
func (s *Uint64Index) FromArgs(args ...interface{}) ([]byte, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("must provide only a single argument")
	}
	i, ok := args[0].(uint64)
	if !ok {
		return nil, fmt.Errorf("argument must be a uint64: %#v", args[0])
	}
	val := strconv.FormatUint(i, 10)
	val += "\x00"
	return []byte(val), nil
}

// Schema constants.
const (
	KeyValueStore = "kvs"
	SessionStore  = "ss"
	RequestStore  = "rs"

	Identifier = "id"
)

var schema = &memdb.DBSchema{
	Tables: map[string]*memdb.TableSchema{
		KeyValueStore: &memdb.TableSchema{
			Name: KeyValueStore,
			Indexes: map[string]*memdb.IndexSchema{
				Identifier: &memdb.IndexSchema{
					Name:   Identifier,
					Unique: true,
					Indexer: &memdb.StringFieldIndex{
						Field: "Key",
					},
				},
			},
		},
		SessionStore: &memdb.TableSchema{
			Name: SessionStore,
			Indexes: map[string]*memdb.IndexSchema{
				Identifier: &memdb.IndexSchema{
					Name:   Identifier,
					Unique: true,
					Indexer: &Uint64Index{
						Field: "ClientID",
					},
				},
			},
		},
		RequestStore: &memdb.TableSchema{
			Name: RequestStore,
			Indexes: map[string]*memdb.IndexSchema{
				Identifier: &memdb.IndexSchema{
					Name:   Identifier,
					Unique: true,
					Indexer: &memdb.CompoundIndex{
						Indexes: []memdb.Indexer{
							&Uint64Index{
								Field: "ClientID",
							},
							&Uint64Index{
								Field: "ClientSeq",
							},
						},
					},
				},
			},
		},
	},
}
