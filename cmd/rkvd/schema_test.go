package main

import (
	"fmt"
	"testing"

	memdb "github.com/hashicorp/go-memdb"
	"github.com/relab/rkv/rkvpb"
)

func TestUint64Index(t *testing.T) {
	db, err := memdb.NewMemDB(schema)

	if err != nil {
		t.Error(err)
	}

	txn := db.Txn(true)
	for i := uint64(0); i < 10; i++ {
		for j := uint64(0); j < 10; j++ {
			err := txn.Insert(RequestStore, &rkvpb.InsertRequest{
				ClientID:  i,
				ClientSeq: j,
			})

			if err != nil {
				t.Error(err)
			}
		}
	}
	txn.Commit()

	txn = db.Txn(false)
	raw, err := txn.First(RequestStore, Identifier, uint64(1), uint64(1))

	if err != nil {
		t.Error(err)
	}

	req, ok := raw.(*rkvpb.InsertRequest)

	if !ok {
		t.Error(fmt.Errorf("%v != *rkvpb.InsertRequest", req))
	}

	raw, err = txn.First(RequestStore, Identifier, uint64(1), uint64(11))

	if err != nil {
		t.Error(err)
	}

	if raw != nil {
		t.Error(fmt.Errorf("Expected %v to be nil", raw))
	}

	defer txn.Abort()
}

type Request struct {
	ClientID  uint64
	ClientSeq uint64
}

func BenchmarkUint64Index(b *testing.B) {
	db, err := memdb.NewMemDB(schema)

	if err != nil {
		b.Error(err)
	}

	b.ResetTimer()
	txn := db.Txn(true)
	for i := 0; i < b.N; i++ {
		err := txn.Insert(RequestStore, &Request{
			ClientID:  uint64(i),
			ClientSeq: uint64(i),
		})

		if err != nil {
			b.Error(err)
		}
	}
	txn.Commit()
}

func BenchmarkStringIndex(b *testing.B) {
	db, err := memdb.NewMemDB(schema)

	if err != nil {
		b.Error(err)
	}

	b.ResetTimer()
	txn := db.Txn(true)
	for i := 0; i < b.N; i++ {
		ii := fmt.Sprintf("%d", i)
		err := txn.Insert(KeyValueStore, &KeyValue{
			Key:   ii,
			Value: ii,
		})

		if err != nil {
			b.Error(err)
		}
	}
	txn.Commit()
}
