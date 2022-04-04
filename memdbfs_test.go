package memdbfs_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/dougrich/go-memdbfs"
	"github.com/hashicorp/go-memdb"
)

type testMessage struct {
	Name string `json:"name"`
	Text string `json:"text"`
}
type testCount struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

var (
	schema = &memdb.DBSchema{
		Tables: map[string]*memdb.TableSchema{
			"message": &memdb.TableSchema{
				Name: "message",
				Indexes: map[string]*memdb.IndexSchema{
					"id": &memdb.IndexSchema{
						Name:    "id",
						Unique:  true,
						Indexer: &memdb.StringFieldIndex{Field: "Name"},
					},
				},
			},
			"counter": &memdb.TableSchema{
				Name: "counter",
				Indexes: map[string]*memdb.IndexSchema{
					"id": &memdb.IndexSchema{
						Name:    "id",
						Unique:  true,
						Indexer: &memdb.StringFieldIndex{Field: "Name"},
					},
				},
			},
		},
	}
	stash = `{
	"message": [
		{"name":"Dorothy","text":"Fancy"},
		{"name":"Joe","text":"Hi"},
		{"name":"Lucy","text":"Whatup"},
		{"name":"Tariq","text":"I'm cool"}
	],
	"counter": [
		{"name":"Dorothy","count":12},
		{"name":"Joe","count":15},
		{"name":"Lucy","count":25},
		{"name":"Tariq","count":22}
	]
}`
	messages = []*testMessage{
		&testMessage{"Dorothy", "Fancy"},
		&testMessage{"Joe", "Hi"},
		&testMessage{"Lucy", "Whatup"},
		&testMessage{"Tariq", "I'm cool"},
	}
	counts = []*testCount{
		&testCount{"Dorothy", 12},
		&testCount{"Joe", 15},
		&testCount{"Lucy", 25},
		&testCount{"Tariq", 22},
	}
)

func TestStash(t *testing.T) {
	db, err := memdb.NewMemDB(schema)
	if err != nil {
		panic(err)
	}
	txn := db.Txn(true)
	// Insert some messages
	for _, p := range messages {
		if err := txn.Insert("message", p); err != nil {
			t.Fatalf("Unexpected error inserting message: %v", err)
		}
	}
	// Insert some counts
	for _, p := range counts {
		if err := txn.Insert("counter", p); err != nil {
			t.Fatalf("Unexpected error inserting counter: %v", err)
		}
	}

	txn.Commit()
	// serialize it to a strings
	var sb strings.Builder
	err = memdbfs.Stash(&sb, db, schema)
	if err != nil {
		t.Fatalf("Unexpected error stashing: %v", err)
	}
	actual := sb.String()
	expected := stash
	if actual != expected {
		t.Fatalf("Serialized mismatch:\nExpected:\n%s\nActual:\n%s", expected, actual)
	}
}

func TestUnstash(t *testing.T) {
	db, err := memdb.NewMemDB(schema)
	if err != nil {
		panic(err)
	}
	err = memdbfs.Unstash(strings.NewReader(stash), db, map[string]func(json.RawMessage) (interface{}, error){
		"message": func(r json.RawMessage) (interface{}, error) {
			p := testMessage{}
			err := json.Unmarshal(r, &p)
			return &p, err
		},
		"counter": func(r json.RawMessage) (interface{}, error) {
			p := testCount{}
			err := json.Unmarshal(r, &p)
			return &p, err
		},
	})
	if err != nil {
		t.Fatalf("Unstash failed: %v", err)
	}
	txn := db.Txn(false)

	it, err := txn.Get("message", "id")
	if err != nil {
		t.Fatal("Unable to get the message iterator")
	}
	i := 0
	for obj := it.Next(); obj != nil; obj = it.Next() {
		actual := obj.(*testMessage)
		expected := messages[i]
		if actual.Name != expected.Name {
			t.Errorf("messages[%d].Name - expected %s, got %s", i, expected.Name, actual.Name)
		}
		if actual.Text != expected.Text {
			t.Errorf("messages[%d].Text - expected %s, got %s", i, expected.Text, actual.Text)
		}
		i = i + 1
		if i > len(messages) {
			t.Fatalf("Too many results returned for messages: %d", i)
		}
	}
	if i != len(messages) {
		t.Errorf("Too few results returned for messages, expected %d, got %d", len(messages), i)
	}

	it, err = txn.Get("counter", "id")
	if err != nil {
		t.Fatal("Unable to get the counter iterator")
	}
	i = 0
	for obj := it.Next(); obj != nil; obj = it.Next() {
		actual := obj.(*testCount)
		expected := counts[i]
		if actual.Name != expected.Name {
			t.Errorf("counter[%d].Name - expected %s, got %s", i, expected.Name, actual.Name)
		}
		if actual.Count != expected.Count {
			t.Errorf("counter[%d].Count - expected %d, got %d", i, expected.Count, actual.Count)
		}
		i = i + 1
		if i > len(counts) {
			t.Fatalf("Too many results returned for counts")
		}
	}
	if i != len(counts) {
		t.Errorf("Too few results returned for counts, expected %d, got %d", len(counts), i)
	}
}
