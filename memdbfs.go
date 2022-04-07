package memdbfs

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/hashicorp/go-memdb"
)

func Stash(o io.Writer, db *memdb.MemDB, schema *memdb.DBSchema) error {
	txn := db.Snapshot().Txn(false)
	fmt.Fprintf(o, "{")
	dotableonce := true
	for tablename, _ := range schema.Tables {
		if dotableonce {
			dotableonce = false
		} else {
			fmt.Fprintf(o, ",")
		}
		fmt.Fprintf(o, "\n")
		jsonname, err := json.Marshal(tablename)
		if err != nil {
			return err
		}
		fmt.Fprintf(o, "\t%s: [", jsonname)
		it, err := txn.Get(tablename, "id")
		if err != nil {
			return err
		}
		doonce := true
		for obj := it.Next(); obj != nil; obj = it.Next() {
			jsonobj, err := json.Marshal(obj)
			if err != nil {
				return err
			}
			if doonce {
				doonce = false
			} else {
				fmt.Fprintf(o, ",")
			}
			fmt.Fprintf(o, "\n\t\t%s", jsonobj)
		}
		fmt.Fprintf(o, "\n\t]")
	}
	fmt.Fprintf(o, "\n}")
	return nil
}

type TypeFactory map[string]func(json.RawMessage) (interface{}, error)

func Unstash(o io.Reader, db *memdb.MemDB, types TypeFactory) error {
	if o == nil {
		// shortcircuit if we don't have a reader
		return nil
	}
	txn := db.Txn(true)
	dec := json.NewDecoder(o)
	// read open bracket
	_, err := dec.Token()
	if err != nil {
		txn.Abort()
		return err
	}
	for {
		t, err := dec.Token()
		if t == json.Delim('}') {
			break
		}
		if err != nil {
			txn.Abort()
			return err
		}
		tablename, ok := t.(string)
		if !ok {
			txn.Abort()
			return errors.New("Expected a string delimiter")
		}
		t, err = dec.Token()
		if err != nil {
			txn.Abort()
			return err
		}

		factory, ok := types[tablename]
		if !ok {
			txn.Abort()
			return fmt.Errorf("Missing type factory for table %s", tablename)
		}

		// while the array contains values
		for dec.More() {
			var j json.RawMessage
			// decode an array value (Message)
			err := dec.Decode(&j)
			if err != nil {
				txn.Abort()
				return err
			}
			v, err := factory(j)
			if err != nil {
				txn.Abort()
				return err
			}
			// write
			err = txn.Insert(tablename, v)
			if err != nil {
				txn.Abort()
				return err
			}
		}
		t, err = dec.Token()
		if err != nil {
			txn.Abort()
			return err
		}
	}
	txn.Commit()
	return nil
}
