package fsm

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/hashicorp/raft"
)

type Fsm struct {
	DataBase Database
}

func NewFsm() *Fsm {
	fsm := &Fsm{
		DataBase: NewDatabase(),
	}
	return fsm
}

func (f *Fsm) Apply(l *raft.Log) interface{} {
	fmt.Println("apply data:", string(l.Data))
	data := strings.Split(string(l.Data), ",")
	op := data[0]
	if op == "set" {
		key := data[1]
		value := data[2]
		f.DataBase.Set(key, value)
	}

	return nil
}

func (f *Fsm) Snapshot() (raft.FSMSnapshot, error) {
	return &f.DataBase, nil
}

func (f *Fsm) Restore(io.ReadCloser) error {
	return nil
}

type Database struct {
	Data map[string]string
	mu   sync.Mutex
}

func NewDatabase() Database {
	return Database{
		Data: make(map[string]string),
	}
}

func (d *Database) Get(key string) string {
	d.mu.Lock()
	value := d.Data[key]
	d.mu.Unlock()
	return value
}

func (d *Database) Set(key, value string) {
	d.mu.Lock()
	d.Data[key] = value
	d.mu.Unlock()
}

func (d *Database) Persist(sink raft.SnapshotSink) error {
	d.mu.Lock()
	data, err := json.Marshal(d.Data)
	d.mu.Unlock()
	if err != nil {
		return err
	}
	if _, err = sink.Write(data); err != nil {
		return err
	}
	if err = sink.Close(); err != nil {
		return err
	}
	return nil
}

func (d *Database) Release() {}
