package standalone_storage

import (
	"fmt"
	"path"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engine *engine_util.Engines
	config *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	dbPath := conf.DBPath

	kvPath := path.Join(dbPath, "kv")
	kvEngine := engine_util.CreateDB(kvPath, false)

	raftPath := path.Join(dbPath, "raft")
	raftEngine := engine_util.CreateDB(raftPath, true)

	return &StandAloneStorage{
		engine: engine_util.NewEngines(kvEngine, raftEngine, kvPath, raftPath),
		config: conf,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	// Doesn't need to do something temporarily
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	err := s.engine.Close()
	return err
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.engine.Kv.NewTransaction(false)
	return NewStandAloneStorageReader(txn), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	wb := engine_util.WriteBatch{}
	for _, mod := range batch {
		switch mod.Data.(type) {
		case storage.Put:
			{
				wb.SetCF(mod.Data.(storage.Put).Cf, mod.Data.(storage.Put).Key, mod.Data.(storage.Put).Value)
			}
		case storage.Delete:
			{
				wb.DeleteCF(mod.Data.(storage.Delete).Cf, mod.Data.(storage.Delete).Key)
			}
		default:
			{
				fmt.Printf("The type isn't supported!\n")
			}
		}
	}
	return wb.WriteToDB(s.engine.Kv)
}

type StandAloneStorageReader struct {
	txn *badger.Txn
}

func NewStandAloneStorageReader(txn *badger.Txn) *StandAloneStorageReader {
	return &StandAloneStorageReader{
		txn: txn,
	}
}

func (r *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	value, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return value, err
}

func (r *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *StandAloneStorageReader) Close() {
	r.txn.Discard()
}
