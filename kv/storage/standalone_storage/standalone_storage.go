package standalone_storage

import (
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/Connor1996/badger"
	"path"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engine *engine_util.Engines

}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	DBPath := conf.DBPath
	kvPath := path.Join(DBPath, "kv")
	raftPath := path.Join(DBPath, "raft")
	kv := engine_util.CreateDB(kvPath, conf.Raft)
	raft := engine_util.CreateDB(raftPath, conf.Raft)

	return &StandAloneStorage{
		engine: engine_util.NewEngines(kv, raft, kvPath, raftPath),
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	

	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.engine.Close()
}


type StandAloneStorageReader struct{
	kvTxn *badger.Txn
}

func NewStandAloneStorageReader(kvTxn *badger.Txn) *StandAloneStorageReader {
	return &StandAloneStorageReader{
		kvTxn: kvTxn,
	}
}

func (reader *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error)  {
	value, err := engine_util.GetCFFromTxn(reader.kvTxn, cf, key)
	if err != nil{
		return nil, nil
	}
	return value, err
}

func (reader *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator  {
	return engine_util.NewCFIterator(cf, reader.kvTxn)
}

func (reader *StandAloneStorageReader) Close() {
	reader.kvTxn.Discard()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return NewStandAloneStorageReader(s.engine.Kv.NewTransaction(false)), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, modify := range batch{
		//这边interface用法是啥？
		_, isPut := interface{}(modify.Data).(storage.Put)
		_, isDelete := interface{}(modify.Data).(storage.Delete)
		if isPut {
			put := modify.Data.(storage.Put)
			err := engine_util.PutCF(s.engine.Kv, put.Cf, put.Key, put.Value)
			if err != nil {
				return err
			}
		}
		if isDelete {
			delete := modify.Data.(storage.Delete)
			err := engine_util.DeleteCF(s.engine.Kv, delete.Cf, delete.Key)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
