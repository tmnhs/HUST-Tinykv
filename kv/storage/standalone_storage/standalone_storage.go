package standalone_storage

import (
	"errors"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

var ErrUnknownStorageType = errors.New("standalone_storage: unknown write storage type")

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	kvDB *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{
		kvDB: engine_util.CreateDB(conf.DBPath, conf.Raft),
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	//log.Info("StandAlone Storage Starting...")
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.kvDB.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return &StandAloneStorageReader{
		//创建一个只读事务，false
		s.kvDB.NewTransaction(false),
	}, nil
}

type StandAloneStorageReader struct {
	btxn *badger.Txn
}

func (reader *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	//log.Info("StandAlone Storage Read Get")
	val, err := engine_util.GetCFFromTxn(reader.btxn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (reader *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, reader.btxn)
}

func (reader *StandAloneStorageReader) Close() {
	//取消事务
	reader.btxn.Discard()
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	//创建一个写事务,true
	txn := s.kvDB.NewTransaction(true)

	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put://修改
			//log.Info("StandAlone Storage Write Put")
			put := m.Data.(storage.Put)
			if err := txn.Set(engine_util.KeyWithCF(put.Cf, put.Key), put.Value); err != nil {
				return err
			}
		case storage.Delete://删除
			//log.Info("StandAlone Storage Write Delete")
			del := m.Data.(storage.Delete)
			if err := txn.Delete(engine_util.KeyWithCF(del.Cf, del.Key)); err != nil {
				return err
			}
		default://未知类型，报错
			return ErrUnknownStorageType
		}
	}
	//提交事务
	if err := txn.Commit(); err != nil {
		return err
	}
	return nil
}


