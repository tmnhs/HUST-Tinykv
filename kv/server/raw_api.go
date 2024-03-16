package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/kv/storage"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	resp := &kvrpcpb.RawGetResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	//defer reader.Close()

	val, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	if val == nil {
		resp.NotFound = true
	}
	resp.Value = val
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (resp *kvrpcpb.RawPutResponse, err error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	resp = &kvrpcpb.RawPutResponse{}
	batch := []storage.Modify{
		{
			Data: storage.Put{
				Cf:    req.Cf,
				Key:   req.Key,
				Value: req.Value,
			},
		}}
	err = server.storage.Write(req.Context, batch)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	return
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (resp *kvrpcpb.RawDeleteResponse, err error) {
	// Your Code Here (1).
	resp = &kvrpcpb.RawDeleteResponse{}
	batch := []storage.Modify{
		{
			Data: storage.Delete{
				Cf:  req.Cf,
				Key: req.Key,
			},
		}}
	err = server.storage.Write(req.Context, batch)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	return
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	resp := &kvrpcpb.RawScanResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	//defer reader.Close()

	iter := reader.IterCF(req.Cf)

	var num uint32 = 0

	for iter.Seek(req.StartKey); iter.Valid(); iter.Next() {
		if num >= req.Limit {
			break
		}
		num++
		item := iter.Item()
		val, err := item.ValueCopy(nil)
		if err != nil {                 // error occurs
			return nil, err
		}
		resp.Kvs = append(resp.Kvs, &kvrpcpb.KvPair{
			Key:   item.KeyCopy(nil),
			Value: val,
		})
	}
	return resp, nil
}
