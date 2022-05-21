package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	rsp := &kvrpcpb.RawGetResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return rsp, err
	}
	defer reader.Close()

	rsp.Value, err = reader.GetCF(req.Cf, req.Key)
	if rsp.Value == nil {
		rsp.NotFound = true
	}
	return rsp, err
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	var mod storage.Modify
	mod.Data = storage.Put{
		Key:   req.Key,
		Value: req.Value,
		Cf:    req.Cf,
	}
	batch := []storage.Modify{mod}
	err := server.storage.Write(nil, batch)

	return &kvrpcpb.RawPutResponse{}, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	var mod storage.Modify
	mod.Data = storage.Delete{
		Key: req.Key,
		Cf:  req.Cf,
	}
	batch := []storage.Modify{mod}
	err := server.storage.Write(nil, batch)

	return &kvrpcpb.RawDeleteResponse{}, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	rsp := &kvrpcpb.RawScanResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return rsp, err
	}
	iter := reader.IterCF(req.Cf)
	iter.Seek(req.StartKey)
	kvs := make([]*kvrpcpb.KvPair, 0)
	limit := req.Limit
	for ; iter.Valid(); iter.Next() {
		item := iter.Item()
		value, _ := item.Value()
		kvs = append(kvs, &kvrpcpb.KvPair{
			Key:   item.Key(),
			Value: value,
		})
		limit -= 1
		if limit == 0 {
			break
		}
	}
	rsp.Kvs = kvs
	iter.Close()
	reader.Close()
	return rsp, nil
}
