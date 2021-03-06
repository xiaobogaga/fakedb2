package prog

import (
	"context"
	"github.com/xiaobogaga/fakedb2/internal"
	"github.com/xiaobogaga/fakedb2/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"net"
	"strconv"
	"sync"
	"time"
)

var serverLog = util.GetLog("server")

type Server struct {
	UnimplementedServiceMethodsServer
	Lock               sync.Mutex
	transactionManager *internal.TransactionManager
	Sessions           map[int32]*Session
	SessionId          int32
}

var bufferSize = 1024

func NewServer(ctx context.Context, pageDataFile, walFile, checkPointFile string, flushDuration time.Duration) (*Server, error) {
	serverLog.InfoF("init buffer manager")
	// First. do recovery
	bufManager, err := internal.NewBufferManager(ctx, pageDataFile, flushDuration)
	if err != nil {
		return nil, err
	}
	serverLog.InfoF("init log manager")
	logManager, err := internal.NewLogManager(ctx, bufferSize, checkPointFile, walFile, flushDuration)
	if err != nil {
		return nil, err
	}
	serverLog.InfoF("start recovery")
	oracle := internal.Recovery(logManager, bufManager)
	serverLog.InfoF("start trans manager")
	txnManager := internal.NewTransactionManager(logManager.MaxTransactionId+1, bufManager, logManager, oracle)
	server := &Server{
		transactionManager: txnManager,
		Sessions:           map[int32]*Session{},
	}
	return server, nil
}

func (server *Server) Get(ctx context.Context, input *GetRequest) (*Response, error) {
	serverLog.InfoF("handle get request: clientId: %d, key: %s", input.ClientId, input.Key)
	server.Lock.Lock()
	session := server.Sessions[input.ClientId]
	server.Lock.Unlock()
	resp := &Response{ClientId: input.ClientId}
	if session == nil {
		resp.Error = "invalid clientId. please init first"
		return resp, nil
	}
	txn := session.Txn
	if session.Txn == nil {
		txn = server.transactionManager.NewTransaction()
		txn.Begin()
	}
	defer func() {
		if session.Txn == nil {
			txn.Commit()
		}
	}()
	_, ok, value, err := txn.Get([]byte(input.Key))
	if err != nil {
		resp.Error = err.Error()
		return resp, nil
	}
	if !ok {
		resp.Error = internal.ErrKeyNotFound.Error()
		return resp, nil
	}
	resp.Value = string(value)
	return resp, nil
}

func (server *Server) Get2(ctx context.Context, input *GetRequest) (*Response, error) {
	serverLog.InfoF("handle get2 request: clientId: %d, key: %s", input.ClientId, input.Key)
	server.Lock.Lock()
	session := server.Sessions[input.ClientId]
	server.Lock.Unlock()
	resp := &Response{ClientId: input.ClientId}
	if session == nil {
		resp.Error = "invalid clientId. please init first"
		return resp, nil
	}
	txn := session.Txn
	if session.Txn == nil {
		txn = server.transactionManager.NewTransaction()
		txn.Begin()
	}
	defer func() {
		if session.Txn == nil {
			txn.Commit()
		}
	}()
	_, ok, value, err := txn.Get([]byte(input.Key))
	if err != nil {
		resp.Error = err.Error()
		return resp, nil
	}
	if !ok {
		resp.Error = internal.ErrKeyNotFound.Error()
		return resp, nil
	}
	resp.Value = string(value)
	return resp, nil
}

func (server *Server) Set(ctx context.Context, input *SetRequest) (resp *Response, err error) {
	server.Lock.Lock()
	session := server.Sessions[input.ClientId]
	server.Lock.Unlock()
	resp = &Response{ClientId: input.ClientId}
	if session == nil {
		resp.Error = "invalid clientId. please init first"
		return resp, nil
	}
	txn := session.Txn
	if session.Txn == nil {
		txn = server.transactionManager.NewTransaction()
		txn.Begin()
	}
	defer func() {
		if session.Txn == nil {
			err1 := txn.Commit()
			if err1 != nil && resp.Error == "" {
				resp.Error = err1.Error()
			}
		}
	}()
	err = txn.Set([]byte(input.Key), []byte(input.Value))
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, nil
}

func (server *Server) Begin(ctx context.Context, input *BeginRequest) (*Response, error) {
	server.Lock.Lock()
	session := server.Sessions[input.ClientId]
	server.Lock.Unlock()
	resp := &Response{ClientId: input.ClientId}
	if session == nil {
		resp.Error = "invalid clientId. please init first"
		return resp, nil
	}
	txn := session.Txn
	if txn != nil {
		resp.Error = "please commit or abort old transaction first"
		return resp, nil
	}
	txn = server.transactionManager.NewTransaction()
	txn.Begin()
	session.Txn = txn
	resp.TransactionId = txn.TransactionId
	return resp, nil
}

func (server *Server) Commit(ctx context.Context, input *CommitRequest) (*Response, error) {
	server.Lock.Lock()
	session := server.Sessions[input.ClientId]
	server.Lock.Unlock()
	resp := &Response{ClientId: input.ClientId}
	if session == nil {
		resp.Error = "invalid clientId. please init first"
		return resp, nil
	}
	txn := session.Txn
	if txn == nil {
		resp.Error = "please begin a transaction first"
		return resp, nil
	}
	err := txn.Commit()
	if err != nil {
		resp.Error = err.Error()
	}
	session.Txn = nil
	return resp, nil
}

func (server *Server) Rollback(ctx context.Context, input *RollbackRequest) (*Response, error) {
	serverLog.InfoF("handle rollback request: clientId: %d", input.ClientId)
	server.Lock.Lock()
	session := server.Sessions[input.ClientId]
	server.Lock.Unlock()
	resp := &Response{ClientId: input.ClientId}
	if session == nil {
		resp.Error = "invalid clientId. please init first"
		return resp, nil
	}
	txn := session.Txn
	if txn == nil {
		resp.Error = "please begin a transaction first"
		return resp, nil
	}
	txn.Rollback()
	session.Txn = nil
	return resp, nil
}

func (server *Server) Close(ctx context.Context, input *CloseRequest) (*Response, error) {
	server.Lock.Lock()
	session := server.Sessions[input.ClientId]
	server.Lock.Unlock()
	resp := &Response{ClientId: input.ClientId}
	if session == nil {
		resp.Error = "invalid clientId. please init first"
		return resp, nil
	}
	delete(server.Sessions, input.ClientId)
	if session.Txn != nil {
		session.Txn.Rollback()
	}
	return resp, nil
}

func (server *Server) Init(ctx context.Context, input *InitRequest) (*Response, error) {
	serverLog.InfoF("handle init request: ")
	server.Lock.Lock()
	defer server.Lock.Unlock()
	server.Sessions[server.SessionId] = &Session{
		SessionId: server.SessionId,
	}
	resp := &Response{ClientId: server.SessionId}
	server.SessionId++
	return resp, nil
}

func (server *Server) Del(ctx context.Context, input *DelRequest) (*Response, error) {
	serverLog.InfoF("handle del request: ")
	server.Lock.Lock()
	session := server.Sessions[input.ClientId]
	server.Lock.Unlock()
	resp := &Response{ClientId: input.ClientId}
	if session == nil {
		resp.Error = "invalid clientId. please init first"
		return resp, nil
	}
	txn := session.Txn
	if session.Txn == nil {
		txn = server.transactionManager.NewTransaction()
		txn.Begin()
	}
	defer func() {
		if session.Txn == nil {
			err1 := txn.Commit()
			if err1 != nil && resp.Error == "" {
				resp.Error = err1.Error()
			}
		}
	}()
	err := txn.Del([]byte(input.Key))
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, nil
}

func (server *Server) Flush(ctx context.Context, input *FlushRequest) (*Response, error) {
	serverLog.InfoF("handle flush request")
	server.transactionManager.Log.FlushLogs()
	return &Response{}, nil
}

func (server *Server) FlushDirtyPage(ctx context.Context, input *FlushDirtyPageRequest) (*Response, error) {
	serverLog.InfoF("handle flush request")
	server.transactionManager.Buf.FlushDirtyPage(server.transactionManager.Log)
	return &Response{}, nil
}

func (server *Server) CheckPoint(ctx context.Context, input *CheckPointRequest) (*Response, error) {
	serverLog.InfoF("handle flush request")
	server.transactionManager.Log.DoCheckPoint(server.transactionManager.Buf, server.transactionManager.Oracle)
	return &Response{}, nil
}

func RunServer(ctx context.Context, port int, dataFile, checkPointFile, walFile string, duration time.Duration) {
	// duration = time.Millisecond * 200
	listener, err := net.Listen("tcp", ":"+strconv.FormatInt(int64(port), 10))
	if err != nil {
		panic(err)
	}
	server, err := NewServer(ctx, dataFile, walFile, checkPointFile, duration)
	if err != nil {
		panic(err)
	}
	s := grpc.NewServer()
	RegisterServiceMethodsServer(s, server)
	reflection.Register(s)
	err = s.Serve(listener)
	if err != nil {
		panic(err)
	}
}

type Session struct {
	Txn       *internal.Transaction
	SessionId int32
}
