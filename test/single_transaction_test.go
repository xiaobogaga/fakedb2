package test

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/xiaobogaga/fakedb2/prog"
	"github.com/xiaobogaga/fakedb2/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

var namePrefix = "fakedb2-test"
var dataFile = "/tmp/" + namePrefix + ".db"
var checkPointFile = "/tmp/" + namePrefix + ".checkpoint"
var walFile = "/tmp/" + namePrefix + ".walFile"
var port = 10010
var logPath = "/tmp/" + namePrefix + ".log"
var random = rand.New(rand.NewSource(time.Now().Unix()))

func RunServer(ctx context.Context, port int, dataFile, checkPointFile, walFile string) net.Listener {
	flushDuration := time.Millisecond * 200
	listener, err := net.Listen("tcp", ":"+strconv.FormatInt(int64(port), 10))
	if err != nil {
		panic(err)
	}
	server, err := prog.NewServer(ctx, dataFile, walFile, checkPointFile, flushDuration)
	if err != nil {
		panic(err)
	}
	s := grpc.NewServer()
	prog.RegisterServiceMethodsServer(s, server)
	reflection.Register(s)
	go func() {
		_ = s.Serve(listener)
	}()
	return listener
}

func startTestServer(t *testing.T) (context.CancelFunc, net.Listener) {
	ctx, cancel := context.WithCancel(context.Background())
	listener := RunServer(ctx, port, dataFile, checkPointFile, walFile)
	return cancel, listener
}

func InitLog(t *testing.T) {
	err := util.InitLogger(logPath, 1024*16, time.Second*10, true)
	assert.Nil(t, err)
}

func clearTestData(t *testing.T) {
	err := os.Remove(dataFile)
	assert.Nil(t, err)
	err = os.Remove(checkPointFile)
	assert.Nil(t, err)
	err = os.Remove(walFile)
	assert.Nil(t, err)
	util.CloseLog()
	os.Remove(logPath)
}

func createTestClient(t *testing.T) (*grpc.ClientConn, prog.ServiceMethodsClient, *prog.ClientSession) {
	address := "localhost:" + strconv.FormatInt(int64(port), 10)
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	client := prog.NewServiceMethodsClient(conn)
	resp, err := client.Init(context.Background(), &prog.InitRequest{})
	assert.Nil(t, err)
	assert.Equal(t, "", resp.Error)
	return conn, client, &prog.ClientSession{ClientId: resp.ClientId}
}

func InitNewClient(t *testing.T, client prog.ServiceMethodsClient) *prog.ClientSession {
	resp, err := client.Init(context.Background(), &prog.InitRequest{})
	assert.Nil(t, err)
	assert.Equal(t, "", resp.Error)
	return &prog.ClientSession{ClientId: resp.ClientId}
}

func CloseOldClient(client prog.ServiceMethodsClient, session *prog.ClientSession) {
	client.Close(context.Background(), &prog.CloseRequest{ClientId: session.ClientId})
}

func Set(t *testing.T, client prog.ServiceMethodsClient, session *prog.ClientSession, args []string, expected string) {
	if len(args) != 2 {
		println("client: err: wrong command format")
		return
	}
	key := args[0]
	value := args[1]
	setRequest := &prog.SetRequest{
		ClientId: session.ClientId,
		Key:      key,
		Value:    value,
	}
	if session.TransactionId != nil {
		setRequest.TransactionId = *session.TransactionId
	}
	resp, err := client.Set(context.Background(), setRequest)
	assert.Nil(t, err)
	assert.Equal(t, "", resp.Error)
	time.Sleep(time.Millisecond*300 + time.Millisecond*time.Duration(rand.Intn(400)))
}

// del key
func Del(t *testing.T, client prog.ServiceMethodsClient, session *prog.ClientSession, args []string, expectedErrMsg string) {
	if len(args) != 1 {
		println("client: err: wrong command format")
		return
	}
	key := args[0]
	delRequest := &prog.DelRequest{
		ClientId: session.ClientId,
		Key:      key,
	}
	if session.TransactionId != nil {
		delRequest.TransactionId = *session.TransactionId
	}
	resp, err := client.Del(context.Background(), delRequest)
	assert.Nil(t, err)
	assert.Equal(t, expectedErrMsg, resp.Error)
	time.Sleep(time.Millisecond*300 + time.Millisecond*time.Duration(rand.Intn(400)))
}

var keyNotFound = "key not found"

//  get key
func Get(t *testing.T, client prog.ServiceMethodsClient, session *prog.ClientSession, args []string, expected string, expectedErrMsg string) {
	if len(args) != 1 {
		println("client: err: wrong command format")
		return
	}
	key := args[0]
	getRequest := &prog.GetRequest{
		ClientId: session.ClientId,
		Key:      key,
	}
	if session.TransactionId != nil {
		getRequest.TransactionId = *session.TransactionId
	}
	resp, err := client.Get(context.Background(), getRequest)
	assert.Nil(t, err)
	assert.Equal(t, expectedErrMsg, resp.Error)
	assert.Equal(t, expected, resp.Value)
	time.Sleep(time.Millisecond*300 + time.Millisecond*time.Duration(rand.Intn(400)))
}

func Get2(t *testing.T, client prog.ServiceMethodsClient, session *prog.ClientSession, args []string, expected string, expectedErrMsg string) {
	if len(args) != 1 {
		println("client: err: wrong command format")
		return
	}
	key := args[0]
	getRequest := &prog.GetRequest{
		ClientId: session.ClientId,
		Key:      key,
	}
	if session.TransactionId != nil {
		getRequest.TransactionId = *session.TransactionId
	}
	resp, err := client.Get2(context.Background(), getRequest)
	assert.Nil(t, err)
	assert.Equal(t, expectedErrMsg, resp.Error)
	assert.Equal(t, expected, resp.Value)
	time.Sleep(time.Millisecond*300 + time.Millisecond*time.Duration(rand.Intn(400)))
}

func Begin(t *testing.T, client prog.ServiceMethodsClient, session *prog.ClientSession) {
	if session.TransactionId != nil {
		println("client: already in a transaction")
		return
	}
	beginRequest := &prog.BeginRequest{
		ClientId: session.ClientId,
	}
	resp, err := client.Begin(context.Background(), beginRequest)
	assert.Nil(t, err)
	assert.Equal(t, "", resp.Error)
	session.TransactionId = &resp.TransactionId
	time.Sleep(time.Millisecond*300 + time.Millisecond*time.Duration(rand.Intn(400)))
}

func Rollback(t *testing.T, client prog.ServiceMethodsClient, session *prog.ClientSession) {
	if session.TransactionId == nil {
		println("client: not in a transaction, please begin transaction first")
		return
	}
	rollBackRequest := &prog.RollbackRequest{
		ClientId:      session.ClientId,
		TransactionId: *session.TransactionId,
	}
	resp, err := client.Rollback(context.Background(), rollBackRequest)
	assert.Nil(t, err)
	assert.Equal(t, "", resp.Error)
	session.TransactionId = nil
	time.Sleep(time.Millisecond*300 + time.Millisecond*time.Duration(rand.Intn(400)))
}

func Commit(t *testing.T, client prog.ServiceMethodsClient, session *prog.ClientSession, expectedErr string) {
	if session.TransactionId == nil {
		println("client: not in a transaction, please begin transaction first")
		return
	}
	commitRequest := &prog.CommitRequest{
		ClientId:      session.ClientId,
		TransactionId: *session.TransactionId,
	}
	resp, err := client.Commit(context.Background(), commitRequest)
	assert.Nil(t, err)
	if expectedErr == "" {
		assert.Equal(t, expectedErr, resp.Error)
	} else {
		assert.True(t, strings.Contains(resp.Error, expectedErr))
	}
	session.TransactionId = nil
	time.Sleep(time.Millisecond*300 + time.Millisecond*time.Duration(rand.Intn(400)))
}

func TestSingleTransaction_many_actions(t *testing.T) {
	InitLog(t)
	cancel, listener := startTestServer(t)
	defer clearTestData(t)
	conn, client, session := createTestClient(t)
	key1 := "1"
	key2 := "2"
	val1 := "1"
	val2 := "2"
	val11 := "11"
	val22 := "22"

	Get(t, client, session, []string{key1}, "", keyNotFound)
	Get(t, client, session, []string{key2}, "", keyNotFound)
	Set(t, client, session, []string{key1, val1}, "")
	Set(t, client, session, []string{key2, val2}, "")
	Get(t, client, session, []string{key1}, val1, "")
	Get(t, client, session, []string{key2}, val2, "")
	Begin(t, client, session)
	Set(t, client, session, []string{key1, val11}, "")
	Get(t, client, session, []string{key1}, val11, "")
	Get(t, client, session, []string{key2}, val2, "")
	Set(t, client, session, []string{key2, val22}, "")
	Get2(t, client, session, []string{key2}, val22, "")
	Rollback(t, client, session)
	Get(t, client, session, []string{key1}, val1, "")
	Get(t, client, session, []string{key2}, val2, "")
	Del(t, client, session, []string{key1}, "")
	Del(t, client, session, []string{key2}, "")
	Begin(t, client, session)
	Get(t, client, session, []string{key1}, "", keyNotFound)
	Get(t, client, session, []string{key2}, "", keyNotFound)
	Set(t, client, session, []string{key1, val11}, "")
	Get2(t, client, session, []string{key1}, val11, "")
	Set(t, client, session, []string{key2, val2}, "")
	Get2(t, client, session, []string{key2}, val2, "")
	Commit(t, client, session, "")
	Begin(t, client, session)
	Get2(t, client, session, []string{key1}, val11, "")
	Get2(t, client, session, []string{key2}, val2, "")
	closeServerClient(t, cancel, conn, listener)
}

func TestSingleTransaction(t *testing.T) {
	InitLog(t)
	cancel, listener := startTestServer(t)
	defer clearTestData(t)
	conn, client, session := createTestClient(t)
	// set two keys.
	key1 := "1"
	val1 := "1"
	key2 := "2"
	val2 := "2"
	tempKey := "temp"
	Set(t, client, session, []string{key1, val1}, "")
	Set(t, client, session, []string{key2, val2}, "")
	Get(t, client, session, []string{key1}, val1, "")
	Get(t, client, session, []string{key2}, val2, "")
	Get(t, client, session, []string{tempKey}, "", keyNotFound)
	Del(t, client, session, []string{key1}, "")
	Del(t, client, session, []string{key2}, "")
	Del(t, client, session, []string{tempKey}, keyNotFound)
	// Second round.
	Set(t, client, session, []string{key1, val1}, "")
	Set(t, client, session, []string{key2, val2}, "")
	Get(t, client, session, []string{key1}, val1, "")
	Get(t, client, session, []string{key2}, val2, "")
	Get(t, client, session, []string{tempKey}, "", keyNotFound)
	Del(t, client, session, []string{key1}, "")
	Del(t, client, session, []string{key2}, "")
	Del(t, client, session, []string{tempKey}, keyNotFound)
	closeServerClient(t, cancel, conn, listener)
}

func TestSingleTransaction_begin_commit_rollback(t *testing.T) {
	InitLog(t)
	defer clearTestData(t)
	cancel, listener := startTestServer(t)
	conn, client, session := createTestClient(t)
	key1 := "1"
	val1 := "1"
	key2 := "2"
	val2 := "2"
	tempKey := "temp"
	// Test begin-commit first.
	Begin(t, client, session)
	Set(t, client, session, []string{key1, val1}, "")
	Set(t, client, session, []string{key2, val2}, "")
	Get(t, client, session, []string{key1}, val1, "")
	Get(t, client, session, []string{key2}, val2, "")
	Get(t, client, session, []string{tempKey}, "", keyNotFound)
	Commit(t, client, session, "")
	CloseOldClient(client, session)
	session = InitNewClient(t, client)
	Get(t, client, session, []string{key1}, val1, "")
	Get(t, client, session, []string{key2}, val2, "")
	Get(t, client, session, []string{tempKey}, "", keyNotFound)

	closeServerClient(t, cancel, conn, listener)
	// Restart
	cancel, listener = startTestServer(t)
	conn, client, session = createTestClient(t)
	Get(t, client, session, []string{key2}, val2, "")
	Get(t, client, session, []string{key1}, val1, "")
	Get(t, client, session, []string{tempKey}, "", keyNotFound)
	closeServerClient(t, cancel, conn, listener)

	// restart, Begin-del-rollback
	cancel, listener = startTestServer(t)
	conn, client, session = createTestClient(t)
	Del(t, client, session, []string{key1}, "")
	Begin(t, client, session)
	Del(t, client, session, []string{key2}, "")
	Rollback(t, client, session)
	Get(t, client, session, []string{tempKey}, "", keyNotFound)
	Get(t, client, session, []string{key1}, "", keyNotFound)
	Get(t, client, session, []string{key2}, val2, "")
	closeServerClient(t, cancel, conn, listener)

	// restart check.
	cancel, listener = startTestServer(t)
	conn, client, session = createTestClient(t)
	Get(t, client, session, []string{tempKey}, "", keyNotFound)
	Get(t, client, session, []string{key1}, "", keyNotFound)
	Get2(t, client, session, []string{key2}, val2, "")
	closeServerClient(t, cancel, conn, listener)

}

func closeServerClient(t *testing.T, cancel context.CancelFunc, conn *grpc.ClientConn, listener net.Listener) {
	cancel()
	err := conn.Close()
	assert.Nil(t, err)
	err = listener.Close()
	assert.Nil(t, err)
}

func TestSingleTransaction_restart_recovery(t *testing.T) {
	InitLog(t)
	cancel, listener := startTestServer(t)
	// defer clearTestData(t)
	conn, client, session := createTestClient(t)
	// set two keys.
	key1 := "1"
	val1 := "1"
	key2 := "2"
	val2 := "2"
	tempKey := "temp"
	Set(t, client, session, []string{key1, val1}, "")
	Set(t, client, session, []string{key2, val2}, "")
	Get(t, client, session, []string{key1}, val1, "")
	Get(t, client, session, []string{key2}, val2, "")
	Get(t, client, session, []string{tempKey}, "", keyNotFound)
	Del(t, client, session, []string{key1}, "")
	Del(t, client, session, []string{key2}, "")
	Del(t, client, session, []string{tempKey}, keyNotFound)
	// Second round.
	Set(t, client, session, []string{key1, val1}, "")
	Set(t, client, session, []string{key2, val2}, "")
	Get(t, client, session, []string{key1}, val1, "")
	Get(t, client, session, []string{key2}, val2, "")
	Get(t, client, session, []string{tempKey}, "", keyNotFound)
	Del(t, client, session, []string{key1}, "")

	closeServerClient(t, cancel, conn, listener)
	// restart
	cancel, listener = startTestServer(t)
	conn, client, session = createTestClient(t)
	Get(t, client, session, []string{key2}, val2, "")
	Get(t, client, session, []string{tempKey}, "", keyNotFound)
	Get(t, client, session, []string{key1}, "", keyNotFound)
	closeServerClient(t, cancel, conn, listener)
	clearTestData(t)
}
