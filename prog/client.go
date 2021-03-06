package prog

import (
	"bufio"
	"context"
	"fmt"
	"google.golang.org/grpc"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

var welcomeMessage = "hi :)"

func showWelcomeMessage() {
	println(welcomeMessage)
}

var prompt = "fakedb2> "

func showPrompt() {
	fmt.Print(prompt)
}

func showHelp() {
	help := `* set key value
* get key
* del key
* begin
* commit
* rollback
* help
`
	fmt.Printf(help)
}

func interact(sig chan os.Signal, client ServiceMethodsClient) {
	showWelcomeMessage()
	reader := bufio.NewReader(os.Stdin)
	resp, err := client.Init(context.Background(), &InitRequest{})
	if err != nil {
		panic(err)
	}
	if resp.Error != "" {
		fmt.Printf("server: init err: %s\n", resp.Error)
		return
	}
	lock := &sync.Mutex{}
	session := &ClientSession{ClientId: resp.ClientId}
	go func() {
		<-sig
		HandleQuit(lock, client, session, nil)
		os.Exit(0)
	}()
	for {
		showPrompt()
		inputBytes, _, _ := reader.ReadLine()
		input := strings.TrimSpace(string(inputBytes))
		splits := strings.Split(input, " ")
		if len(splits) == 0 {
			println("client: input format is not correct")
			continue
		}
		switch splits[0] {
		case "quit":
			HandleQuit(lock, client, session, splits[1:])
			return
		case "begin":
			HandleBegin(lock, client, session, splits[1:])
		case "rollback":
			HandleRollback(lock, client, session, splits[1:])
		case "commit":
			HandleCommit(lock, client, session, splits[1:])
		case "set":
			HandleSet(lock, client, session, splits[1:])
		case "del":
			HandleDel(lock, client, session, splits[1:])
		case "get":
			HandleGet(lock, client, session, splits[1:])
		case "flushLog":
			HandleFlush(client, session, splits[1:])
		case "flushDirtyPage":
			HandleFlushDiryPage(client, session, splits[1:])
		case "checkPoint":
			HandleCheckPoint(client, session, splits[1:])
		case "help":
			showHelp()
		default:
			println("client: unknown command")
		}
	}
}

type ClientSession struct {
	ClientId      int32
	TransactionId *uint64
}

func HandleQuit(lock *sync.Mutex, client ServiceMethodsClient, session *ClientSession, args []string) {
	lock.Lock()
	defer lock.Unlock()
	if len(args) > 0 {
		println("client: err: wrong command format")
		return
	}
	if session.ClientId < 0 {
		return
	}
	closeRequest := &CloseRequest{
		ClientId: session.ClientId,
	}
	resp, err := client.Close(context.Background(), closeRequest)
	if err != nil {
		fmt.Printf("client: err: %s\n", err.Error())
		return
	}
	if resp.Error != "" {
		fmt.Printf("server: err: %s\n", resp.Error)
	}
	println("bye")
	session.ClientId = -1
}

func HandleBegin(lock *sync.Mutex, client ServiceMethodsClient, session *ClientSession, args []string) {
	lock.Lock()
	defer lock.Unlock()
	if len(args) > 0 {
		println("client: err: wrong command format")
		return
	}
	if session.TransactionId != nil {
		println("client: already in a transaction")
		return
	}
	beginRequest := &BeginRequest{
		ClientId: session.ClientId,
	}
	resp, err := client.Begin(context.Background(), beginRequest)
	if err != nil {
		fmt.Printf("client: err: %s\n", err.Error())
		return
	}
	if resp.Error != "" {
		fmt.Printf("server: err: %s\n", resp.Error)
	}
	session.TransactionId = &resp.TransactionId
}

func HandleRollback(lock *sync.Mutex, client ServiceMethodsClient, session *ClientSession, args []string) {
	lock.Lock()
	defer lock.Unlock()
	if len(args) > 0 {
		println("client: err: wrong command format")
		return
	}
	if session.TransactionId == nil {
		println("client: not in a transaction, please begin transaction first")
		return
	}
	rollBackRequest := &RollbackRequest{
		ClientId:      session.ClientId,
		TransactionId: *session.TransactionId,
	}
	resp, err := client.Rollback(context.Background(), rollBackRequest)
	if err != nil {
		fmt.Printf("client: err: %s\n", err.Error())
		return
	}
	if resp.Error != "" {
		fmt.Printf("server: err: %s\n", resp.Error)
	}
	session.TransactionId = nil
}

func HandleCommit(lock *sync.Mutex, client ServiceMethodsClient, session *ClientSession, args []string) {
	lock.Lock()
	defer lock.Unlock()
	if len(args) > 0 {
		println("client: err: wrong command format")
		return
	}
	if session.TransactionId == nil {
		println("client: not in a transaction, please begin transaction first")
		return
	}
	commitRequest := &CommitRequest{
		ClientId:      session.ClientId,
		TransactionId: *session.TransactionId,
	}
	resp, err := client.Commit(context.Background(), commitRequest)
	if err != nil {
		fmt.Printf("client: err: %s\n", err.Error())
		return
	}
	if resp.Error != "" {
		fmt.Printf("server: err: %s\n", resp.Error)
	}
	session.TransactionId = nil
}

// set key, value
func HandleSet(lock *sync.Mutex, client ServiceMethodsClient, session *ClientSession, args []string) {
	lock.Lock()
	defer lock.Unlock()
	if len(args) != 2 {
		println("client: err: wrong command format")
		return
	}
	key := args[0]
	value := args[1]
	setRequest := &SetRequest{
		ClientId: session.ClientId,
		Key:      key,
		Value:    value,
	}
	if session.TransactionId != nil {
		setRequest.TransactionId = *session.TransactionId
	}
	resp, err := client.Set(context.Background(), setRequest)
	if err != nil {
		fmt.Printf("client: err: %s\n", err.Error())
		return
	}
	if resp.Error != "" {
		fmt.Printf("server: err: %s\n", resp.Error)
	}
}

// del key
func HandleDel(lock *sync.Mutex, client ServiceMethodsClient, session *ClientSession, args []string) {
	lock.Lock()
	defer lock.Unlock()
	if len(args) != 1 {
		println("client: err: wrong command format")
		return
	}
	key := args[0]
	delRequest := &DelRequest{
		ClientId: session.ClientId,
		Key:      key,
	}
	if session.TransactionId != nil {
		delRequest.TransactionId = *session.TransactionId
	}
	resp, err := client.Del(context.Background(), delRequest)
	if err != nil {
		fmt.Printf("client: err: %s\n", err.Error())
		return
	}
	if resp.Error != "" {
		fmt.Printf("server: err: %s\n", resp.Error)
	}
}

//  get key
func HandleGet(lock *sync.Mutex, client ServiceMethodsClient, session *ClientSession, args []string) {
	lock.Lock()
	defer lock.Unlock()
	if len(args) != 1 {
		println("client: err: wrong command format")
		return
	}
	key := args[0]
	getRequest := &GetRequest{
		ClientId: session.ClientId,
		Key:      key,
	}
	if session.TransactionId != nil {
		getRequest.TransactionId = *session.TransactionId
	}
	resp, err := client.Get(context.Background(), getRequest)
	if err != nil {
		fmt.Printf("client: err: %s\n", err.Error())
		return
	}
	if resp.Error != "" {
		fmt.Printf("server: err: %s\n", resp.Error)
		return
	}
	fmt.Printf("server: ok, value: %s\n", string(resp.Value))
}

func HandleFlush(client ServiceMethodsClient, session *ClientSession, args []string) {
	if len(args) > 0 {
		println("client: err: wrong command format")
		return
	}
	flushRequest := &FlushRequest{}
	resp, err := client.Flush(context.Background(), flushRequest)
	if err != nil {
		fmt.Printf("client: err: %s\n", err.Error())
		return
	}
	if resp.Error != "" {
		fmt.Printf("server: err: %s\n", resp.Error)
		return
	}
	fmt.Printf("server: ok\n")
}

func HandleFlushDiryPage(client ServiceMethodsClient, session *ClientSession, args []string) {
	if len(args) > 0 {
		println("client: err: wrong command format")
		return
	}
	flushRequest := &FlushDirtyPageRequest{}
	resp, err := client.FlushDirtyPage(context.Background(), flushRequest)
	if err != nil {
		fmt.Printf("client: err: %s\n", err.Error())
		return
	}
	if resp.Error != "" {
		fmt.Printf("server: err: %s\n", resp.Error)
		return
	}
	fmt.Printf("server: ok\n")
}

func HandleCheckPoint(client ServiceMethodsClient, session *ClientSession, args []string) {
	if len(args) > 0 {
		println("client: err: wrong command format")
		return
	}
	checkPointRequest := &CheckPointRequest{}
	resp, err := client.CheckPoint(context.Background(), checkPointRequest)
	if err != nil {
		fmt.Printf("client: err: %s\n", err.Error())
		return
	}
	if resp.Error != "" {
		fmt.Printf("server: err: %s\n", resp.Error)
		return
	}
	fmt.Printf("server: ok\n")
}

func RunClient(port int) {
	address := "localhost:" + strconv.FormatInt(int64(port), 10)
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	c := NewServiceMethodsClient(conn)
	sig := make(chan os.Signal)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	interact(sig, c)
}
