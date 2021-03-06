package test

import "testing"

func Test_ReadCommitted(t *testing.T) {
	InitLog(t)
	cancel, listener := startTestServer(t)
	defer clearTestData(t)
	conn, client, session1 := createTestClient(t)
	session2 := InitNewClient(t, client)
	// set two keys. cannot read uncommitted.
	key1 := "1"
	val1 := "1"
	Begin(t, client, session1)
	Set(t, client, session1, []string{key1, val1}, "")
	Rollback(t, client, session1)
	Get(t, client, session2, []string{key1}, "", keyNotFound)
	closeServerClient(t, cancel, conn, listener)

	// Restart, read committed.
	cancel, listener = startTestServer(t)
	conn, client, session1 = createTestClient(t)
	session2 = InitNewClient(t, client)
	Set(t, client, session1, []string{key1, val1}, "")
	Get(t, client, session2, []string{key1}, val1, "")
	closeServerClient(t, cancel, conn, listener)
}

func Test_NonBlockingMultiTrans(t *testing.T) {
	InitLog(t)
	cancel, listener := startTestServer(t)
	defer clearTestData(t)

	conn, client, session1 := createTestClient(t)
	session2 := InitNewClient(t, client)
	session3 := InitNewClient(t, client)

	key1 := "1"
	val1 := "1"
	key2 := "2"
	val2 := "2"
	Begin(t, client, session1)
	Set(t, client, session1, []string{key1, val1}, "")
	Get(t, client, session1, []string{key1}, val1, "")
	Get(t, client, session1, []string{key2}, "", keyNotFound)

	Begin(t, client, session2)
	Get(t, client, session2, []string{key1}, "", keyNotFound)
	Set(t, client, session2, []string{key2, val2}, "")
	Get(t, client, session2, []string{key2}, val2, "")

	Begin(t, client, session3)
	Get(t, client, session1, []string{key2}, "", keyNotFound)
	Get(t, client, session2, []string{key1}, "", keyNotFound)
	closeServerClient(t, cancel, conn, listener)
}

func Test_ReadSnapshot(t *testing.T) {
	InitLog(t)
	cancel, listener := startTestServer(t)
	defer clearTestData(t)

	conn, client, session1 := createTestClient(t)
	session2 := InitNewClient(t, client)
	session3 := InitNewClient(t, client)

	key1 := "1"
	val1 := "1"

	Set(t, client, session1, []string{key1, val1}, "")
	Begin(t, client, session2)
	Del(t, client, session1, []string{key1}, "")
	Begin(t, client, session3)
	Get(t, client, session3, []string{key1}, "", keyNotFound)
	Get(t, client, session2, []string{key1}, val1, "")
	closeServerClient(t, cancel, conn, listener)
}

func Test_WriteWriteConflict(t *testing.T) {
	InitLog(t)
	cancel, listener := startTestServer(t)
	defer clearTestData(t)

	conn, client, session1 := createTestClient(t)
	session2 := InitNewClient(t, client)

	key1 := "1"
	val1 := "1"
	val11 := "11"

	Begin(t, client, session1)
	Begin(t, client, session2)
	Set(t, client, session1, []string{key1, val1}, "")
	Set(t, client, session2, []string{key1, val11}, "")
	Commit(t, client, session1, "")
	Commit(t, client, session2, "conflict")
	closeServerClient(t, cancel, conn, listener)

	cancel, listener = startTestServer(t)
	conn, client, session3 := createTestClient(t)
	Get(t, client, session3, []string{key1}, val1, "")
	Begin(t, client, session3)
	Set(t, client, session3, []string{key1, val11}, "")
	Commit(t, client, session3, "")
	session4 := InitNewClient(t, client)
	Get(t, client, session4, []string{key1}, val11, "")
	closeServerClient(t, cancel, conn, listener)

	cancel, listener = startTestServer(t)
	conn, client, session5 := createTestClient(t)
	Get(t, client, session5, []string{key1}, val11, "")
	closeServerClient(t, cancel, conn, listener)
}

//func Test_RepeatableRead(t *testing.T) {
//	InitLog(t)
//	cancel, listener := startTestServer(t)
//	defer clearTestData(t)
//	conn, client, session1 := createTestClient(t)
//	session2 := InitNewClient(t, client)
//	// set two keys. cannot read uncommitted.
//	key1 := "1"
//	val1 := "1"
//	Set(t, client, session1, []string{key1, val1}, "")
//	Begin(t, client, session2)
//	Get(t, client, session2, []string{key1}, val1, "")
//	Begin(t, client, session1)
//	Del(t, client, session1, []string{key1}, "")
//
//	Get(t, client, session2, []string{key1}, val1, "")
//	closeServerClient(t, cancel, conn, listener)
//}
