fakedb2, a transactional database support snapshot isolation, based on tso, can save two key-value pairs. Key, value size cannot be larger than 8K.
Built for learning purpose.

## Install:

```shell script
go get github.com/xiaobogaga/fakedb2
go install github.com/xiaobogaga/fakedb2/fakedb2
```

## Usage

### start server:

```shell script
fakedb2
```

### start client cli

```shell script
fakedb2 -client
hi :)
fakedb2> help
* set key value
* get key
* del key
* begin
* commit
* rollback
* help
fakedb2>
```
and then type help for more information. begin, commit, rollback to begin, commit or rollback a transaction.

