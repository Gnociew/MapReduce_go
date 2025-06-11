# 编译 wordcount_mapper.go 和 wordcount_reducer.go
go build -buildmode=plugin -o=./compiled/wordcount_mapper.so ./udf/wordcount_mapper.go
go build -buildmode=plugin -o=./compiled/wordcount_reducer.so ./udf/wordcount_reducer.go
go build -buildmode=plugin -o=./compiled/wordcount_combiner.so ./udf/wordcount_combiner.go

# 编译 master 和 worker
go build -o ./compiled/master ./cmd/master/main.go
go build -o ./compiled/worker ./cmd/worker/main.go

go run main.go -mode=MapReduce
