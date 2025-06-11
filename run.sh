# 编译 wordcount_mapper.go 和 wordcount_reducer.go
go build -buildmode=plugin -o=./compiled/wordcount_mapper.so ./udf/wordcount_mapper.go
go build -buildmode=plugin -o=./compiled/wordcount_reducer.so ./udf/wordcount_reducer.go

# 编译 master 和 worker
go build -o ./compiled/master ./cmd/master/main.go
go build -o ./compiled/worker ./cmd/worker/main.go

go run main.go -mode=MapReduce

# 另起一个终端运行下方内容：
# go run main.go -mode=InputDir -MapPath=./compiled/wordcount_mapper.so -ReducePath=./compiled/wordcount_reducer.so -data=./Data/input