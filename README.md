- 终端 1 启动 master 和 worker
    ```
    sh run.sh
    ```

- 终端 2 输入 map、reduce、combine 函数和文件路径
    ```
    go run main.go -mode=InputDir -MapPath=./compiled/wordcount_mapper.so -ReducePath=./compiled/wordcount_reducer.so -CombinePath=./compiled/wordcount_combiner.so -data=./Data/input
    ```