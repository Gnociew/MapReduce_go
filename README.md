
# 简易 MapReduce 框架

## 项目简介

这是一个用 Go 语言编写的简易 MapReduce 框架，旨在提供一个基础的并行计算框架。它能够将一个数据处理任务分解成多个小任务并分配到多个工作节点上进行并行计算。

该框架包含以下模块：
- **Master**：负责协调整个任务的执行。
- **Worker**：负责实际的计算工作：
    - **Map 阶段**：将输入数据映射为中间 `(key, value)` 对。
    - **Combine 阶段（可选）**：将 Map 阶段得到的中间结果进行合并。
    - **shuffle 阶段**：通过哈希优化 shuffle 操作，使 Reduce 任务更为均衡。
    - **Reduce 阶段**：对每个 `key` 对应的所有值进行聚合。

## 功能介绍
该框架支持以下功能：
- **支持自定义 Map, Reduce 和 Combine 函数**：提供了可编程的 map、combine 和 reduce 接口；用户可以自定义具体的映射、合并和聚合操作。
- **map 和 reduce 任务的数量**：map 任务数量由输入数据量决定，reduce 任务数量由 map 任务数量决定。
- **并行计算**：通过多个工作节点同时处理数据，提高计算效率。
- **心跳机制**：检测 Master 和 Worker 之间的健康状态。
- **支持多文件读入**：用户可将任意数量文件放在同一文件夹下，传入文件夹路径即可。

## 版本

- `go1.23.3 darwin/arm64`

## 使用
1. 直接运行 wordcount 程序：
    - 终端 1 启动 master 和 worker
        ```
        sh run.sh
        ```

    - 终端 2 输入 map、reduce、combine 函数和文件路径
        ```
        go run main.go -mode=InputDir -MapPath=./compiled/wordcount_mapper.so -ReducePath=./compiled/wordcount_reducer.so -CombinePath=./compiled/wordcount_combiner.so -data=./Data/input
        ```
2. 运行自定义 mapreduce 任务
- 实现 mr 包提供的 mapper、combiner 和 reducer 函数，将他们编译成 `.so` 文件
- 编译 master 和 worker
- 启动两个终端
    - 终端 1 启动 master 和worker
        ```
        go run main.go -mode=MapReduce
        ```
    - 终端 2 输入 map、reduce、combine 函数和文件路径
        ```
        go run main.go -mode=InputDir -MapPath=... -ReducePath=... -CombinePath=... -data=...
        ```