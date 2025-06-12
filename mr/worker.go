package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"plugin"
	"sort"
)

// temp 文件路径
const tempFilePath = "Data/mr-temp"

// Worker 结构定义
type Worker struct {
	ID          string
	MapFunc     Map
	CombineFunc Combine
	ReduceFunc  Reduce
	masterAddr  string
}

// 中间结果的键值对
type KeyValue struct {
	Key   string
	Value interface{}
}

// 根据 Key 对 KeyValue 进行排序
/* 在 Go 中，一个类型实现了某个接口，只要它实现了接口中定义的所有方法，不需要显式声明它实现了这个接口。 */
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key } // 升序排列，a[i].Key < a[j].Key 时返回 true

// 使用fnv哈希函数计算字符串的哈希值
/* 将 Map 阶段的中间结果分配到不同的 Reduce 任务中 */
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// 创建一个新的Worker
func NewWorker(masterAddr string, ID string) *Worker {

	return &Worker{
		ID:         ID,
		MapFunc:    nil,
		ReduceFunc: nil,
		masterAddr: masterAddr,
	}
}

// 启动Worker服务
func (w *Worker) Start() {
	for {
		// 请求任务
		task := w.askForTask()

		switch task.Type {
		case TaskTypeMap:
			log.Printf("Worker %s 收到Map任务: %d", w.ID, task.MapID)
			w.doMap(task)
		case TaskTypeReduce:
			log.Printf("Worker %s 收到Reduce任务: %d", w.ID, task.ReduceID)
			w.doReduce(task)
		case TaskTypeExit:
			fmt.Println("所有任务已完成，Worker退出")
			return
		}

		// 发送心跳
		w.sendHeartbeat()
	}
}

// 请求任务
func (w *Worker) askForTask() Task {
	args := AskTaskArgs{
		WorkerID: w.ID,
	}
	reply := AskTaskReply{}

	/* call 是一个封装好的函数，用于发送和接收 RPC 请求 */
	ok := call(w.masterAddr, "Master.AskTask", &args, &reply)
	if !ok {
		log.Fatalf("无法请求任务")
	}

	return reply.Task
}

// 执行Map任务
func (w *Worker) doMap(task Task) {
	fmt.Printf("Worker %s 开始执行Map任务 %d\n", w.ID, task.MapID)
	w.sendHeartbeat()

	// 加载Map函数
	mapPlugin, err := plugin.Open(task.FucPath)
	if err != nil {
		log.Fatalf("无法加载Map插件: %v", err)
	}
	mapSymbol, err := mapPlugin.Lookup("Map")
	if err != nil {
		log.Fatalf("无法查找Map函数: %v", err)
	}
	mapFunc := *(mapSymbol.(*Map))
	w.MapFunc = mapFunc

	lines := make([]string, 0)
	inputFile, err := os.Open(task.InputFile)
	if err != nil {
		panic(err)
	}
	defer inputFile.Close()
	scanner := bufio.NewScanner(inputFile)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		panic(err)
	}

	pairs := make([]*Pair, 0) // mr.Pair 类型的切片，用于存储中间结果
	for _, line := range lines {
		// 对每一行调用 mapFunc 函数，返回值是一个 mr.Pair 类型的切片（可能包含多个键值对）
		// ...（展开运算符），将函数返回的切片解包，将返回切片中的每一个元素添加到 pairs 切片中
		pairs = append(pairs, w.MapFunc(line)...)
	}

	// 将中间结果按Reduce任务分组
	intermediate := make([][]KeyValue, task.NReduce) // 二维切片，用于存储每个 Reduce 任务的中间结果
	for i := 0; i < task.NReduce; i++ {
		intermediate[i] = make([]KeyValue, 0)
	}

	// 将结果分配到不同的Reduce任务
	for _, pair := range pairs {
		key := fmt.Sprintf("%v", pair.Key) // 将any类型转换为字符串用于哈希
		reduceTaskID := ihash(key) % task.NReduce

		intermediate[reduceTaskID] = append(intermediate[reduceTaskID], KeyValue{
			Key:   key,
			Value: pair.Value,
		})
	}

	// 如果有 Combine 函数，则对中间结果进行合并
	if task.CombinePath != "" {
		w.sendHeartbeat()

		// 加载Combine函数
		combinePlugin, err := plugin.Open(task.CombinePath)
		if err != nil {
			log.Fatalf("无法加载Combine插件: %v", err)
		}
		combineSymbol, err := combinePlugin.Lookup("Combine")
		if err != nil {
			log.Fatalf("无法查找Combine函数: %v", err)
		}
		combineFunc := *(combineSymbol.(*Combine))
		w.CombineFunc = combineFunc
		fmt.Printf("Worker %s 使用Combine函数进行中间结果合并\n", w.ID)

		// 对每个 Reduce 任务的中间结果进行合并
		for i := 0; i < task.NReduce; i++ {
			// 将 []KeyValue 转换为 []*Pair
			pairs := make([]*Pair, 0, len(intermediate[i]))
			for _, kv := range intermediate[i] {
				pairs = append(pairs, &Pair{
					Key:   kv.Key,
					Value: kv.Value,
				})
			}
			// 使用 Combine 函数对中间结果进行合并
			combined := w.CombineFunc(pairs)
			// 将合并后的结果重新赋值给 intermediate[i]
			intermediate[i] = make([]KeyValue, 0, len(combined)) // 重新分配切片空间
			for _, kv := range combined {
				intermediate[i] = append(intermediate[i], KeyValue{
					Key:   fmt.Sprintf("%v", kv.Key), // 将any类型转换为字符串
					Value: kv.Value,
				})
			}
		}
	}

	// currentDir, err := os.Getwd()
	// if err != nil {
	// 	log.Fatalf("无法获取当前工作目录: %v", err)
	// }
	// // fmt.Println("当前工作目录:", currentDir)

	// 将中间结果写入临时文件
	for i := 0; i < task.NReduce; i++ {
		// 创建临时文件
		tempFile, err := os.CreateTemp(tempFilePath, "mr-map-*")
		if err != nil {
			log.Fatalf("无法创建临时文件: %v", err)
		}
		defer tempFile.Close()

		// 将键值对序列化为JSON
		enc := json.NewEncoder(tempFile) // 创建JSON编码器，tempFile 是目标文件对象
		for _, kv := range intermediate[i] {
			err := enc.Encode(&kv) // json.Encode() 需要一个指针，因为它会直接修改目标地址中的数据
			if err != nil {
				log.Fatalf("无法写入临时文件: %v", err)
			}
		}

		// 重命名临时文件为最终文件
		finalName := fmt.Sprintf("mr-%d-%d", task.MapID, i)
		finalPath := filepath.Join(tempFilePath, finalName)
		os.Rename(tempFile.Name(), finalPath)
	}

	// 通知Master任务已完成
	w.finishTask(task)

	fmt.Printf("Worker %s 完成Map任务 %d\n", w.ID, task.MapID)
}

// 执行Reduce任务
func (w *Worker) doReduce(task Task) {
	fmt.Printf("Worker %s 开始执行Reduce任务 %d\n", w.ID, task.ReduceID)
	w.sendHeartbeat()

	// 加载Reduce函数
	reducePlugin, err := plugin.Open(task.FucPath)
	if err != nil {
		log.Fatalf("无法加载Reduce插件: %v", err)
	}
	reduceSymbol, err := reducePlugin.Lookup("Reduce")
	if err != nil {
		log.Fatalf("无法查找Reduce函数: %v", err)
	}
	reduceFunc := *(reduceSymbol.(*Reduce))
	w.ReduceFunc = reduceFunc

	fmt.Printf("启动 Worker %s\n", w.ID)

	// 读取所有中间文件
	intermediate := make([]KeyValue, 0)
	for _, filename := range task.MapFiles {
		// fmt.Printf("Reduce任务 %d 读取中间文件 %s\n", task.ReduceID, filename)

		finalPath := filepath.Join(tempFilePath, filename)
		file, err := os.Open(finalPath)
		if err != nil {
			log.Fatalf("无法打开中间文件 %v: %v", filename, err)
		}

		// 从文件中解码键值对
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	// 按Key排序
	sort.Sort(ByKey(intermediate))

	// 创建输出目录（如果不存在）
	outDir := "Data/mr-out"
	if _, err := os.Stat(outDir); os.IsNotExist(err) {
		os.Mkdir(outDir, 0755)
	}

	// 创建输出文件
	outFile, err := os.Create(filepath.Join(outDir, fmt.Sprintf("mr-out-%d", task.ReduceID)))
	if err != nil {
		log.Fatalf("无法创建输出文件: %v", err)
	}
	defer outFile.Close()

	// 对每个不同的Key调用Reduce函数
	i := 0
	for i < len(intermediate) {
		// 收集相同Key的所有Value
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := make([]any, 0)
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		// 调用用户定义的Reduce函数
		key := intermediate[i].Key
		result := w.ReduceFunc(key, values)

		// 将结果写入输出文件
		fmt.Fprintf(outFile, "%v\t%v\n", result.Key, result.Value)

		i = j
	}

	// 通知Master任务已完成
	w.finishTask(task)

	fmt.Printf("Worker %s 完成Reduce任务 %d\n", w.ID, task.ReduceID)
}

// 通知Master任务已完成
func (w *Worker) finishTask(task Task) {
	args := FinishTaskArgs{
		WorkerID: w.ID,
		Task:     task,
	}
	reply := FinishTaskReply{}

	ok := call(w.masterAddr, "Master.FinishTask", &args, &reply)
	if !ok {
		log.Fatalf("无法通知任务完成")
	}
	if !reply.Success {
		log.Fatalf("任务完成失败: %v", task)
	}
}

// 发送心跳信号
func (w *Worker) sendHeartbeat() {
	args := HeartbeatArgs{
		WorkerID: w.ID,
	}
	reply := HeartbeatReply{}

	call(w.masterAddr, "Master.Heartbeat", &args, &reply)
}

// 封装RPC调用
/* 在 Go 的 RPC 调用中，传递给远程函数的参数通常是一个结构体，而不是多个单独的参数 */
func call(address string, rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", address) // 通过 HTTP 协议连接到远程 RPC 服务
	if err != nil {
		log.Printf("RPC连接错误: %v", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err != nil {
		log.Printf("RPC调用错误: %v", err)
		return false
	}

	return true
}
