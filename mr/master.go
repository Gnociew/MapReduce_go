package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// 任务状态
const (
	TaskStatusIdle     = 0 // 空闲
	TaskStatusRunning  = 1 // 运行中
	TaskStatusComplete = 2 // 完成
	TaskStatusFailed   = 3 // 失败
)

// 任务类型
const (
	TaskTypeMap    = 1
	TaskTypeReduce = 2
	TaskTypeWait   = 3 // 如果没有可分配的任务，返回给 Worker 等待
	TaskTypeExit   = 4 // 所有任务完成，返回一个 Exit 任务给 Worker
)

// 任务定义
type Task struct {
	ID        int
	Type      int
	FucPath   string // Map或Reduce函数的共享库路径
	Status    int
	InputFile string // Map任务的输入文件
	MapID     int
	ReduceID  int
	NReduce   int
	NMap      int
	MapFiles  []string // Reduce任务的输入文件（Map输出文件的列表）
}

// Master结构体
// 负责管理Map和Reduce任务的状态
type Master struct {
	mu sync.Mutex // 互斥锁，保护共享资源

	mapPath    string
	reducePath string
	inputFiles []string

	nMap        int // Map任务的数量
	nReduce     int // Reduce任务的数量
	mapTasks    []Task
	reduceTasks []Task

	mapDone    bool
	reduceDone bool

	workerCount int                  // 当前活跃的 Worker 数量
	heartbeats  map[string]time.Time // 记录每个 Worker 的心跳时间，用于检测 Worker 是否超时

	IsHealthy     bool // 健康状态，默认设置为健康
	IsInitialized bool // 是否已初始化
	InitFlag      bool
}

// 请求获取任务的参数
/* 当 Worker 向 Master 请求任务时，会传递该结构体，告诉 Master 请求的是哪个 Worker 的任务 */
type AskTaskArgs struct {
	WorkerID string
}

// 请求获取任务的响应
/* 当 Worker 请求任务后，Master 返回的响应中包含分配给 Worker 的任务信息 */
type AskTaskReply struct {
	Task Task
}

// 完成任务的参数
type FinishTaskArgs struct {
	WorkerID string
	Task     Task
}

// 完成任务的响应
type FinishTaskReply struct {
	Success bool
}

// 心跳参数
/* Worker 向 Master 发送心跳请求，表明它仍在工作并且没有超时或崩溃 */
type HeartbeatArgs struct {
	WorkerID string
}

// 心跳响应
type HeartbeatReply struct {
	Success bool
}

// 创建一个新的Master
func NewMaster(nReduce int) *Master {
	m := &Master{
		nMap:        0,
		nReduce:     nReduce,
		mapPath:     "",
		reducePath:  "",
		mapDone:     false,
		reduceDone:  false,
		workerCount: 0,
		heartbeats:  make(map[string]time.Time),
		IsHealthy:   true,
	}

	// 启动一个 goroutine 来检查任务是否超时
	go m.checkTaskTimeout()

	return m
}

// 设置map, reduce函数和输入文件路径
func (m *Master) SetPaths(args PathsArgs, reply *string) error {
	log.Println("设置Map和Reduce函数路径以及输入目录...")

	m.mu.Lock()
	defer m.mu.Unlock()

	// 检查 Map 和 Reduce 函数共享库文件是否存在
	if _, err := os.Stat(args.MapPath); os.IsNotExist(err) {
		fmt.Printf("Map函数共享库文件 %s 不存在", args.MapPath)
	} else {
		m.mapPath = args.MapPath
	}

	if _, err := os.Stat(args.ReducePath); os.IsNotExist(err) {
		fmt.Printf("Reduce函数共享库文件 %s 不存在", args.ReducePath)
	} else {
		m.reducePath = args.ReducePath
	}

	// 检查输入目录是否存在
	if _, err := os.Stat(args.InputDir); os.IsNotExist(err) {
		fmt.Printf("输入目录 %s 不存在\n", args.InputDir)
	}

	// 获取输入目录中的所有文件
	var inputFiles []string
	/* filepath.Walk() 用于递归遍历输入目录中的所有文件 */
	err := filepath.Walk(args.InputDir, func(path string, info os.FileInfo, err error) error {
		/* 这里的 err 是 filepath.Walk 在遍历文件时遇到错误时传递给回调函数的 */
		if err != nil {
			return err
		}
		// 排除目录和隐藏文件
		if !info.IsDir() && !strings.HasPrefix(info.Name(), ".") {
			inputFiles = append(inputFiles, path)
		}
		return nil
	})
	if err != nil {
		fmt.Printf("读取输入目录失败: %v\n", err)
		os.Exit(1)
	}

	if len(inputFiles) == 0 {
		fmt.Printf("输入目录 %s 中没有文件\n", args.InputDir)
		os.Exit(1)
	}

	// 设置输入路径
	m.inputFiles = inputFiles
	m.nMap = len(inputFiles)

	*reply = "路径设置成功"

	// 初始化任务
	m.initTasks()

	return nil
}

// 初始化 Map 和 Reduce 任务
func (m *Master) initTasks() {

	/* 这里不能再写一个锁，因为调用它的函数已经写了，该函数执行完锁才会在上层函数释放，这里写了会有死锁问题 */

	// 初始化Map任务
	log.Println("初始化Map任务...")
	m.mapTasks = make([]Task, m.nMap) // 切片类型为Task，长度为nMap
	for i := 0; i < m.nMap; i++ {
		m.mapTasks[i] = Task{
			ID:        i,
			Type:      TaskTypeMap,
			FucPath:   m.mapPath,
			Status:    TaskStatusIdle, // 空闲状态
			InputFile: m.inputFiles[i],
			MapID:     i,
			NReduce:   m.nReduce,
			NMap:      m.nMap,
		}
	}

	// 初始化Reduce任务
	log.Println("初始化Reduce任务...")
	m.reduceTasks = make([]Task, m.nReduce)
	for i := 0; i < m.nReduce; i++ {
		mapFiles := make([]string, m.nMap)
		for j := 0; j < m.nMap; j++ {
			mapFiles[j] = fmt.Sprintf("mr-%d-%d", j, i) // Map 任务的输出文件列表
		}

		m.reduceTasks[i] = Task{
			ID:       i + m.nMap,
			Type:     TaskTypeReduce,
			FucPath:  m.reducePath,
			Status:   TaskStatusIdle,
			ReduceID: i,
			MapFiles: mapFiles,
			NReduce:  m.nReduce,
			NMap:     m.nMap,
		}
	}

	m.IsInitialized = true
	log.Printf("Map任务数量: %d, Reduce任务数量: %d\n", m.nMap, m.nReduce)
}

// 向Worker分配任务
/* Worker通过RPC调用Master的AskTask方法来请求任务，Master会根据当前任务的状态分配Map或Reduce任务 */
func (m *Master) AskTask(args *AskTaskArgs, reply *AskTaskReply) error {
	// log.Printf("Worker %s 请求任务\n", args.WorkerID)

	// 如果没有可分配的任务，返回Wait任务
	if !m.IsInitialized {
		if !m.InitFlag {
			log.Printf("Worker %s 请求任务，但请先设置Map和Reduce函数路径和输入目录\n", args.WorkerID)
			// log.Println("暂无可分配任务，请先设置Map和Reduce函数路径和输入目录")
			m.InitFlag = true
		}
		reply.Task = Task{Type: TaskTypeWait}
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// 更新 Worker 心跳
	m.heartbeats[args.WorkerID] = time.Now()

	// 如果所有任务都完成了，返回Exit任务
	if m.reduceDone {
		reply.Task = Task{Type: TaskTypeExit}
		return nil
	}

	// 检查Map任务是否都完成了
	allMapDone := true
	for _, task := range m.mapTasks {
		if task.Status != TaskStatusComplete {
			allMapDone = false
			break
		}
	}

	// 如果所有Map任务都完成了，设置mapDone为true
	if allMapDone && !m.mapDone {
		m.mapDone = true
	}

	// 分配Map任务
	if !m.mapDone {
		for i := range m.mapTasks {
			if m.mapTasks[i].Status == TaskStatusIdle {
				m.mapTasks[i].Status = TaskStatusRunning
				reply.Task = m.mapTasks[i]
				return nil
			}
		}
	}

	// 如果Map任务都完成了，分配Reduce任务
	if m.mapDone {
		// 检查Reduce任务是否都完成了
		allReduceDone := true
		for _, task := range m.reduceTasks {
			if task.Status != TaskStatusComplete {
				allReduceDone = false
				break
			}
		}

		// 如果所有Reduce任务都完成了，设置reduceDone为true
		if allReduceDone && !m.reduceDone {
			m.reduceDone = true
			// 所有任务都完成了，返回Exit任务
			reply.Task = Task{Type: TaskTypeExit}
			return nil
		}

		// 分配Reduce任务
		for i := range m.reduceTasks {
			if m.reduceTasks[i].Status == TaskStatusIdle {
				m.reduceTasks[i].Status = TaskStatusRunning
				reply.Task = m.reduceTasks[i]
				return nil
			}
		}
	}

	return nil
}

// 接收Worker完成任务的通知
func (m *Master) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 更新心跳
	m.heartbeats[args.WorkerID] = time.Now()

	task := args.Task
	reply.Success = true

	if task.Type == TaskTypeMap {
		if m.mapTasks[task.MapID].Status == TaskStatusRunning {
			m.mapTasks[task.MapID].Status = TaskStatusComplete
		}
	} else if task.Type == TaskTypeReduce {
		if m.reduceTasks[task.ReduceID].Status == TaskStatusRunning {
			m.reduceTasks[task.ReduceID].Status = TaskStatusComplete
		}
	}

	return nil
}

// 接受并处理 Worker 的心跳
func (m *Master) Heartbeat(args *HeartbeatArgs, reply *HeartbeatReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.heartbeats[args.WorkerID] = time.Now()
	reply.Success = true

	return nil
}

// 检查任务是否超时
func (m *Master) checkTaskTimeout() {
	for {
		time.Sleep(10 * time.Second) // 每 10 秒检查一次

		m.mu.Lock()

		// 所有任务都完成了，退出检查
		if m.reduceDone {
			m.mu.Unlock()
			return
		}

		// 检查 Map 任务超时
		for i := range m.mapTasks {
			if m.mapTasks[i].Status == TaskStatusRunning {
				// 检查此任务对应的Worker是否超时
				// 由于实现简单，此处没有记录哪个 Worker 在执行哪个任务，在实际实现中应该记录任务分配情况
				// 这里简单处理：如果有任务在运行状态超过一定时间，认为超时
				m.mapTasks[i].Status = TaskStatusIdle
			}
		}

		// 检查 Reduce 任务超时
		if m.mapDone {
			for i := range m.reduceTasks {
				if m.reduceTasks[i].Status == TaskStatusRunning {
					m.reduceTasks[i].Status = TaskStatusIdle
				}
			}
		}

		m.mu.Unlock()
	}
}

// 启动Master的RPC服务
func (m *Master) StartServer(port string) {

	// 将 Master 的方法注册为 RPC 服务供给 Worker 调用
	err := rpc.Register(m)
	if err != nil {
		log.Fatal("注册Master失败:", err)
	} else {
		log.Println("RPC服务注册成功")
	}

	// 设置 HTTP 处理程序以处理通过 HTTP 协议接收到的 RPC 请求
	rpc.HandleHTTP()

	l, e := net.Listen("tcp", port)
	if e != nil {
		log.Fatal("listen error:", e)
	}

	// 启动 HTTP 服务器，监听来自 Worker 的请求
	go http.Serve(l, nil)

	fmt.Printf("Master is running on port %s\n", port)

	// 所有任务完成后，退出 Master
	go func() {
		for {
			time.Sleep(10 * time.Second)

			if m.Done() {
				log.Println("所有任务完成，Master 退出")
				l.Close()
				os.Exit(0)
			}
		}
	}()

	select {}
}

// 检查所有任务是否完成
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.reduceDone
}

func (m *Master) HealthCheck(args *struct{}, reply *string) error {
	// 返回服务健康状态
	if m.IsHealthy {
		*reply = "Master 服务运行正常"
	} else {
		*reply = "Master 服务不可用"
	}
	return nil
}
