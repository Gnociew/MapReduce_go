package main

import (
	"fmt"
	"log"
	"net/rpc"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"
)

// StartMapReduce 启动MapReduce分布式计算框架
func StartMapReduce() {
	log.Println("开始启动MapReduce分布式计算框架...")

	// 创建信号处理，确保退出时能清理所有子进程
	setupSignalHandler()

	if err := compileMapReduce(); err != nil {
		log.Fatalf("编译Map和Reduce函数失败: %v", err)
	}

	if err := compileMasterWorker(); err != nil {
		log.Fatalf("编译Master和Worker程序失败: %v", err)
	}

	if err := prepareDirectories(); err != nil {
		log.Fatalf("准备目录失败: %v", err)
	}

	if err := runMapReduce(); err != nil {
		log.Fatalf("运行MapReduce失败: %v", err)
	}

	if err := mergeOutput(); err != nil {
		log.Fatalf("合并输出结果失败: %v", err)
	}

	log.Println("MapReduce任务已完成！结果保存在 Data/output/output.txt 文件中。")
}

// 编译Map和Reduce函数库
func compileMapReduce() error {
	log.Println("开始编译Map和Reduce函数库...")

	/* 使用 exec.Command 创建一个新的命令，.Run() 执行之前创建的编译命令。 */
	cmdMapper := exec.Command("go", "build", "-buildmode=plugin", "-o=./compiled/wordcount_mapper.so", "./udf/wordcount_mapper.go")
	if err := cmdMapper.Run(); err != nil {
		return fmt.Errorf("编译mapper失败: %v", err)
	}

	cmdReducer := exec.Command("go", "build", "-buildmode=plugin", "-o=./compiled/wordcount_reducer.so", "./udf/wordcount_reducer.go")
	if err := cmdReducer.Run(); err != nil {
		return fmt.Errorf("编译reducer失败: %v", err)
	}

	return nil
}

// 编译Master和Worker程序
func compileMasterWorker() error {
	log.Println("开始编译Master和Worker程序...")

	cmdMaster := exec.Command("go", "build", "-o", "./compiled/master", "./cmd/master/main.go")
	if err := cmdMaster.Run(); err != nil {
		return fmt.Errorf("编译master失败: %v", err)
	}

	cmdWorker := exec.Command("go", "build", "-o", "./compiled/worker", "./cmd/worker/main.go")
	if err := cmdWorker.Run(); err != nil {
		return fmt.Errorf("编译worker失败: %v", err)
	}

	return nil
}

// 准备输入和输出目录
func prepareDirectories() error {
	log.Println("准备输入和输出目录...")

	// 创建输入目录
	if err := os.MkdirAll("Data/input", 0755); err != nil {
		return fmt.Errorf("创建输入目录失败: %v", err)
	}

	// 创建输出目录
	if err := os.MkdirAll("Data/output", 0755); err != nil {
		return fmt.Errorf("创建输出目录失败: %v", err)
	}

	// 创建mr-out目录
	if err := os.MkdirAll("Data/mr-out", 0755); err != nil {
		return fmt.Errorf("创建mr-out目录失败: %v", err)
	}

	// 检查并创建示例输入文件
	// inputFile := filepath.Join("Data/input", "sample.txt")
	// if _, err := os.Stat(inputFile); os.IsNotExist(err) {
	// 	log.Println("创建示例输入文件...")
	// 	if err := os.WriteFile(inputFile, []byte(sampleContent), 0644); err != nil {
	// 		return fmt.Errorf("创建示例输入文件失败: %v", err)
	// 	}
	// }

	return nil
}

// 运行MapReduce进程
func runMapReduce() error {
	log.Println("启动Master服务...")

	// 启动Master进程
	/*
		Start 方法启动外部命令后立即返回（非阻塞），并返回一个 *Process 对象。通常用于需要后台执行命令的场景。
		Run 方法会等待命令执行完成并返回结果，适用于需要等待命令执行完成的场景。
	*/
	// 子进程会继承父进程的工作目录
	masterCmd := exec.Command("./compiled/master", "-port=:7777", "-input=Data/input", "-reduce=3")

	masterCmd.Stdout = os.Stdout
	masterCmd.Stderr = os.Stderr

	if err := masterCmd.Start(); err != nil {
		return fmt.Errorf("启动Master失败: %v", err)
	}

	// 注册Master进程到进程管理表，便于后续清理
	registerProcess(masterCmd.Process)

	// 确保Master服务启动完成
	time.Sleep(2 * time.Second)

	// 连接到Master服务的RPC端点
	client, err := rpc.DialHTTP("tcp", "localhost:7777")
	if err != nil {
		log.Fatalf("连接Master失败: %v", err)
	}

	// 调用HealthCheck方法
	var reply string
	err = client.Call("Master.HealthCheck", new(struct{}), &reply)
	if err != nil {
		log.Fatalf("调用健康检查失败: %v", err)
	}
	fmt.Println("健康检查结果:", reply)

	log.Println("启动Worker服务...")

	/*
		WaitGroup 类型用于同步多个 goroutine 的执行。
		当启动多个 goroutine 时，WaitGroup 可以用来等待这些 goroutine 完成它们的工作，确保在所有进程完成之前，主程序不会退出。
		goroutine 是 Go 的轻量级线程，用于实现并发执行。
		goroutine 的调度是由 Go 运行时管理的，而不是操作系统，因此切换和管理 goroutine 的成本较低
	*/
	var wg sync.WaitGroup

	// 启动 Worker 进程
	for i := 0; i < 3; i++ {
		wg.Add(1) // 增加WaitGroup计数器

		/*
			go 关键字用于启动一个新的 goroutine，
			在一个函数调用前加上 go，Go 运行时就会为这个函数创建一个新的并发执行的任务。
		*/
		go func(id int) {
			defer wg.Done() // 函数结束时减少 WaitGroup 计数器

			workerCmd := exec.Command("./compiled/worker",
				"-master=localhost:7777",
				"-map=./compiled/wordcount_mapper.so",
				"-reduce=./compiled/wordcount_reducer.so",
				fmt.Sprintf("-id=%s", strconv.Itoa(id)))

			workerCmd.Stdout = os.Stdout
			workerCmd.Stderr = os.Stderr

			// log.Printf("启动Worker %d...\n", id)
			if err := workerCmd.Start(); err != nil {
				log.Printf("启动Worker %d失败: %v\n", id, err)
				return
			}

			// 注册进程以便清理
			/* 此处的进程是 exec.Command 执行外部命令时，返回的操作系统进程，和 goroutine 不同 */
			registerProcess(workerCmd.Process)

			// 等待Worker完成
			/* Wait() 会阻塞当前的 goroutine，直到外部进程完成执行 */
			if err := workerCmd.Wait(); err != nil {
				log.Printf("Worker %d执行失败: %v\n", id, err)
				return
			}

			log.Printf("Worker %d执行完成\n", id)
		}(i + 1) // i+1 是Worker进程的 ID
	}

	log.Println("等待所有任务完成...")
	wg.Wait() // 阻塞主程序的执行，直到所有 Worker 完成并调用了 wg.Done()

	// 等待Master完成
	if err := masterCmd.Wait(); err != nil {
		return fmt.Errorf("master执行失败: %v", err)
	}

	return nil
}

// 合并输出结果
func mergeOutput() error {
	log.Println("合并Reduce输出...")

	// 获取所有mr-out文件
	matches, err := filepath.Glob("Data/mr-out/mr-out-*") // match：文件路径的切片
	if err != nil {
		return fmt.Errorf("查找输出文件失败: %v", err)
	}

	// 打开输出文件
	outFile, err := os.Create("Data/output/output.txt")
	if err != nil {
		return fmt.Errorf("创建输出文件失败: %v", err)
	}
	defer outFile.Close()

	// 合并所有中间输出
	for _, file := range matches {
		data, err := os.ReadFile(file)
		if err != nil {
			return fmt.Errorf("读取文件 %s 失败: %v", file, err)
		}

		if _, err := outFile.Write(data); err != nil {
			return fmt.Errorf("写入合并输出失败: %v", err)
		}
	}

	return nil
}

// 进程管理
var (
	processes      = make(map[int]*os.Process) // 进程ID到进程对象的映射
	processesMutex sync.Mutex                  // 互斥锁，用于保护进程映射的并发访问
)

// 注册进程以便清理
func registerProcess(p *os.Process) {
	processesMutex.Lock()
	defer processesMutex.Unlock()
	processes[p.Pid] = p
}

// 清理所有子进程
func cleanupProcesses() {
	processesMutex.Lock()
	defer processesMutex.Unlock()

	for pid, process := range processes {
		log.Printf("终止进程 %d...\n", pid)
		if err := process.Kill(); err != nil {
			log.Printf("终止进程 %d 失败: %v\n", pid, err)
		}
	}
}

// 设置信号处理器
func setupSignalHandler() {
	// 用于接收操作系统信号的通道
	c := make(chan os.Signal, 1)
	// 设置信号通知，将 os.Interrupt（通常是 Ctrl+C）和 SIGTERM（终止信号）发送到通道 c
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	// 启动一个 goroutine 来处理信号
	go func() {
		<-c // 阻塞直到接收到信号
		log.Println("收到终止信号，正在清理...")
		cleanupProcesses()
		os.Exit(1)
	}()
}

func main() {
	StartMapReduce()
}
