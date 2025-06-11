/*启动并管理任务的调度，接收来自 Worker（TaskTracker）的请求，执行 MapReduce 任务。*/

package main

import (
	"flag"
	"fmt"
	"mr/mr"
	"time"
)

func main() {
	// log.Println("111")

	// 解析命令行参数
	var port string

	flag.StringVar(&port, "port", ":7777", "Master服务监听端口")
	flag.Parse() // 解析命令行参数，覆盖默认值

	// 创建 & 启动 Master
	m := mr.NewMaster()
	// log.Println("222")
	m.StartServer(port)
	// log.Println("333")

	// 循环等待所有任务完成
	for !m.Done() {
		time.Sleep(time.Second) // 暂停当前线程以避免频繁检查，降低 CPU 的使用率
	}

	fmt.Println("所有任务已完成，Master退出")
}
