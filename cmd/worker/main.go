package main

import (
	"flag"
	"mr/mr"
)

func main() {
	// 解析命令行参数
	var masterAddr string
	var ID string

	flag.StringVar(&masterAddr, "master", "localhost:7777", "Master服务地址")
	flag.StringVar(&ID, "id", "0", "Worker ID")
	flag.Parse()

	// 创建Worker
	w := mr.NewWorker(masterAddr, ID)

	// 启动Worker
	w.Start()
}
