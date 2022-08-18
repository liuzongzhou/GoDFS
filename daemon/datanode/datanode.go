package datanode

import (
	"errors"
	"github.com/liuzongzhou/GoDFS/datanode"
	"log"
	"net"
	"net/rpc"
	"strconv"
)

// InitializeDataNodeUtil 初始化dataNode节点进程
func InitializeDataNodeUtil(serverPort int, dataLocation string) {
	// 生成dataNode实例
	dataNodeInstance := new(datanode.Service)
	// 记录元数据信息：根目录，端口号
	dataNodeInstance.DataDirectory = dataLocation
	dataNodeInstance.ServicePort = uint16(serverPort)

	log.Printf("Data storage location is %s\n", dataLocation)
	// 向注册中心注册实例
	err := rpc.Register(dataNodeInstance)
	if err != nil {
		return
	}

	rpc.HandleHTTP()

	var listener net.Listener
	initErr := errors.New("init")
	//选择空闲的端口号作为连接
	for initErr != nil {
		listener, initErr = net.Listen("tcp", ":"+strconv.Itoa(serverPort))
		serverPort += 1
	}
	log.Printf("DataNode port is %d\n", serverPort-1)
	//变更实例对应的端口号
	dataNodeInstance.ServicePort = uint16(serverPort - 1)
	defer listener.Close()
	//返回正确的端口连接，方便正确启动nameNode节点管理
	log.Printf("DataNode daemon started on port: " + strconv.Itoa(serverPort-1))
	//采纳这个连接
	rpc.Accept(listener)
}
