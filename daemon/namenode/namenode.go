package namenode

import (
	"errors"
	"github.com/liuzongzhou/GoDFS/datanode"
	"github.com/liuzongzhou/GoDFS/namenode"
	"log"
	"net"
	"net/rpc"
	"strconv"
	"time"
)

// removeElementFromSlice 在切片中移除无用信息值,返回切片
func removeElementFromSlice(elements []string, index int) []string {
	return append(elements[:index], elements[index+1:]...)
}

// discoverDataNodes 发现当前的dataNodes，并维护到listOfDataNodes
// 两种方式：
//1.当终端没有输入dataNodes List,则从7000-7050遍历尝试连接发现，并加入listOfDataNodes
//2.当终端输入dataNodes List，直接采用终端提供的dataNodes
func discoverDataNodes(nameNodeInstance *namenode.Service, listOfDataNodes *[]string) error {
	nameNodeInstance.IdToDataNodes = make(map[uint64]datanode.DataNodeInstance)

	var i int
	availableNumberOfDataNodes := len(*listOfDataNodes)
	if availableNumberOfDataNodes == 0 {
		log.Printf("No DataNodes specified, discovering ...\n")

		host := "localhost"
		serverPort := 7000

		pingRequest := datanode.NameNodePingRequest{Host: host, Port: nameNodeInstance.Port}
		var pingResponse datanode.NameNodePingResponse
		//从7000-7050遍历尝试连接发现，并加入listOfDataNodes
		for serverPort < 7050 {
			dataNodeUri := host + ":" + strconv.Itoa(serverPort)
			dataNodeInstance, initErr := rpc.Dial("tcp", dataNodeUri)
			if initErr == nil {
				*listOfDataNodes = append(*listOfDataNodes, dataNodeUri)
				log.Printf("Discovered DataNode %s\n", dataNodeUri)

				pingErr := dataNodeInstance.Call("Service.Ping", pingRequest, &pingResponse)
				if pingErr != nil {
					log.Println(pingErr)
				}
				if pingResponse.Ack {
					log.Printf("Ack received from %s\n", dataNodeUri)
				} else {
					log.Printf("No ack received from %s\n", dataNodeUri)
				}
			}
			serverPort += 1
		}
	}

	availableNumberOfDataNodes = len(*listOfDataNodes)
	// 维护nameNode中IdToDataNodes的元数据信息
	for i = 0; i < availableNumberOfDataNodes; i++ {
		host, port, err := net.SplitHostPort((*listOfDataNodes)[i])
		if err != nil {
			log.Println(err)
			return err
		}
		dataNodeInstance := datanode.DataNodeInstance{Host: host, ServicePort: port}
		nameNodeInstance.IdToDataNodes[uint64(i)] = dataNodeInstance
	}

	return nil
}

// InitializeNameNodeUtil 初始化nameNode节点进程
func InitializeNameNodeUtil(primaryPort string, serverHost string, serverPort int, blockSize int, replicationFactor int, listOfDataNodes []string) {
	// 生成nameNode实例
	nameNodeInstance := namenode.NewService(primaryPort, serverHost, uint64(blockSize), uint64(replicationFactor), uint16(serverPort))
	// 发现当前存在的dataNodes或者终端给的，并维护到listOfDataNodes
	err := discoverDataNodes(nameNodeInstance, &listOfDataNodes)
	if err != nil {
		log.Println(err)
		return
	}

	// 协程：对管理的dataNodes进行心跳检测
	go heartbeatToDataNodes(listOfDataNodes, nameNodeInstance)
	// 向注册中心注册实例
	err = rpc.Register(nameNodeInstance)
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
	//变更实例对应的端口号
	nameNodeInstance.Port = uint16(serverPort - 1)
	defer listener.Close()

	//当端口号不是主节点端口号时，说明是备份nameNode节点，开启协程进行元数据间的备份
	atoi, _ := strconv.Atoi(nameNodeInstance.PrimaryPort)
	if nameNodeInstance.Port != uint16(atoi) {
		go ReplicationnameNode(nameNodeInstance)
	}

	log.Printf("BlockSize is %d\n", blockSize)
	log.Printf("Replication Factor is %d\n", replicationFactor)
	log.Printf("List of DataNode(s) in service is %q\n", listOfDataNodes)
	log.Printf("NameNode port is %d\n", serverPort-1)
	log.Printf("NameNode daemon started on port: " + strconv.Itoa(serverPort-1))
	//采纳这个连接
	rpc.Accept(listener)

}

// ReplicationnameNode 同步主nameNode节点的元数据信息：FileNameToBlocks，IdToDataNodes，BlockToDataNodeIds
// FileNameSize,DirectoryToFileName
func ReplicationnameNode(nameNode *namenode.Service) {
	//开启定时任务，每隔1s进行一次元数据同步
	for range time.Tick(time.Second * 1) {
		//与主节点建立rpc连接，当连接不上时说明主节点挂了，不需要再同步，直接使用备份节点进行操作，并将备份节点升级成为主节点
		nameNodeInstance, err := rpc.Dial("tcp", "localhost:"+nameNode.PrimaryPort)
		if err != nil {
			log.Println("primary nameNode is dead,second nameNode become primary nameNode")
			nameNode.PrimaryPort = strconv.FormatUint(uint64(nameNode.Port), 10)
			log.Printf("now primary nameNode port is %s\n", nameNode.PrimaryPort)
			return
		}
		request := true
		var reply namenode.Service
		//通过rpc调用同步nameNode元数据方法,返回元数据
		err = nameNodeInstance.Call("Service.ReplicationnameNode", request, &reply)
		if err != nil {
			log.Println(err)
			continue
		}
		//更新备份nameNode的元数据信息
		nameNode.FileNameToBlocks = reply.FileNameToBlocks
		nameNode.IdToDataNodes = reply.IdToDataNodes
		nameNode.BlockToDataNodeIds = reply.BlockToDataNodeIds
		nameNode.FileNameSize = reply.FileNameSize
		nameNode.DirectoryToFileName = reply.DirectoryToFileName
	}
}

// heartbeatToDataNodes nameNode与dataNodes实现心跳检测
func heartbeatToDataNodes(listOfDataNodes []string, nameNode *namenode.Service) {
	// 设置一个5秒的定时任务
	for range time.Tick(time.Second * 5) {
		// 遍历所有的dataNodes节点
		for i, hostPort := range listOfDataNodes {
			//1.与dataNode建立连接，检测连接是否正常
			nameNodeClient, connectionErr := rpc.Dial("tcp", hostPort)

			if connectionErr != nil {
				log.Printf("Unable to connect to node %s\n", hostPort)
				var reply bool
				// 断连了，需要重新分配数据，将断连节点上的数据进行备份
				reDistributeError := nameNode.ReDistributeData(&namenode.ReDistributeDataRequest{DataNodeUri: hostPort}, &reply)
				if reDistributeError != nil {
					log.Println(reDistributeError)
				}
				//维护IdToDataNodes元数据信息，删除对应的kye：Id
				delete(nameNode.IdToDataNodes, uint64(i))
				//listOfDataNodes 更新数据，移除死亡的节点
				listOfDataNodes = removeElementFromSlice(listOfDataNodes, i)
				continue
			}
			//2.调用datanode Heartbeat方法,返检测调用方法是否正常
			var response bool
			hbErr := nameNodeClient.Call("Service.Heartbeat", true, &response)
			if hbErr != nil || !response {
				//如果调用rpc方法失败了 说明dataNode节点信息也出现问题
				log.Printf("No heartbeat received from %s\n", hostPort)
				var reply bool
				// 断连了，需要重新分配数据，将断连节点上的数据进行备份
				reDistributeError := nameNode.ReDistributeData(&namenode.ReDistributeDataRequest{DataNodeUri: hostPort}, &reply)
				if reDistributeError != nil {
					log.Println(reDistributeError)
				}
				//维护IdToDataNodes元数据信息，删除对应的kye：Id
				delete(nameNode.IdToDataNodes, uint64(i))
				//listOfDataNodes 更新数据，移除死亡的节点
				listOfDataNodes = removeElementFromSlice(listOfDataNodes, i)
			}
		}
	}
}
