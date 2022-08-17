package datanode

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/liuzongzhou/GoDFS/util"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
)

type Service struct {
	DataDirectory string
	ServicePort   uint16
	NameNodeHost  string
	NameNodePort  uint16
}

type DataNodePutRequest struct {
	RemoteFilePath   string
	BlockId          string
	Data             string
	ReplicationNodes []util.DataNodeInstance
}

type DataNodeGetRequest struct {
	RemoteFilePath string
	BlockId        string
}
type DataNodeDeleteRequest struct {
	RemoteFilepath string
	BlockId        string
}
type DataNodeReplyStatus struct {
	Status bool
}

type DataNodeData struct {
	Data string
}

type NameNodePingRequest struct {
	Host string
	Port uint16
}

type NameNodePingResponse struct {
	Ack bool
}

func (dataNode *Service) Ping(request *NameNodePingRequest, reply *NameNodePingResponse) error {
	dataNode.NameNodeHost = request.Host
	dataNode.NameNodePort = request.Port
	log.Printf("Received ping from NameNode, recorded as {NameNodeHost: %s, NameNodePort: %d}\n", dataNode.NameNodeHost, dataNode.NameNodePort)

	*reply = NameNodePingResponse{Ack: true}
	return nil
}

func (dataNode *Service) Heartbeat(request bool, response *bool) error {
	if request {
		log.Println("Received heartbeat from NameNode")
		*response = true
		return nil
	}
	return errors.New("HeartBeatError")
}

//forwardForReplication 同步备份节点
func (dataNode *Service) forwardForReplication(request *DataNodePutRequest, reply *DataNodeReplyStatus) error {
	blockId := request.BlockId
	blockAddresses := request.ReplicationNodes
	//当blockAddresses长度为0时，说明所有备份节点已经备份完，直接结束
	if len(blockAddresses) == 0 {
		return nil
	}
	//与主datanode节点写入的方式保持一致
	startingDataNode := blockAddresses[0]
	remainingDataNodes := blockAddresses[1:]

	dataNodeInstance, rpcErr := rpc.Dial("tcp", startingDataNode.Host+":"+startingDataNode.ServicePort)
	//rpc调用出现问题，直接返回false
	if rpcErr != nil {
		log.Println(rpcErr)
		*reply = DataNodeReplyStatus{Status: false}
		return rpcErr
	}
	defer dataNodeInstance.Close()
	//装载写入请求
	payloadRequest := DataNodePutRequest{
		RemoteFilePath:   request.RemoteFilePath,
		BlockId:          blockId,
		Data:             request.Data,
		ReplicationNodes: remainingDataNodes,
	}

	rpcErr = dataNodeInstance.Call("Service.PutData", payloadRequest, &reply)
	//rpc调用出现问题，直接返回false
	if rpcErr != nil {
		log.Println(rpcErr)
		*reply = DataNodeReplyStatus{Status: false}
		return rpcErr
	}
	return nil
}

// PutData 将BlockSize大小的数据写入datanode节点对应的路径
//文件写入请求：路径，BlockId，实际数据，备份的节点信息
// reply：是否写入成功
func (dataNode *Service) PutData(request *DataNodePutRequest, reply *DataNodeReplyStatus) error {
	//创建对应节点路径下的文件：datanode节点根目录+相对路径+BlockId
	fileWriteHandler, err := os.Create(dataNode.DataDirectory + request.RemoteFilePath + request.BlockId)
	//创建失败，返回false
	if err != nil {
		log.Println(err)
		*reply = DataNodeReplyStatus{Status: false}
		return err
	}
	defer fileWriteHandler.Close()
	//缓冲流的方式写入
	fileWriter := bufio.NewWriter(fileWriteHandler)
	_, err = fileWriter.WriteString(request.Data)
	if err != nil {
		log.Println(err)
		*reply = DataNodeReplyStatus{Status: false}
		return err
	}
	fileWriter.Flush()
	*reply = DataNodeReplyStatus{Status: true}
	//同步备份节点
	return dataNode.forwardForReplication(request, reply)
}

//GetData 获取blockId对应的数据内容
//读取请求：FilePath+BlockId
func (dataNode *Service) GetData(request *DataNodeGetRequest, reply *DataNodeData) error {
	//这边是去datanode底层读取数据，所以光有相对路径不行，还得拼接上根目录DataDirectory
	dataBytes, err := ioutil.ReadFile(dataNode.DataDirectory + request.RemoteFilePath + request.BlockId)
	//读取失败，返回err，返回上层，尝试其他节点
	if err != nil {
		return err
	}
	*reply = DataNodeData{Data: string(dataBytes)}
	return nil
}

//MakeDir 创建datanode节点文件目录
func (dataNode *Service) MakeDir(request string, reply *DataNodeReplyStatus) error {
	//datanode节点的根目录
	directory := dataNode.DataDirectory
	//判断当前目录是否存在，存在说明创建过了，直接返回nil
	if _, err2 := os.Stat(directory + request); err2 == nil {
		*reply = DataNodeReplyStatus{Status: true}
		return nil
	}
	//当前目录不存在，开始创建
	err := os.MkdirAll(directory+request, os.ModePerm)
	if err == nil {
		*reply = DataNodeReplyStatus{Status: true}
		fmt.Println("创建目录成功") //可以创建成功
		return nil
	}
	return errors.New("创建目录失败")
}

func (dataNode *Service) DeletePath(request *DataNodeDeleteRequest, reply *DataNodeReplyStatus) error {
	directory := dataNode.DataDirectory
	//判断当前目录是否存在，不存在说明已经成功删除了，直接返回nil
	_, err := os.Stat(directory + request.RemoteFilepath)
	if os.IsNotExist(err) {
		*reply = DataNodeReplyStatus{Status: true}
		return nil
	}
	//当前目录存在，则删除
	err2 := os.RemoveAll(directory + request.RemoteFilepath)
	if err2 == nil {
		*reply = DataNodeReplyStatus{Status: true}
		fmt.Println("删除目录成功") //可以删除成功
		return nil
	}
	return errors.New("删除目录失败")
}

func (dataNode *Service) DeleteFile(request *DataNodeDeleteRequest, reply *DataNodeReplyStatus) error {
	directory := dataNode.DataDirectory
	//判断当前文件是否存在，不存在说明已经删除了，直接返回nil
	_, err := os.Stat(directory + request.RemoteFilepath + request.BlockId)
	if os.IsNotExist(err) {
		*reply = DataNodeReplyStatus{Status: true}
		return nil
	}
	//当前文件存在，则删除
	err2 := os.Remove(directory + request.RemoteFilepath + request.BlockId)
	if err2 == nil {
		*reply = DataNodeReplyStatus{Status: true}
		fmt.Println("删除文件成功") //可以删除成功
		return nil
	}
	return errors.New("删除文件失败")
}

type DataNodeReNameRequest struct {
	ReNameSrcPath  string
	ReNameDestPath string
}

func (dataNode *Service) ReNameDir(request *DataNodeReNameRequest, reply *DataNodeReplyStatus) error {
	directory := dataNode.DataDirectory
	err := os.Rename(directory+request.ReNameSrcPath, directory+request.ReNameDestPath)
	if err == nil {
		*reply = DataNodeReplyStatus{Status: true}
		fmt.Println("重命名成功") //可以创建成功
		return nil
	}
	return errors.New("重命名失败")
}
