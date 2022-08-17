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
type DataNodeWriteStatus struct {
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

func (dataNode *Service) forwardForReplication(request *DataNodePutRequest, reply *DataNodeWriteStatus) error {
	blockId := request.BlockId
	blockAddresses := request.ReplicationNodes

	if len(blockAddresses) == 0 {
		return nil
	}

	startingDataNode := blockAddresses[0]
	remainingDataNodes := blockAddresses[1:]

	dataNodeInstance, rpcErr := rpc.Dial("tcp", startingDataNode.Host+":"+startingDataNode.ServicePort)
	util.Check(rpcErr)
	defer dataNodeInstance.Close()
	payloadRequest := DataNodePutRequest{
		RemoteFilePath:   request.RemoteFilePath,
		BlockId:          blockId,
		Data:             request.Data,
		ReplicationNodes: remainingDataNodes,
	}

	rpcErr = dataNodeInstance.Call("Service.PutData", payloadRequest, &reply)
	util.Check(rpcErr)
	return nil
}

func (dataNode *Service) PutData(request *DataNodePutRequest, reply *DataNodeWriteStatus) error {
	fileWriteHandler, err := os.Create(dataNode.DataDirectory + request.RemoteFilePath + request.BlockId)
	util.Check(err)
	defer fileWriteHandler.Close()

	fileWriter := bufio.NewWriter(fileWriteHandler)
	_, err = fileWriter.WriteString(request.Data)
	util.Check(err)
	fileWriter.Flush()
	*reply = DataNodeWriteStatus{Status: true}

	return dataNode.forwardForReplication(request, reply)
}

func (dataNode *Service) GetData(request *DataNodeGetRequest, reply *DataNodeData) error {
	dataBytes, err := ioutil.ReadFile(dataNode.DataDirectory + request.RemoteFilePath + request.BlockId)
	util.Check(err)

	*reply = DataNodeData{Data: string(dataBytes)}
	return nil
}
func (dataNode *Service) MakeDir(request string, reply *DataNodeWriteStatus) error {
	directory := dataNode.DataDirectory
	//判断当前目录是否存在，存在说明创建过了，直接返回nil
	if _, err2 := os.Stat(directory + request); err2 == nil {
		*reply = DataNodeWriteStatus{Status: true}
		return nil
	}
	//当前目录不存在，开始创建
	err := os.MkdirAll(directory+request, os.ModePerm)
	if err == nil {
		*reply = DataNodeWriteStatus{Status: true}
		fmt.Println("创建目录成功") //可以创建成功
		return nil
	}
	return errors.New("创建目录失败")
}

func (dataNode *Service) DeletePath(request string, reply *DataNodeWriteStatus) error {
	directory := dataNode.DataDirectory
	//判断当前目录是否存在，不存在说明已经删除了，直接返回nil
	_, err := os.Stat(directory + request)
	if os.IsNotExist(err) {
		*reply = DataNodeWriteStatus{Status: true}
		return nil
	}
	//当前目录存在，则删除
	err2 := os.RemoveAll(directory + request)
	if err2 == nil {
		*reply = DataNodeWriteStatus{Status: true}
		fmt.Println("删除目录成功") //可以删除成功
		return nil
	}
	return errors.New("删除目录失败")
}

func (dataNode *Service) DeleteFile(request string, reply *DataNodeWriteStatus) error {
	directory := dataNode.DataDirectory
	//判断当前文件是否存在，不存在说明已经删除了，直接返回nil
	_, err := os.Stat(directory + request)
	if os.IsNotExist(err) {
		*reply = DataNodeWriteStatus{Status: true}
		return nil
	}
	//当前文件存在，则删除
	err2 := os.Remove(directory + request)
	if err2 == nil {
		*reply = DataNodeWriteStatus{Status: true}
		fmt.Println("删除文件成功") //可以删除成功
		return nil
	}
	return errors.New("删除文件失败")
}

type DataNodeReNameRequest struct {
	ReNameSrcPath  string
	ReNameDestPath string
}

func (dataNode *Service) ReNameDir(request *DataNodeReNameRequest, reply *DataNodeWriteStatus) error {
	directory := dataNode.DataDirectory
	err := os.Rename(directory+request.ReNameSrcPath, directory+request.ReNameDestPath)
	if err == nil {
		*reply = DataNodeWriteStatus{Status: true}
		fmt.Println("重命名成功") //可以创建成功
		return nil
	}
	return errors.New("重命名失败")
}
