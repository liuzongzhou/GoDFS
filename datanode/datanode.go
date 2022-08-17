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
	err := os.MkdirAll(directory+request, os.ModePerm)
	if err == nil {
		*reply = DataNodeWriteStatus{Status: true}
		fmt.Println("创建成功") //可以创建成功
		return nil
	}
	return errors.New("创建失败")
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
		fmt.Println("创建成功") //可以创建成功
		return nil
	}
	return errors.New("创建失败")
}
