package client

import (
	"github.com/liuzongzhou/GoDFS/datanode"
	"github.com/liuzongzhou/GoDFS/namenode"
	"github.com/liuzongzhou/GoDFS/util"
	"net/rpc"
	"os"
)

// Put 上传文件
func Put(nameNodeInstance *rpc.Client, sourcePath string, fileName string) (putStatus bool) {
	//完整的文件路径
	fullFilePath := sourcePath + fileName
	fileSizeHandler, err := os.Stat(fullFilePath)
	util.Check(err)
	//文件的大小
	fileSize := uint64(fileSizeHandler.Size())
	request := namenode.NameNodeWriteRequest{FileName: fileName, FileSize: fileSize}

	var reply []namenode.NameNodeMetaData
	//rpc调用Service.WriteData方法，写数据
	err = nameNodeInstance.Call("Service.WriteData", request, &reply)
	util.Check(err)

	var blockSize uint64
	err = nameNodeInstance.Call("Service.GetBlockSize", true, &blockSize)
	util.Check(err)

	fileHandler, err := os.Open(fullFilePath)
	util.Check(err)

	dataStagingBytes := make([]byte, blockSize)
	for _, metaData := range reply {
		n, err := fileHandler.Read(dataStagingBytes)
		util.Check(err)
		dataStagingBytes = dataStagingBytes[:n]

		blockId := metaData.BlockId
		blockAddresses := metaData.BlockAddresses

		startingDataNode := blockAddresses[0]
		remainingDataNodes := blockAddresses[1:]

		dataNodeInstance, rpcErr := rpc.Dial("tcp", startingDataNode.Host+":"+startingDataNode.ServicePort)
		util.Check(rpcErr)
		defer dataNodeInstance.Close()

		request := datanode.DataNodePutRequest{
			BlockId:          blockId,
			Data:             string(dataStagingBytes),
			ReplicationNodes: remainingDataNodes,
		}
		var reply datanode.DataNodeWriteStatus

		rpcErr = dataNodeInstance.Call("Service.PutData", request, &reply)
		util.Check(rpcErr)
		putStatus = true
	}
	return
}

func Get(nameNodeInstance *rpc.Client, fileName string) (fileContents string, getStatus bool) {
	request := namenode.NameNodeReadRequest{FileName: fileName}
	var reply []namenode.NameNodeMetaData

	err := nameNodeInstance.Call("Service.ReadData", request, &reply)
	util.Check(err)

	fileContents = ""

	for _, metaData := range reply {
		blockId := metaData.BlockId
		blockAddresses := metaData.BlockAddresses
		blockFetchStatus := false

		for _, selectedDataNode := range blockAddresses {
			dataNodeInstance, rpcErr := rpc.Dial("tcp", selectedDataNode.Host+":"+selectedDataNode.ServicePort)
			if rpcErr != nil {
				continue
			}

			defer dataNodeInstance.Close()

			request := datanode.DataNodeGetRequest{
				BlockId: blockId,
			}
			var reply datanode.DataNodeData

			rpcErr = dataNodeInstance.Call("Service.GetData", request, &reply)
			util.Check(rpcErr)
			fileContents += reply.Data
			blockFetchStatus = true
			break
		}

		if !blockFetchStatus {
			getStatus = false
			return
		}
	}

	getStatus = true
	return
}

// Put 上传文件
//func Put(nameNodeInstance *rpc.Client, sourcePath string, fileName string) (putStatus bool) {
//	//完整的文件路径
//	fullFilePath := sourcePath + fileName
//	fileSizeHandler, err := os.Stat(fullFilePath)
//	util.Check(err)
//	//文件的大小
//	fileSize := uint64(fileSizeHandler.Size())
//	request := namenode.NameNodeWriteRequest{FileName: fileName, FileSize: fileSize}
//
//	var reply []namenode.NameNodeMetaData
//	//rpc调用Service.WriteData方法，写数据
//	err = nameNodeInstance.Call("Service.WriteData", request, &reply)
//	util.Check(err)
//
//	var blockSize uint64//创建目录

func Mkdir(nameNodeInstance *rpc.Client, remote_file_path string) (mkDir bool) {
	var reply []util.DataNodeInstance
	var request = true
	err := nameNodeInstance.Call("Service.GetIdToDataNodes", request, &reply)
	util.Check(err)
	for _, dataNodeInstance1 := range reply {
		dataNodeInstance, rpcErr := rpc.Dial("tcp", dataNodeInstance1.Host+":"+dataNodeInstance1.ServicePort)
		util.Check(rpcErr)
		defer dataNodeInstance.Close()
		var reply datanode.DataNodeWriteStatus
		var request = remote_file_path
		rpcErr = dataNodeInstance.Call("Service.MakeDir", request, &reply)
		util.Check(rpcErr)
		mkDir = reply.Status
	}
	return
}
