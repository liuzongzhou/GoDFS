package client

import (
	"github.com/liuzongzhou/GoDFS/datanode"
	"github.com/liuzongzhou/GoDFS/namenode"
	"github.com/liuzongzhou/GoDFS/util"
	"log"
	"net/rpc"
	"os"
)

// Put 上传文件
func Put(nameNodeInstance *rpc.Client, sourcePath string, fileName string, remotefilepath string) (putStatus bool) {
	//完整的文件路径
	fullFilePath := sourcePath + fileName
	fileSizeHandler, err := os.Stat(fullFilePath)
	util.Check(err)
	//文件的大小
	fileSize := uint64(fileSizeHandler.Size())
	request := namenode.NameNodeWriteRequest{RemoteFilePath: remotefilepath, FileName: fileName, FileSize: fileSize}

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
			RemoteFilePath:   remotefilepath,
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

func Get(nameNodeInstance *rpc.Client, remotefilepath string, fileName string, local_file_path string) (getStatus bool) {
	request := namenode.NameNodeReadRequest{FileName: remotefilepath + fileName}
	var reply []namenode.NameNodeMetaData

	err := nameNodeInstance.Call("Service.ReadData", request, &reply)
	util.Check(err)

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
				RemoteFilePath: remotefilepath,
				BlockId:        blockId,
			}
			var reply datanode.DataNodeData

			rpcErr = dataNodeInstance.Call("Service.GetData", request, &reply)
			util.Check(rpcErr)
			f, err := os.OpenFile(local_file_path, os.O_RDONLY|os.O_CREATE|os.O_APPEND, 0666)
			if err != nil {
				log.Println("open file error :", err)
				return
			}
			defer f.Close()

			_, err = f.WriteString(reply.Data)
			if err != nil {
				log.Println(err)
				return
			}
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

func Mkdir(nameNodeInstance *rpc.Client, remote_file_path string) (mkDir bool) {
	var reply []util.DataNodeInstance
	var request = namenode.NameNodeMkDirRequest{ReMoteFilePath: remote_file_path}
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
		//当有一个节点创建目录失败时，终端，返回操作失败，提示用户再试一次
		if !mkDir {
			break
		}
	}
	return
}

func Stat(nameNodeInstance *rpc.Client, remote_file_path string, fileName string) (filename1 string, filesize uint64) {
	request := namenode.NameNodeReadRequest{FileName: remote_file_path + fileName}
	var reply namenode.NameNodeFileSize
	err := nameNodeInstance.Call("Service.FileSize", request, &reply)
	if err == nil {
		filename1 = fileName
		filesize = reply.FileSize
		return
	}
	filename1 = ""
	return
}

func ReName(nameNodeInstance *rpc.Client, renameSrcPath string, renameDestPath string) (reNameStatus bool) {
	request := namenode.NameNodeReNameRequest{ReNameSrcPath: renameSrcPath, ReNameDestPath: renameDestPath}
	var reply []util.DataNodeInstance
	err := nameNodeInstance.Call("Service.ReName", request, &reply)
	util.Check(err)
	for _, dataNodeInstance1 := range reply {
		dataNodeInstance, rpcErr := rpc.Dial("tcp", dataNodeInstance1.Host+":"+dataNodeInstance1.ServicePort)
		util.Check(rpcErr)
		defer dataNodeInstance.Close()
		var reply datanode.DataNodeWriteStatus
		var request = datanode.DataNodeReNameRequest{ReNameSrcPath: renameSrcPath, ReNameDestPath: renameDestPath}
		rpcErr = dataNodeInstance.Call("Service.ReNameDir", request, &reply)
		util.Check(rpcErr)
		reNameStatus = reply.Status
	}
	return
}

func DeletePath(nameNodeInstance *rpc.Client, remote_file_path string) (deletePathStatus bool) {
	request := namenode.NameNodeDeleteRequest{Remote_file_path: remote_file_path}
	var reply []util.DataNodeInstance
	err := nameNodeInstance.Call("Service.GetIdToDataNodes", request, &reply)
	util.Check(err)
	//1.先删除datanode下对应的文件路径
	for _, dataNodeInstance1 := range reply {
		dataNodeInstance, rpcErr := rpc.Dial("tcp", dataNodeInstance1.Host+":"+dataNodeInstance1.ServicePort)
		util.Check(rpcErr)
		defer dataNodeInstance.Close()
		var reply datanode.DataNodeWriteStatus
		var request = datanode.DataNodeDeleteRequest{Remotefilepath: remote_file_path}
		rpcErr = dataNodeInstance.Call("Service.DeletePath", request, &reply)
		util.Check(rpcErr)
		deletePathStatus = reply.Status
		//当出现错误时，说明某个节点删除失败，所以直接返回
		if !deletePathStatus {
			return
		}
	}
	//2.删除路径成功，这个时候所有的block都删除了，需要处理namenode里面的缓存数据：
	//FileNameToBlocks,
	//BlockToDataNodeIds,
	//FileNameSize,
	//DirectoryToFileName
	var reply1 bool
	err = nameNodeInstance.Call("Service.DeleteMetaData", request, &reply1)
	util.Check(err)
	deletePathStatus = reply1
	return
}

func DeleteFile(nameNodeInstance *rpc.Client, remote_file_path string, filename string) (deleteFileStatus bool) {
	request := namenode.NameNodeDeleteRequest{Remote_file_path: remote_file_path}
	var reply []util.DataNodeInstance
	err := nameNodeInstance.Call("Service.GetIdToDataNodes", request, &reply)
	util.Check(err)
	for _, dataNodeInstance1 := range reply {
		dataNodeInstance, rpcErr := rpc.Dial("tcp", dataNodeInstance1.Host+":"+dataNodeInstance1.ServicePort)
		util.Check(rpcErr)
		defer dataNodeInstance.Close()
		var reply datanode.DataNodeWriteStatus
		var request = datanode.DataNodeDeleteRequest{Remotefilepath: remote_file_path, FileName: filename}
		rpcErr = dataNodeInstance.Call("Service.DeleteFile", request, &reply)
		util.Check(rpcErr)
		deleteFileStatus = reply.Status
		if !deleteFileStatus {
			break
		}
	}
	return
}
