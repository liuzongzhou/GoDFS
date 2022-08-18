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
	//查看文件元信息
	fileSizeHandler, err := os.Stat(fullFilePath)
	//文件不存在或者路径有问题，直接返回false
	if err != nil {
		log.Println(err)
		putStatus = false
		return
	}
	//获取文件的大小
	fileSize := uint64(fileSizeHandler.Size())
	//文件写入请求：路径，文件名，文件大小
	request := namenode.NameNodeWriteRequest{RemoteFilePath: remotefilepath, FileName: fileName, FileSize: fileSize}
	//返回数据：元数据数组，包含每个BlockId对应多个datanodeId(备份)
	var reply []namenode.NameNodeMetaData
	//rpc调用Service.WriteData方法，写数据
	err = nameNodeInstance.Call("Service.WriteData", request, &reply)
	//rpc调用出现问题，直接返回false
	if err != nil {
		log.Println(err)
		putStatus = false
		return
	}
	// 通过rpc调用获取block块的大小
	var blockSize uint64
	err = nameNodeInstance.Call("Service.GetBlockSize", true, &blockSize)
	//rpc调用出现问题，直接返回false
	if err != nil {
		log.Println(err)
		putStatus = false
		return
	}

	//打开本地路径文件
	fileHandler, err := os.Open(fullFilePath)
	//文件不存在或者路径有问题，直接返回false
	if err != nil {
		log.Println(err)
		putStatus = false
		return
	}

	dataStagingBytes := make([]byte, blockSize)
	for _, metaData := range reply {
		n, err := fileHandler.Read(dataStagingBytes)
		util.Check(err)
		//append形式读取blocksize大小的文件
		dataStagingBytes = dataStagingBytes[:n]
		//要存取的blockId
		blockId := metaData.BlockId
		//该BlockId 对应的datanodes地址（ip+端口）
		blockAddresses := metaData.BlockAddresses
		//第一个为主datanode节点
		startingDataNode := blockAddresses[0]
		//剩下的节点都为备份节点
		remainingDataNodes := blockAddresses[1:]
		//与主datanode节点建立连接
		dataNodeInstance, rpcErr := rpc.Dial("tcp", startingDataNode.Host+":"+startingDataNode.ServicePort)
		//rpc连接出现问题，直接返回false
		if rpcErr != nil {
			log.Println(rpcErr)
			putStatus = false
			return
		}

		defer dataNodeInstance.Close()
		//文件写入请求：路径，BlockId，实际数据，备份的节点信息
		request := datanode.DataNodePutRequest{
			RemoteFilePath:   remotefilepath,
			BlockId:          blockId,
			Data:             string(dataStagingBytes),
			ReplicationNodes: remainingDataNodes,
		}
		//
		var reply datanode.DataNodeReplyStatus

		rpcErr = dataNodeInstance.Call("Service.PutData", request, &reply)
		//rpc调用出现问题，直接返回false
		if rpcErr != nil {
			log.Println(rpcErr)
			putStatus = false
			return
		}
		//如果写入失败，返回false
		if !reply.Status {
			putStatus = false
			return
		}
	}
	putStatus = true
	return
}

// Get 从远端下载文件,返回结果：下载是否成功
func Get(nameNodeInstance *rpc.Client, remoteFilepath string, fileName string, local_file_path string) (getStatus bool) {
	//文件读取请求：文件名：路径+文件名
	request := namenode.NameNodeReadRequest{FileName: remoteFilepath + fileName}
	//返回数据：元数据数组，包含每个BlockId对应多个datanodeId(备份)
	var reply []namenode.NameNodeMetaData
	// rpc调用读取文件对应的元数据数组
	err := nameNodeInstance.Call("Service.ReadData", request, &reply)
	//rpc调用出现问题，直接返回false
	if err != nil {
		log.Println(err)
		getStatus = false
		return
	}
	//遍历元数据数组信息,如果元数据为空，则直接返回false
	if len(reply) == 0 {
		return false
	}
	for _, metaData := range reply {
		//1个blockId 对应多个节点信息
		blockId := metaData.BlockId
		blockAddresses := metaData.BlockAddresses
		//该blockId文件抓取内容状态
		blockFetchStatus := false
		//遍历blockAddresses，第一个肯定是主节点
		for _, selectedDataNode := range blockAddresses {
			//rpc建立连接
			dataNodeInstance, rpcErr := rpc.Dial("tcp", selectedDataNode.Host+":"+selectedDataNode.ServicePort)
			//如果连接不上，还有备份节点，不必急于结束，实现了当单节点故障时，无障碍读取数据
			if rpcErr != nil {
				log.Printf("DataNode %v : %v read data fail,next datanode\n", selectedDataNode.Host, selectedDataNode.ServicePort)
				continue
			}

			defer dataNodeInstance.Close()
			// 连接成功的话，读取请求：FilePath+BlockId
			request := datanode.DataNodeGetRequest{
				RemoteFilePath: remoteFilepath,
				BlockId:        blockId,
			}
			//返回读取的数据内容
			var reply datanode.DataNodeData
			//rpc调用GetData方法，获取blockId对应的数据内容
			rpcErr = dataNodeInstance.Call("Service.GetData", request, &reply)
			//如果返回有故障，不必急于结束，实现了当单节点故障时，无障碍读取数据
			if rpcErr != nil {
				log.Printf("DataNode %v : %v read data fail,next datanode\n", selectedDataNode.Host, selectedDataNode.ServicePort)
				continue
			}
			//返回数据正常，则在本地目标路径打开文件，采用追加写入的方式，不存在则创建
			f, err := os.OpenFile(local_file_path, os.O_RDONLY|os.O_CREATE|os.O_APPEND, 0666)
			//打开失败（不是有效路径）,直接返回失败
			if err != nil {
				log.Println("open file error :", err)
				return false
			}
			defer f.Close()
			//打开成功，写入返回的数据，可以采用缓冲流写，也可以不采用
			_, err = f.WriteString(reply.Data)
			//出现问题，反回false
			if err != nil {
				log.Println(err)
				return false
			}
			//如果写入成功，那这个blockId文件的写入就完成，进行下一个blockId即可，更新blockFetchStatus状态
			blockFetchStatus = true
			log.Printf("DataNode %v : %v read data success,next BlockId\n", selectedDataNode.Host, selectedDataNode.ServicePort)
			break
		}
		//如果所有datanode,包含备份都遍历完了，但是还没写入成功，那就返回false
		if !blockFetchStatus {
			return false
		}
	}
	return true
}

// Mkdir 创建远端存储文件目录,返回创建成功与否
func Mkdir(nameNodeInstance *rpc.Client, remote_file_path string) (mkDir bool) {
	var reply []datanode.DataNodeInstance
	var request = true
	//获取当前存活的datanode元数据组信息：host+port
	err := nameNodeInstance.Call("Service.GetIdToDataNodes", request, &reply)
	//rpc调用出现问题，直接返回false
	if err != nil {
		log.Println(err)
		return false
	}
	//没有存活的节点，直接返回false
	if len(reply) == 0 {
		return false
	}
	//遍历所有存活的datanode节点
	for _, replydataNode := range reply {
		//建立rpc连接
		dataNodeInstance, rpcErr := rpc.Dial("tcp", replydataNode.Host+":"+replydataNode.ServicePort)
		//rpc连接出现问题，直接返回false
		if rpcErr != nil {
			log.Println(rpcErr)
			return false
		}
		defer dataNodeInstance.Close()
		//返回回复状态，是否成功
		var reply datanode.DataNodeReplyStatus
		//写入请求：相对路径
		var request = remote_file_path
		//rpc 调用创建datanode节点文件目录
		rpcErr = dataNodeInstance.Call("Service.MakeDir", request, &reply)
		//rpc调用出现问题，直接返回false，当有一个节点创建目录失败时，返回操作失败，可以建议提示用户再试一次
		if rpcErr != nil {
			log.Println(rpcErr)
			return false
		}
	}
	return true
}

// Stat 获取文件元数据信息：文件名+文件大小
func Stat(nameNodeInstance *rpc.Client, remote_file_path string, fileName string) (filename string, filesize uint64) {
	request := namenode.NameNodeReadRequest{FileName: remote_file_path + fileName}
	var reply namenode.NameNodeFileSize
	err := nameNodeInstance.Call("Service.FileSize", request, &reply)
	if err == nil {
		filename = fileName
		filesize = reply.FileSize
		return
	}
	//如果有异常，返回空文件名
	filename = ""
	return
}

// ReName 文件夹的重命名 返回重命名是否成功
func ReName(nameNodeInstance *rpc.Client, renameSrcPath string, renameDestPath string) (reNameStatus bool) {
	request := namenode.NameNodeReNameRequest{ReNameSrcPath: renameSrcPath, ReNameDestPath: renameDestPath}
	var reply []datanode.DataNodeInstance
	// rpc调用NameNode的ReName方法，传入重命名的renameSrcPath和renameDestPath
	// 修改NameNode的元数据信息并返回文件夹所在的DataNode节点
	err := nameNodeInstance.Call("Service.ReName", request, &reply)
	util.Check(err)
	// 对文件夹所在的DataNode节点进行遍历
	for _, dataNodeInstance1 := range reply {
		// 取得DataNode实例
		dataNodeInstance, rpcErr := rpc.Dial("tcp", dataNodeInstance1.Host+":"+dataNodeInstance1.ServicePort)
		util.Check(rpcErr)
		defer dataNodeInstance.Close()
		var reply datanode.DataNodeReplyStatus
		// 调用DataNode的ReNameDir方法去修改实际的路径信息
		var request = datanode.DataNodeReNameRequest{ReNameSrcPath: renameSrcPath, ReNameDestPath: renameDestPath}
		rpcErr = dataNodeInstance.Call("Service.ReNameDir", request, &reply)
		util.Check(rpcErr)
		reNameStatus = reply.Status
	}
	// 将NameNode元数据信息和DataNode的实际数据信息修改完之后返回
	return
}

// ReNameFile 文件的重命名，返回文件重命名是否成功
func ReNameFile(nameNodeInstance *rpc.Client, renameSrcFile string, renameDestFile string) (reNameStatus bool) {
	request := namenode.NameNodeReNameFileRequest{ReNameSrcFileName: renameSrcFile, ReNameDestFileName: renameDestFile}
	var reply *bool
	// rpc调用NameNode的ReName方法，传入重命名的ReNameSrcFileName和ReNameDestFileName
	// 修改NameNode的元数据信息并返回修改是否成功
	err := nameNodeInstance.Call("Service.ReNameFile", request, &reply)
	util.Check(err)
	reNameStatus = *reply
	return
}

// List 展示文件目录下面的文件信息， 传入文件目录，返回文件信息
func List(nameNodeInstance *rpc.Client, remoteDirName string) (fileInfo map[string]uint64) {
	request := namenode.NameNodeListRequest{RemoteDirPath: remoteDirName}
	var reply []namenode.ListMetaData
	// 在List过程中，我们只需要操作文件元数据，所以只需要调用NameNode的List方法即可
	err := nameNodeInstance.Call("Service.List", request, &reply)
	fileInfo = make(map[string]uint64)
	// 返回的数据通过Map形式返回
	for _, listMetaData := range reply {
		fileInfo[listMetaData.FileName] = listMetaData.FileSize
	}
	util.Check(err)
	return
}

// DeletePath 删除远端文件目录
func DeletePath(nameNodeInstance *rpc.Client, remote_file_path string) (deletePathStatus bool) {
	var request = true
	var reply []datanode.DataNodeInstance
	//1.先得到当前存活的所有dataNodes节点信息
	err := nameNodeInstance.Call("Service.GetIdToDataNodes", request, &reply)
	util.Check(err)
	//2.删除datanode下对应的文件路径
	for _, selectedDataNode := range reply {
		dataNodeInstance, rpcErr := rpc.Dial("tcp", selectedDataNode.Host+":"+selectedDataNode.ServicePort)
		//如果连接失败，可能当前datanode节点死亡，跳过
		if rpcErr != nil {
			log.Printf("DataNode %v : %v delete path fail,next datanode\n", selectedDataNode.Host, selectedDataNode.ServicePort)
			continue
		}
		defer dataNodeInstance.Close()
		var reply datanode.DataNodeReplyStatus
		//包装delete请求数据
		var request = datanode.DataNodeDeleteRequest{RemoteFilepath: remote_file_path}
		//通过rpc调用DeletePath,返回删除成功与否
		rpcErr = dataNodeInstance.Call("Service.DeletePath", request, &reply)
		//rpc调用失败，打印错误信息，返回错误
		if rpcErr != nil {
			log.Println(rpcErr)
			return false
		}
		deletePathStatus = reply.Status
		//当出现错误时，说明某个节点删除失败，所以直接返回
		if !deletePathStatus {
			return
		}
	}
	//3.删除路径成功，这个时候所有的block都删除了，需要处理nameNode里面的缓存数据：
	//FileNameToBlocks,
	//BlockToDataNodeIds,
	//FileNameSize,
	//DirectoryToFileName
	var reply1 bool
	var request1 = namenode.NameNodeDeleteRequest{RemoteFilePath: remote_file_path}
	//rpc 调用DeleteMetaData方法，删除相关元数据信息
	err = nameNodeInstance.Call("Service.DeleteMetaData", request1, &reply1)
	//rpc调用失败，打印错误信息，返回错误
	if err != nil {
		log.Println(err)
		return false
	}
	deletePathStatus = reply1
	return
}

//DeleteFile 删除远端文件
func DeleteFile(nameNodeInstance *rpc.Client, remoteFilePath string, filename string) (deleteFileStatus bool) {
	request := namenode.NameNodeReadRequest{FileName: remoteFilePath + filename}
	var reply []namenode.NameNodeMetaData
	// 通过rpc调用 ReadData 读取文件对应的元数据数组
	// 返回信息：BlockIds对应的BlockAddresses（datanode的host+port）
	err := nameNodeInstance.Call("Service.ReadData", request, &reply)
	//rpc调用失败，打印错误信息，返回错误
	if err != nil {
		log.Println(err)
		return false
	}
	for _, metaData := range reply {
		blockId := metaData.BlockId
		blockAddresses := metaData.BlockAddresses
		//每个BlockId对应多个BlockAddress，所以要遍历删除
		for _, selectedDataNode := range blockAddresses {
			dataNodeInstance, rpcErr := rpc.Dial("tcp", selectedDataNode.Host+":"+selectedDataNode.ServicePort)
			//如果连接失败，可能当前datanode节点死亡，跳过
			if rpcErr != nil {
				log.Printf("DataNode %v : %v delete path fail,next datanode\n", selectedDataNode.Host, selectedDataNode.ServicePort)
				continue
			}

			defer dataNodeInstance.Close()

			request := datanode.DataNodeDeleteRequest{
				RemoteFilepath: remoteFilePath,
				BlockId:        blockId,
			}
			var reply datanode.DataNodeReplyStatus
			//通过rpc调用DeleteFile,返回删除成功与否
			rpcErr = dataNodeInstance.Call("Service.DeleteFile", request, &reply)
			util.Check(rpcErr)
			deleteFileStatus = reply.Status
			if !deleteFileStatus {
				return
			}
		}
	}
	//2.删除所有的BlockId成功，需要处理nameNode里面的缓存数据：
	//FileNameToBlocks,
	//BlockToDataNodeIds,
	//FileNameSize,
	//DirectoryToFileName value中的filename
	var reply1 bool
	var request1 = namenode.NameNodeDeleteRequest{RemoteFilePath: remoteFilePath, FileName: filename}
	err = nameNodeInstance.Call("Service.DeleteFileNameMetaData", request1, &reply1)
	//rpc调用失败，打印错误信息，返回错误
	if err != nil {
		log.Println(err)
		return false
	}
	deleteFileStatus = reply1
	return
}
