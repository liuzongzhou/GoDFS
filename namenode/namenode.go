package namenode

import (
	"errors"
	"github.com/google/uuid"
	"github.com/liuzongzhou/GoDFS/datanode"
	"log"
	"math"
	"math/rand"
	"net/rpc"
	"strings"
)

// NameNodeMetaData nameNode的元数据，包含块id，块地址
type NameNodeMetaData struct {
	BlockId        string
	BlockAddresses []datanode.DataNodeInstance //datanode的实例数组
}

type NameNodeReadRequest struct {
	FileName string
}

type NameNodeFileSize struct {
	FileSize uint64
}

// NameNodeWriteRequest nameNode写入请求
type NameNodeWriteRequest struct {
	RemoteFilePath string
	FileName       string
	FileSize       uint64
}

type ReDistributeDataRequest struct {
	DataNodeUri string
}

type UnderReplicatedBlocks struct {
	BlockId           string
	HealthyDataNodeId uint64
}

type NameNodeReNameRequest struct {
	ReNameSrcPath  string
	ReNameDestPath string
}

type NameNodeDeleteRequest struct {
	RemoteFilePath string
	FileName       string
}

type ListMetaData struct {
	FileName string
	FileSize uint64
}

type NameNodeListRequest struct {
	RemoteDirPath string
}

type NameNodeReNameFileRequest struct {
	ReNameSrcFileName  string
	ReNameDestFileName string
}

type Service struct {
	PrimaryPort         string
	Host                string
	Port                uint16
	BlockSize           uint64
	ReplicationFactor   uint64
	IdToDataNodes       map[uint64]datanode.DataNodeInstance
	FileNameToBlocks    map[string][]string //key:path+filename
	BlockToDataNodeIds  map[string][]uint64 //key:BlockId value：主+备份节点
	FileNameSize        map[string]uint64   //文件大小 key:path+filename
	DirectoryToFileName map[string][]string //目录下对应的文件 key:path value：该目录下所有文件名
}

func NewService(primaryPort string, serverHost string, blockSize uint64, replicationFactor uint64, serverPort uint16) *Service {
	return &Service{
		PrimaryPort:         primaryPort,
		Host:                serverHost,
		Port:                serverPort,
		BlockSize:           blockSize,
		ReplicationFactor:   replicationFactor,
		FileNameToBlocks:    make(map[string][]string),
		IdToDataNodes:       make(map[uint64]datanode.DataNodeInstance),
		BlockToDataNodeIds:  make(map[string][]uint64),
		FileNameSize:        make(map[string]uint64),
		DirectoryToFileName: make(map[string][]string),
	}
}

// ReplicationnameNode 获取主nameNode节点的元数据信息：FileNameToBlocks，IdToDataNodes，BlockToDataNodeIds，FileNameSize,DirectoryToFileName
func (nameNode *Service) ReplicationnameNode(request *bool, reply *Service) error {
	if *request {
		*reply = Service{
			IdToDataNodes:       nameNode.IdToDataNodes,
			FileNameSize:        nameNode.FileNameSize,
			BlockToDataNodeIds:  nameNode.BlockToDataNodeIds,
			FileNameToBlocks:    nameNode.FileNameToBlocks,
			DirectoryToFileName: nameNode.DirectoryToFileName,
		}
		return nil
	}
	return errors.New("获取元数据失败")
}

//selectRandomNumbers 随机选择存储节点，尽量做到负载均衡
func selectRandomNumbers(dataNodesAvailable []uint64, replicationFactor uint64) (randomNumberSet []uint64) {
	//当前已经选择的datanodeId,防止备份BlockId文件写再同一个节点上
	numberPresentMap := make(map[uint64]bool)
	for i := uint64(0); i < replicationFactor; {
		//随机选择节点
		datanodeId := dataNodesAvailable[rand.Intn(len(dataNodesAvailable))]
		if _, ok := numberPresentMap[datanodeId]; !ok {
			numberPresentMap[datanodeId] = true
			randomNumberSet = append(randomNumberSet, datanodeId)
			i++
		}
	}
	return
}

// GetBlockSize 获取存储的每个Block大小
func (nameNode *Service) GetBlockSize(request bool, reply *uint64) error {
	if request {
		*reply = nameNode.BlockSize
	}
	return nil
}

// ReadData 读取文件对应的元数据数组
// request:文件名：path + fileName
// 返回信息：BlockIds对应的BlockAddresses（datanode的host+port）
func (nameNode *Service) ReadData(request *NameNodeReadRequest, reply *[]NameNodeMetaData) error {
	//读取nameNode里面FileNameToBlocks的元数据信息，通过key获取BlockIds
	fileBlocks := nameNode.FileNameToBlocks[request.FileName]
	//遍历每个BlockId
	for _, block := range fileBlocks {
		var blockAddresses []datanode.DataNodeInstance
		//存储每个BlockId所有的datanodeId,包含备份的
		targetDataNodeIds := nameNode.BlockToDataNodeIds[block]
		for _, dataNodeId := range targetDataNodeIds {
			//将datanode的host+port打包
			blockAddresses = append(blockAddresses, nameNode.IdToDataNodes[dataNodeId])
		}
		//返回打包的元数据信息
		*reply = append(*reply, NameNodeMetaData{BlockId: block, BlockAddresses: blockAddresses})
	}
	return nil
}

// FileSize 获取文件对应的文件大小
func (nameNode *Service) FileSize(request *NameNodeReadRequest, reply *NameNodeFileSize) error {
	//判断元数据信息FileNameSize是否含有key：path+filename,存在取得value:filesize,否则输出文件不存在
	if value, ok := nameNode.FileNameSize[request.FileName]; ok {
		reply.FileSize = value // 存在
		return nil
	}
	return errors.New("文件不存在")
}

// WriteData 传入写入请求：{路径，文件名，文件大小}
// 返回数据：元数据数组，包含每个BlockId对应多个datanodeId(备份)
func (nameNode *Service) WriteData(request *NameNodeWriteRequest, reply *[]NameNodeMetaData) error {
	// 维护FileNameToBlocks元数据信息，key:路径+文件名 value:blockIds 目的：防止出现同名文件无法判断，唯一性
	nameNode.FileNameToBlocks[request.RemoteFilePath+request.FileName] = []string{}
	// 维护DirectoryToFileName元数据信息 key:路径 value:该路径下的所有文件名
	// 当存在此key，则往value数组里面添加值
	if _, ok := nameNode.DirectoryToFileName[request.RemoteFilePath]; ok {
		nameNode.DirectoryToFileName[request.RemoteFilePath] = append(nameNode.DirectoryToFileName[request.RemoteFilePath], request.FileName)
	} else { //如果不存在，则新建key并添加值
		nameNode.DirectoryToFileName[request.RemoteFilePath] = []string{request.FileName}
	}
	// 维护FileNameSize元数据信息 key:路径+文件 value:该文件的大小
	nameNode.FileNameSize[request.RemoteFilePath+request.FileName] = request.FileSize
	//向上取整 计算上传文件需要分割的块数
	numberOfBlocksToAllocate := uint64(math.Ceil(float64(request.FileSize) / float64(nameNode.BlockSize)))
	// 实现分配方案：文件存在哪些datanode节点上（包含备份）
	*reply = nameNode.allocateBlocks(request.RemoteFilePath+request.FileName, numberOfBlocksToAllocate)
	return nil
}

//GetIdToDataNodes 获取当前存活的datanode元数据组信息：host+port
func (nameNode *Service) GetIdToDataNodes(request *bool, reply *[]datanode.DataNodeInstance) error {
	if *request {
		for _, instance := range nameNode.IdToDataNodes {
			*reply = append(*reply, instance)
		}
	}
	return nil
}

//DeleteMetaData 删除路径相关的元数据信息
func (nameNode *Service) DeleteMetaData(request *NameNodeDeleteRequest, reply *bool) error {
	ReMoteFilePath := request.RemoteFilePath
	//遍历指定目录下的所有文件
	for _, filename := range nameNode.DirectoryToFileName[ReMoteFilePath] {
		//遍历指定文件名下的所有BlockId
		for _, BlockId := range nameNode.FileNameToBlocks[ReMoteFilePath+filename] {
			//删除BlockToDataNodeIds的这个key
			delete(nameNode.BlockToDataNodeIds, BlockId)
		}
		//删除FileNameToBlocks的key：ReMoteFilePath+filename
		delete(nameNode.FileNameToBlocks, ReMoteFilePath+filename)
		//删除FileNameSize的key：ReMoteFilePath+filename
		delete(nameNode.FileNameSize, ReMoteFilePath+filename)
	}
	//删除DirectoryToFileName的key：ReMoteFilePath
	delete(nameNode.DirectoryToFileName, ReMoteFilePath)
	*reply = true
	return nil
}

//DeleteFileNameMetaData 删除文件相关的元数据信息
func (nameNode *Service) DeleteFileNameMetaData(request *NameNodeDeleteRequest, reply *bool) error {
	ReMoteFilePath := request.RemoteFilePath
	fileName := request.FileName
	//遍历指定文件名下的所有BlockId
	for _, BlockId := range nameNode.FileNameToBlocks[ReMoteFilePath+fileName] {
		//删除BlockToDataNodeIds的这个key
		delete(nameNode.BlockToDataNodeIds, BlockId)
	}
	//删除FileNameToBlocks的key：ReMoteFilePath+filename
	delete(nameNode.FileNameToBlocks, ReMoteFilePath+fileName)
	//删除FileNameSize的key：ReMoteFilePath+filename
	delete(nameNode.FileNameSize, ReMoteFilePath+fileName)
	//删除DirectoryToFileName[ReMoteFilePath]的value中filename的值，采取的方法是除filename以外遍历插入新的数组
	filenames := nameNode.DirectoryToFileName[ReMoteFilePath]
	var newfilenames []string
	for i, filename := range filenames {
		if filename == fileName {
			newfilenames = append(filenames[:i], filenames[i+1:]...)
		}
	}
	//更新DirectoryToFileName[ReMoteFilePath] = newfilenames
	nameNode.DirectoryToFileName[ReMoteFilePath] = newfilenames
	*reply = true
	return nil
}

// allocateBlocks 实现分配方案：文件存在哪些datanode节点上（包含备份）
func (nameNode *Service) allocateBlocks(fileName string, numberOfBlocks uint64) (metadata []NameNodeMetaData) {
	//建立FileNameToBlocks的元数据库信息 key：path+filename
	nameNode.FileNameToBlocks[fileName] = []string{}
	var dataNodesAvailable []uint64
	//当前存活的，添加当前可用的datanodeId
	for k, _ := range nameNode.IdToDataNodes {
		dataNodesAvailable = append(dataNodesAvailable, k)
	}
	//统计可用的datanode数量
	dataNodesAvailableCount := uint64(len(dataNodesAvailable))
	//总共要生成的block数，挨个遍历生成，分配datanode节点
	for i := uint64(0); i < numberOfBlocks; i++ {
		//生成uuid，作为datanode底层存储的文件名
		blockId := uuid.New().String()
		//维护FileNameToBlocks的元数据库信息
		nameNode.FileNameToBlocks[fileName] = append(nameNode.FileNameToBlocks[fileName], blockId)

		var blockAddresses []datanode.DataNodeInstance
		var replicationFactor uint64
		// client设置的副本数大于可用的datanode节点数时，则副本数自动变成可用的节点数
		if nameNode.ReplicationFactor > dataNodesAvailableCount {
			replicationFactor = dataNodesAvailableCount
		} else { //否则就按照client 设置的来
			replicationFactor = nameNode.ReplicationFactor
		}
		// 具体安排这个BlockId文件存在哪些datanodes(包含备份)
		targetDataNodeIds := nameNode.assignDataNodes(blockId, dataNodesAvailable, replicationFactor)
		//获取blockId对应的datanodes元数据信息数组
		for _, dataNodeId := range targetDataNodeIds {
			blockAddresses = append(blockAddresses, nameNode.IdToDataNodes[dataNodeId])
		}
		//追加元数据信息
		metadata = append(metadata, NameNodeMetaData{BlockId: blockId, BlockAddresses: blockAddresses})
	}
	return
}

// assignDataNodes 具体安排这个BlockId文件存在哪些dataNodes(包含备份)
func (nameNode *Service) assignDataNodes(blockId string, dataNodesAvailable []uint64, replicationFactor uint64) []uint64 {
	//随机选择存储节点，尽量做到负载均衡
	targetDataNodeIds := selectRandomNumbers(dataNodesAvailable, replicationFactor)
	//维护BlockToDataNodeIds元数据信息，key:blockId value：存储的datanodeId
	nameNode.BlockToDataNodeIds[blockId] = targetDataNodeIds
	return targetDataNodeIds
}

// ReName 重命名文件目录
func (nameNode *Service) ReName(request *NameNodeReNameRequest, reply *[]datanode.DataNodeInstance) error {
	renameSrcPath := request.ReNameSrcPath
	renameDestPath := request.ReNameDestPath
	// 返回NameNodes的元数据信息
	for _, instance := range nameNode.IdToDataNodes {
		*reply = append(*reply, instance)
	}
	// 修改目录下文件的元数据信息（文件绝对路径FileName和存储Blocks的映射关系）
	for fileName, Blocks := range nameNode.FileNameToBlocks {
		// 获取当前目录下需要修改的文件元数据
		if strings.HasPrefix(fileName, renameSrcPath) {
			// 删除当前目录的元数据信息
			delete(nameNode.FileNameToBlocks, fileName)
			// 修改文件的绝对路径信息
			fileName = strings.Replace(fileName, renameSrcPath, renameDestPath, 1)
			// 修改文件绝对路径和Block的映射关系
			nameNode.FileNameToBlocks[fileName] = Blocks
		}
	}
	// 修改目录下文件的元数据信息（文件绝对路径FileName和FileSize的映射关系）
	for fileName, FileSize := range nameNode.FileNameSize {
		// 获取当前目录下需要修改的文件元数据
		if strings.HasPrefix(fileName, renameSrcPath) {
			delete(nameNode.FileNameSize, fileName)
			// 修改文件的绝对路径信息
			fileName = strings.Replace(fileName, renameSrcPath, renameDestPath, 1)
			// 修改文件绝对路径FileName和FileSize的映射关系
			nameNode.FileNameSize[fileName] = FileSize
		}
	}
	// 修改目录下文件的元数据信息（文件绝对路径Directory和FileName的映射关系）
	fileNames := nameNode.DirectoryToFileName[renameSrcPath]
	delete(nameNode.DirectoryToFileName, renameSrcPath)
	nameNode.DirectoryToFileName[renameDestPath] = fileNames
	return nil
}

// ReNameFile 修改文件名
func (nameNode *Service) ReNameFile(request *NameNodeReNameFileRequest, reply *bool) error {
	ReNameSrcFileName := request.ReNameSrcFileName
	ReNameDestFileName := request.ReNameDestFileName
	// 修改文件的元数据信息（文件绝对路径FileName和存储Blocks的映射关系）
	Blocks := nameNode.FileNameToBlocks[ReNameSrcFileName]
	// 删除当前文件的元数据信息
	delete(nameNode.FileNameToBlocks, ReNameSrcFileName)
	nameNode.FileNameToBlocks[ReNameDestFileName] = Blocks
	// 修改文件的元数据信息（文件绝对路径FileName和FileSize的映射关系）
	FileSize := nameNode.FileNameSize[ReNameSrcFileName]
	delete(nameNode.FileNameSize, ReNameSrcFileName)
	// 修改文件绝对路径FileName和FileSize的映射关系
	nameNode.FileNameSize[ReNameDestFileName] = FileSize

	// 处理路径+文件名的字符串
	split := strings.Split(ReNameSrcFileName, "/")
	split1 := strings.Split(ReNameDestFileName, "/")
	// 得到srcFileName
	srcFileName := split[len(split)-1]
	// 得到destFileName
	destFileName := split1[len(split1)-1]
	// 得到path
	var directory string
	for i := 0; i < len(split)-1; i++ {
		directory += split[i] + "/"
	}
	// 修改目录下文件的元数据信息（文件绝对路径Directory和FileName的映射关系）
	filenames := nameNode.DirectoryToFileName[directory]
	for i, filename := range filenames {
		if filename == srcFileName {
			filenames[i] = destFileName
			nameNode.DirectoryToFileName[directory] = filenames
			break
		}
	}

	*reply = true
	return nil
}

// List 罗列出文件夹中的文件信息
func (nameNode *Service) List(request *NameNodeListRequest, reply *[]ListMetaData) error {
	RemoteDirPath := request.RemoteDirPath
	//获取当前目录下的所有文件名
	filenames := nameNode.DirectoryToFileName[RemoteDirPath]
	//遍历这些文件，获取文件的元数据信息
	for _, filename := range filenames {
		filesize := nameNode.FileNameSize[RemoteDirPath+filename]
		// 追加的形式返回文件元数据列表
		*reply = append(*reply, ListMetaData{FileName: filename, FileSize: filesize})
	}
	return nil
}

// ReDistributeData 当有dataNode节点dead，将死亡的节点上的数据进行备份，并重新分配节点（主+备）写入
func (nameNode *Service) ReDistributeData(request *ReDistributeDataRequest, reply *bool) error {
	log.Printf("DataNode %s is dead, trying to redistribute data\n", request.DataNodeUri)
	deadDataNodeSlice := strings.Split(request.DataNodeUri, ":")
	var deadDataNodeId uint64

	// 遍历元数据IdToDataNodes，确定是IdToDataNodes哪个key：Id是dead的节点
	for id, dn := range nameNode.IdToDataNodes {
		if dn.Host == deadDataNodeSlice[0] && dn.ServicePort == deadDataNodeSlice[1] {
			deadDataNodeId = id
			break
		}
	}
	//维护元数据IdToDataNodes，删除dead的key-value
	delete(nameNode.IdToDataNodes, deadDataNodeId)

	//创建需要复制块列表
	var underReplicatedBlocksList []UnderReplicatedBlocks
	// 遍历BlockToDataNodeIds，得到blockId：{对应的dataNodes}
	for blockId, dnIds := range nameNode.BlockToDataNodeIds {
		for i, dnId := range dnIds {
			//当blockId中的dataNodes匹配上deadDataNodeId时，记录1个备份的DataNodeId作为healthyDataNodeId
			//并跳出循环，因为一个节点的BlockId损坏指挥匹配一个DataNodeId
			if dnId == deadDataNodeId {
				//备份的DataNodeId有多个，都存在BlockToDataNodeIds[blockId]，如果是坏的恰好是最后一个就会有问题，所以取余
				healthyDataNodeId := nameNode.BlockToDataNodeIds[blockId][(i+1)%len(dnIds)]
				//包装UnderReplicatedBlocks{blockId，备份的节点Id}
				underReplicatedBlocksList = append(
					underReplicatedBlocksList,
					UnderReplicatedBlocks{blockId, healthyDataNodeId},
				)
				break
			}
		}
	}

	// 判断当前可用dataNodes节点数是否足够
	if len(nameNode.IdToDataNodes) < int(nameNode.ReplicationFactor) {
		log.Println("Replication not possible due to unavailability of sufficient DataNode(s)")
		return nil
	}

	// 获取当前可用的dataNodesId
	var availableNodes []uint64
	for k, _ := range nameNode.IdToDataNodes {
		availableNodes = append(availableNodes, k)
	}

	// 遍历需要复制块列表
	for _, blockToReplicate := range underReplicatedBlocksList {
		var remoteFilePath string
		flag := false
		// 只有要复制的BlockId，要得到文件的有效的路径名（不包含文件名），为了读取数据BlockId中的数据
		for filename, fileblockIds := range nameNode.FileNameToBlocks {
			// 遍历每个文件对应的fileblockIds，当要复制的BlockId与其匹配时，当前的filename:path+name 就是路径
			for _, id := range fileblockIds {
				if blockToReplicate.BlockId == id {
					flag = true
					break
				}
			}
			if flag {
				// 将filename切分，得到只含路径名
				split := strings.Split(filename, "/")
				for i := 0; i < len(split)-1; i++ {
					remoteFilePath += split[i] + "/"
				}
				break
			}
		}
		// 从要复制的节点读取数据
		healthyDataNode := nameNode.IdToDataNodes[blockToReplicate.HealthyDataNodeId]
		dataNodeInstance, rpcErr := rpc.Dial("tcp", healthyDataNode.Host+":"+healthyDataNode.ServicePort)
		if rpcErr != nil {
			log.Println(rpcErr)
			continue
		}

		defer dataNodeInstance.Close()

		getRequest := datanode.DataNodeGetRequest{
			RemoteFilePath: remoteFilePath,
			BlockId:        blockToReplicate.BlockId,
		}
		var getReply datanode.DataNodeData

		rpcErr = dataNodeInstance.Call("Service.GetData", getRequest, &getReply)
		if rpcErr != nil {
			log.Println(rpcErr)
			continue
		}
		// 存放BlockId读取的数据
		blockContents := getReply.Data

		// 1.删除原存在的这个BlockId数据,因为备份不止一块，所以需要遍历删除
		ids := nameNode.BlockToDataNodeIds[blockToReplicate.BlockId]
		for _, dataNodeId := range ids {
			dataNode := nameNode.IdToDataNodes[dataNodeId]
			dataNodeInstance, rpcErr := rpc.Dial("tcp", dataNode.Host+":"+dataNode.ServicePort)
			if rpcErr != nil {
				continue
			}
			defer dataNodeInstance.Close()
			request := datanode.DataNodeDeleteRequest{
				RemoteFilepath: remoteFilePath,
				BlockId:        blockToReplicate.BlockId,
			}
			var reply datanode.DataNodeReplyStatus
			//通过rpc调用DeleteFile
			rpcErr = dataNodeInstance.Call("Service.DeleteFile", request, &reply)
			if rpcErr != nil {
				log.Println(rpcErr)
			}
		}

		//2.删除原存在的这个BlockId数据成功，重新写入数据给节点,更新BlockToDataNodeIds元数据
		//分配给哪些节点，得到目标节点
		targetDataNodeIds := nameNode.assignDataNodes(blockToReplicate.BlockId, availableNodes, nameNode.ReplicationFactor)
		//更新BlockToDataNodeIds元数据，不涉及其他元数据的变更，只是BlockId与dataNodes的映射
		nameNode.BlockToDataNodeIds[blockToReplicate.BlockId] = targetDataNodeIds
		var blockAddresses []datanode.DataNodeInstance
		// 依次生成写入dataNode地址
		for _, dataNodeId := range targetDataNodeIds {
			blockAddresses = append(blockAddresses, nameNode.IdToDataNodes[dataNodeId])
		}
		// 主节点
		startingDataNode := blockAddresses[0]
		// 剩余备份节点
		remainingDataNodes := blockAddresses[1:]

		targetDataNodeInstance, rpcErr := rpc.Dial("tcp", startingDataNode.Host+":"+startingDataNode.ServicePort)
		if rpcErr != nil {
			log.Println(rpcErr)
			continue
		}
		defer targetDataNodeInstance.Close()

		putRequest := datanode.DataNodePutRequest{
			RemoteFilePath:   remoteFilePath,
			BlockId:          blockToReplicate.BlockId,
			Data:             blockContents,
			ReplicationNodes: remainingDataNodes,
		}
		var putReply datanode.DataNodeReplyStatus
		// 将数据写入对用的节点
		rpcErr = targetDataNodeInstance.Call("Service.PutData", putRequest, &putReply)
		if rpcErr != nil {
			log.Println(rpcErr)
			continue
		}
		// 打印重新分配的数据的写入分布情况
		log.Printf("Block %s replication completed for %+v\n", blockToReplicate.BlockId, targetDataNodeIds)
	}

	return nil
}
