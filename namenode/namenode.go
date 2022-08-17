package namenode

import (
	"errors"
	"github.com/google/uuid"
	"github.com/liuzongzhou/GoDFS/datanode"
	"github.com/liuzongzhou/GoDFS/util"
	"log"
	"math"
	"math/rand"
	"net/rpc"
	"strings"
)

// namenode的元数据，包含块id，块地址
type NameNodeMetaData struct {
	BlockId        string
	BlockAddresses []util.DataNodeInstance //datanode的实例数组
}

type NameNodeReadRequest struct {
	FileName string
}
type NameNodeFileSize struct {
	FileSize uint64
}

// NameNodeWriteRequest namenode写入请求
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

type Service struct {
	Port                uint16
	BlockSize           uint64
	ReplicationFactor   uint64
	IdToDataNodes       map[uint64]util.DataNodeInstance
	FileNameToBlocks    map[string][]string //key:path+filename
	BlockToDataNodeIds  map[string][]uint64
	FileNameSize        map[string]uint64 //文件大小  //key:path+filename
	DirectoryToFileName map[string][]string
}

func NewService(blockSize uint64, replicationFactor uint64, serverPort uint16) *Service {
	return &Service{
		Port:                serverPort,
		BlockSize:           blockSize,
		ReplicationFactor:   replicationFactor,
		FileNameToBlocks:    make(map[string][]string),
		IdToDataNodes:       make(map[uint64]util.DataNodeInstance),
		BlockToDataNodeIds:  make(map[string][]uint64),
		FileNameSize:        make(map[string]uint64),
		DirectoryToFileName: make(map[string][]string),
	}
}

func selectRandomNumbers(availableItems []uint64, count uint64) (randomNumberSet []uint64) {
	numberPresentMap := make(map[uint64]bool)
	for i := uint64(0); i < count; {
		chosenItem := availableItems[rand.Intn(len(availableItems))]
		if _, ok := numberPresentMap[chosenItem]; !ok {
			numberPresentMap[chosenItem] = true
			randomNumberSet = append(randomNumberSet, chosenItem)
			i++
		}
	}
	return
}

func (nameNode *Service) GetBlockSize(request bool, reply *uint64) error {
	if request {
		*reply = nameNode.BlockSize
	}
	return nil
}

func (nameNode *Service) ReadData(request *NameNodeReadRequest, reply *[]NameNodeMetaData) error {
	fileBlocks := nameNode.FileNameToBlocks[request.FileName]

	for _, block := range fileBlocks {
		var blockAddresses []util.DataNodeInstance

		targetDataNodeIds := nameNode.BlockToDataNodeIds[block]
		for _, dataNodeId := range targetDataNodeIds {
			blockAddresses = append(blockAddresses, nameNode.IdToDataNodes[dataNodeId])
		}

		*reply = append(*reply, NameNodeMetaData{BlockId: block, BlockAddresses: blockAddresses})
	}
	return nil
}
func (nameNode *Service) FileSize(request *NameNodeReadRequest, reply *NameNodeFileSize) error {
	if value, ok := nameNode.FileNameSize[request.FileName]; ok {
		reply.FileSize = value // 存在
		return nil
	}
	return errors.New("文件不存在")
}
func (nameNode *Service) WriteData(request *NameNodeWriteRequest, reply *[]NameNodeMetaData) error {
	nameNode.FileNameToBlocks[request.RemoteFilePath+request.FileName] = []string{}
	if _, ok := nameNode.DirectoryToFileName[request.RemoteFilePath]; ok {
		nameNode.DirectoryToFileName[request.RemoteFilePath] = append(nameNode.DirectoryToFileName[request.RemoteFilePath], request.FileName)
	} else {
		nameNode.DirectoryToFileName[request.RemoteFilePath] = []string{request.FileName}
	}
	//向上取整 需要分配的块数
	nameNode.FileNameSize[request.RemoteFilePath+request.FileName] = request.FileSize
	numberOfBlocksToAllocate := uint64(math.Ceil(float64(request.FileSize) / float64(nameNode.BlockSize)))
	*reply = nameNode.allocateBlocks(request.RemoteFilePath+request.FileName, numberOfBlocksToAllocate)
	return nil
}

type NameNodeMkDirRequest struct {
	ReMoteFilePath string
}

func (nameNode *Service) GetIdToDataNodes(request *NameNodeMkDirRequest, reply *[]util.DataNodeInstance) error {
	for _, instance := range nameNode.IdToDataNodes {
		*reply = append(*reply, instance)
	}
	return nil
}

func (nameNode *Service) DeleteMetaData(request *NameNodeDeleteRequest, reply *bool) error {
	ReMoteFilePath := request.Remote_file_path
	//遍历指定目录下的所有文件
	for _, filename := range nameNode.DirectoryToFileName[ReMoteFilePath] {
		for _, BlockId := range nameNode.FileNameToBlocks[ReMoteFilePath+filename] {
			//删除这个BlockId的key
			delete(nameNode.BlockToDataNodeIds, BlockId)
		}
		delete(nameNode.FileNameToBlocks, ReMoteFilePath+filename)
		delete(nameNode.FileNameSize, ReMoteFilePath+filename)
	}
	delete(nameNode.DirectoryToFileName, ReMoteFilePath)
	*reply = true
	return nil
}

func (nameNode *Service) DeleteFileNameMetaData(request *NameNodeDeleteRequest, reply *bool) error {
	ReMoteFilePath := request.Remote_file_path
	fileName := request.FileName
	//遍历指定目录下的所有文件
	for _, BlockId := range nameNode.FileNameToBlocks[ReMoteFilePath+fileName] {
		//删除这个BlockId的key
		delete(nameNode.BlockToDataNodeIds, BlockId)
	}
	delete(nameNode.FileNameToBlocks, ReMoteFilePath+fileName)
	delete(nameNode.FileNameSize, ReMoteFilePath+fileName)
	delete(nameNode.DirectoryToFileName, ReMoteFilePath)
	*reply = true
	return nil
}
func (nameNode *Service) allocateBlocks(fileName string, numberOfBlocks uint64) (metadata []NameNodeMetaData) {
	nameNode.FileNameToBlocks[fileName] = []string{}
	var dataNodesAvailable []uint64
	for k, _ := range nameNode.IdToDataNodes {
		dataNodesAvailable = append(dataNodesAvailable, k)
	}
	dataNodesAvailableCount := uint64(len(dataNodesAvailable))

	for i := uint64(0); i < numberOfBlocks; i++ {
		blockId := uuid.New().String()
		nameNode.FileNameToBlocks[fileName] = append(nameNode.FileNameToBlocks[fileName], blockId)

		var blockAddresses []util.DataNodeInstance
		var replicationFactor uint64
		if nameNode.ReplicationFactor > dataNodesAvailableCount {
			replicationFactor = dataNodesAvailableCount
		} else {
			replicationFactor = nameNode.ReplicationFactor
		}

		targetDataNodeIds := nameNode.assignDataNodes(blockId, dataNodesAvailable, replicationFactor)
		for _, dataNodeId := range targetDataNodeIds {
			blockAddresses = append(blockAddresses, nameNode.IdToDataNodes[dataNodeId])
		}

		metadata = append(metadata, NameNodeMetaData{BlockId: blockId, BlockAddresses: blockAddresses})
	}
	return
}

func (nameNode *Service) assignDataNodes(blockId string, dataNodesAvailable []uint64, replicationFactor uint64) []uint64 {
	targetDataNodeIds := selectRandomNumbers(dataNodesAvailable, replicationFactor)
	nameNode.BlockToDataNodeIds[blockId] = targetDataNodeIds
	return targetDataNodeIds
}

type NameNodeReNameRequest struct {
	ReNameSrcPath  string
	ReNameDestPath string
}
type NameNodeDeleteRequest struct {
	Remote_file_path string
	FileName         string
}
type ListMetaData struct {
	FileName string
	FileSize uint64
}

func (nameNode *Service) ReName(request *NameNodeReNameRequest, reply *[]util.DataNodeInstance) error {
	renameSrcPath := request.ReNameSrcPath
	renameDestPath := request.ReNameDestPath
	for _, instance := range nameNode.IdToDataNodes {
		*reply = append(*reply, instance)
	}
	for fileName, Blocks := range nameNode.FileNameToBlocks {
		if strings.HasPrefix(fileName, renameSrcPath) {
			delete(nameNode.FileNameToBlocks, fileName)
			fileName = strings.Replace(fileName, renameSrcPath, renameDestPath, 1)
			nameNode.FileNameToBlocks[fileName] = Blocks
		}
	}
	for fileName, FileSize := range nameNode.FileNameSize {
		if strings.HasPrefix(fileName, renameSrcPath) {
			delete(nameNode.FileNameSize, fileName)
			fileName = strings.Replace(fileName, renameSrcPath, renameDestPath, 1)
			nameNode.FileNameSize[fileName] = FileSize
		}
	}
	return nil
}

type NameNodeReNameFileRequest struct {
	ReNameSrcFileName  string
	ReNameDestFileName string
}

func (nameNode *Service) ReNameFile(request *NameNodeReNameFileRequest, reply *[]util.DataNodeInstance) error {
	ReNameSrcFileName := request.ReNameSrcFileName
	ReNameDestFileName := request.ReNameDestFileName
	Blocks := nameNode.FileNameToBlocks[ReNameSrcFileName]
	delete(nameNode.FileNameToBlocks, ReNameSrcFileName)
	nameNode.FileNameToBlocks[ReNameDestFileName] = Blocks
	FileSize := nameNode.FileNameSize[ReNameSrcFileName]
	delete(nameNode.FileNameSize, ReNameSrcFileName)
	nameNode.FileNameSize[ReNameDestFileName] = FileSize
	return nil
}

type NameNodeListRequest struct {
	RemoteDirPath string
}

func (nameNode *Service) List(request *NameNodeListRequest, reply *[]ListMetaData) error {
	RemoteDirPath := request.RemoteDirPath
	for fileName, FileSize := range nameNode.FileNameSize {
		if strings.HasPrefix(fileName, RemoteDirPath) {
			*reply = append(*reply, ListMetaData{FileName: fileName, FileSize: FileSize})
		}
	}
	return nil
}

func (nameNode *Service) ReDistributeData(request *ReDistributeDataRequest, reply *bool) error {
	log.Printf("DataNode %s is dead, trying to redistribute data\n", request.DataNodeUri)
	deadDataNodeSlice := strings.Split(request.DataNodeUri, ":")
	var deadDataNodeId uint64

	// de-register the dead DataNode from IdToDataNodes meta
	for id, dn := range nameNode.IdToDataNodes {
		if dn.Host == deadDataNodeSlice[0] && dn.ServicePort == deadDataNodeSlice[1] {
			deadDataNodeId = id
			break
		}
	}
	delete(nameNode.IdToDataNodes, deadDataNodeId)

	// construct under-replicated blocks list and
	// de-register the block entirely in favour of re-creation
	var underReplicatedBlocksList []UnderReplicatedBlocks
	for blockId, dnIds := range nameNode.BlockToDataNodeIds {
		for i, dnId := range dnIds {
			if dnId == deadDataNodeId {
				healthyDataNodeId := nameNode.BlockToDataNodeIds[blockId][(i+1)%len(dnIds)]
				underReplicatedBlocksList = append(
					underReplicatedBlocksList,
					UnderReplicatedBlocks{blockId, healthyDataNodeId},
				)
				delete(nameNode.BlockToDataNodeIds, blockId)
				// TODO: trigger data deletion on the existing data nodes
				break
			}
		}
	}

	// verify if re-replication would be possible
	if len(nameNode.IdToDataNodes) < int(nameNode.ReplicationFactor) {
		log.Println("Replication not possible due to unavailability of sufficient DataNode(s)")
		return nil
	}

	var availableNodes []uint64
	for k, _ := range nameNode.IdToDataNodes {
		availableNodes = append(availableNodes, k)
	}

	// attempt re-replication of under-replicated blocks
	for _, blockToReplicate := range underReplicatedBlocksList {
		var remoteFilePath string
		flag := false
		for filename, fileblockIds := range nameNode.FileNameToBlocks {
			for _, id := range fileblockIds {
				if blockToReplicate.BlockId == id {
					flag = true
					break
				}
			}
			if flag {
				split := strings.Split(filename, "/")
				for i := 0; i < len(split)-1; i++ {
					remoteFilePath += split[i] + "/"
				}
				break
			}
		}
		// fetch the data from the healthy DataNode
		healthyDataNode := nameNode.IdToDataNodes[blockToReplicate.HealthyDataNodeId]
		dataNodeInstance, rpcErr := rpc.Dial("tcp", healthyDataNode.Host+":"+healthyDataNode.ServicePort)
		if rpcErr != nil {
			continue
		}

		defer dataNodeInstance.Close()

		getRequest := datanode.DataNodeGetRequest{
			RemoteFilePath: remoteFilePath,
			BlockId:        blockToReplicate.BlockId,
		}
		var getReply datanode.DataNodeData

		rpcErr = dataNodeInstance.Call("Service.GetData", getRequest, &getReply)
		util.Check(rpcErr)
		blockContents := getReply.Data

		// initiate the replication of the block contents
		targetDataNodeIds := nameNode.assignDataNodes(blockToReplicate.BlockId, availableNodes, nameNode.ReplicationFactor)
		var blockAddresses []util.DataNodeInstance
		for _, dataNodeId := range targetDataNodeIds {
			blockAddresses = append(blockAddresses, nameNode.IdToDataNodes[dataNodeId])
		}
		startingDataNode := blockAddresses[0]
		remainingDataNodes := blockAddresses[1:]

		targetDataNodeInstance, rpcErr := rpc.Dial("tcp", startingDataNode.Host+":"+startingDataNode.ServicePort)
		util.Check(rpcErr)
		defer targetDataNodeInstance.Close()

		putRequest := datanode.DataNodePutRequest{
			RemoteFilePath:   remoteFilePath,
			BlockId:          blockToReplicate.BlockId,
			Data:             blockContents,
			ReplicationNodes: remainingDataNodes,
		}
		var putReply datanode.DataNodeWriteStatus

		rpcErr = targetDataNodeInstance.Call("Service.PutData", putRequest, &putReply)
		util.Check(rpcErr)

		log.Printf("Block %s replication completed for %+v\n", blockToReplicate.BlockId, targetDataNodeIds)
	}

	return nil
}
