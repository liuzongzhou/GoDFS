package namenode

import (
	"github.com/liuzongzhou/GoDFS/datanode"
	"github.com/liuzongzhou/GoDFS/util"
	"log"
	"testing"
)

// TestNameNodeCreation 创建一个NameNode服务
func TestNameNodeCreation(t *testing.T) {
	testNameNodeService := Service{
		PrimaryPort:         "9000",
		BlockSize:           4,
		ReplicationFactor:   2,
		FileNameToBlocks:    make(map[string][]string),
		IdToDataNodes:       make(map[uint64]datanode.DataNodeInstance),
		BlockToDataNodeIds:  make(map[string][]uint64),
		FileNameSize:        make(map[string]uint64),
		DirectoryToFileName: make(map[string][]string),
	}

	testDataNodeInstance1 := datanode.DataNodeInstance{Host: "localhost", ServicePort: "1234"}
	testDataNodeInstance2 := datanode.DataNodeInstance{Host: "localhost", ServicePort: "4321"}
	testNameNodeService.IdToDataNodes[0] = testDataNodeInstance1
	testNameNodeService.IdToDataNodes[1] = testDataNodeInstance2

	if len(testNameNodeService.IdToDataNodes) != 2 || testNameNodeService.BlockSize != 4 || testNameNodeService.ReplicationFactor != 2 {
		t.Errorf("Unable to initialize NameNode correctly")
	}
}

// TestNameNodeServiceWrite 测试写入数据
func TestNameNodeServiceWrite(t *testing.T) {
	testNameNodeService := Service{
		PrimaryPort:         "9000",
		BlockSize:           4,
		ReplicationFactor:   2,
		FileNameToBlocks:    make(map[string][]string),
		IdToDataNodes:       make(map[uint64]datanode.DataNodeInstance),
		BlockToDataNodeIds:  make(map[string][]uint64),
		FileNameSize:        make(map[string]uint64),
		DirectoryToFileName: make(map[string][]string),
	}

	testDataNodeInstance1 := datanode.DataNodeInstance{Host: "localhost", ServicePort: "1234"}
	testDataNodeInstance2 := datanode.DataNodeInstance{Host: "localhost", ServicePort: "4321"}
	testNameNodeService.IdToDataNodes[0] = testDataNodeInstance1
	testNameNodeService.IdToDataNodes[1] = testDataNodeInstance2

	writeDataPayload := NameNodeWriteRequest{
		RemoteFilePath: "Test1/",
		FileName:       "foo",
		FileSize:       12,
	}

	var reply []NameNodeMetaData
	err := testNameNodeService.WriteData(&writeDataPayload, &reply)
	log.Println(reply)
	util.Check(err)
	if len(reply) != 3 {
		t.Errorf("Unable to set metadata correctly")
	}
}

// TestNameNodeServiceGetIdToDataNodes 测试获取当前存活的datanode元数据组信息：host+port
func TestNameNodeServiceGetIdToDataNodes(t *testing.T) {
	testNameNodeService := Service{
		PrimaryPort:         "9000",
		BlockSize:           4,
		ReplicationFactor:   2,
		FileNameToBlocks:    make(map[string][]string),
		IdToDataNodes:       make(map[uint64]datanode.DataNodeInstance),
		BlockToDataNodeIds:  make(map[string][]uint64),
		FileNameSize:        make(map[string]uint64),
		DirectoryToFileName: make(map[string][]string),
	}

	testDataNodeInstance1 := datanode.DataNodeInstance{Host: "localhost", ServicePort: "1234"}
	testDataNodeInstance2 := datanode.DataNodeInstance{Host: "localhost", ServicePort: "4321"}
	testNameNodeService.IdToDataNodes[0] = testDataNodeInstance1
	testNameNodeService.IdToDataNodes[1] = testDataNodeInstance2

	var reply []datanode.DataNodeInstance
	var request = true
	err := testNameNodeService.GetIdToDataNodes(&request, &reply)
	log.Println(reply)
	util.Check(err)
	if len(reply) != 2 {
		t.Errorf("Unable to get dataNodes")
	}
}

// TestNameNodeServiceGetIdToDataNodes 测试获取主nameNode节点的元数据信息
func TestNameNodeServiceReplicationnameNode(t *testing.T) {
	testNameNodeService := Service{
		PrimaryPort:         "9000",
		BlockSize:           4,
		ReplicationFactor:   2,
		FileNameToBlocks:    make(map[string][]string),
		IdToDataNodes:       make(map[uint64]datanode.DataNodeInstance),
		BlockToDataNodeIds:  make(map[string][]uint64),
		FileNameSize:        make(map[string]uint64),
		DirectoryToFileName: make(map[string][]string),
	}

	testDataNodeInstance1 := datanode.DataNodeInstance{Host: "localhost", ServicePort: "1234"}
	testDataNodeInstance2 := datanode.DataNodeInstance{Host: "localhost", ServicePort: "4321"}
	testNameNodeService.IdToDataNodes[0] = testDataNodeInstance1
	testNameNodeService.IdToDataNodes[1] = testDataNodeInstance2

	var reply Service
	var request = true
	err := testNameNodeService.ReplicationnameNode(&request, &reply)
	log.Println(reply)
	util.Check(err)
	if len(testNameNodeService.IdToDataNodes) != 2 || testNameNodeService.BlockSize != 4 || testNameNodeService.ReplicationFactor != 2 {
		t.Errorf("Unable to get primary namenode information")
	}
}

// TestNameNodeServiceselectRandomNumbers 测试随机选择存储节点，尽量做到负载均衡
func TestNameNodeServiceselectRandomNumbers(t *testing.T) {
	var dataNodesAvailable []uint64
	var replicationFactor uint64
	dataNodesAvailable = []uint64{1, 2, 3, 4}
	replicationFactor = 2

	numbers := selectRandomNumbers(dataNodesAvailable, replicationFactor)
	log.Println(numbers)
	if numbers[0] == numbers[1] {
		t.Errorf("Unable to get random numbers")
	}
}

// TestNameNodeServiceReadData 测试读取文件对应的元数据数组
func TestNameNodeServiceReadData(t *testing.T) {
	testNameNodeService := Service{
		PrimaryPort:         "9000",
		BlockSize:           4,
		ReplicationFactor:   2,
		FileNameToBlocks:    make(map[string][]string),
		IdToDataNodes:       make(map[uint64]datanode.DataNodeInstance),
		BlockToDataNodeIds:  make(map[string][]uint64),
		FileNameSize:        make(map[string]uint64),
		DirectoryToFileName: make(map[string][]string),
	}

	testDataNodeInstance1 := datanode.DataNodeInstance{Host: "localhost", ServicePort: "1234"}
	testDataNodeInstance2 := datanode.DataNodeInstance{Host: "localhost", ServicePort: "4321"}
	testNameNodeService.IdToDataNodes[0] = testDataNodeInstance1
	testNameNodeService.IdToDataNodes[1] = testDataNodeInstance2
	testNameNodeService.BlockToDataNodeIds["0"] = []uint64{0}
	testNameNodeService.BlockToDataNodeIds["1"] = []uint64{1}
	testNameNodeService.FileNameToBlocks["/Test1/foo"] = []string{"0", "1"}

	request := NameNodeReadRequest{
		FileName: "/Test1/foo",
	}

	var reply []NameNodeMetaData
	err := testNameNodeService.ReadData(&request, &reply)
	log.Println(reply)
	util.Check(err)
	if len(reply) != 2 {
		t.Errorf("Unable to read metadata correctly")
	}
}

// TestNameNodeServiceList
func TestNameNodeServiceList(t *testing.T) {
	testNameNodeService := Service{
		PrimaryPort:         "9000",
		BlockSize:           4,
		ReplicationFactor:   2,
		FileNameToBlocks:    make(map[string][]string),
		IdToDataNodes:       make(map[uint64]datanode.DataNodeInstance),
		BlockToDataNodeIds:  make(map[string][]uint64),
		FileNameSize:        make(map[string]uint64),
		DirectoryToFileName: make(map[string][]string),
	}

	testDataNodeInstance1 := datanode.DataNodeInstance{Host: "localhost", ServicePort: "1234"}
	testDataNodeInstance2 := datanode.DataNodeInstance{Host: "localhost", ServicePort: "4321"}
	testNameNodeService.IdToDataNodes[0] = testDataNodeInstance1
	testNameNodeService.IdToDataNodes[1] = testDataNodeInstance2
	testNameNodeService.BlockToDataNodeIds["0"] = []uint64{0}
	testNameNodeService.BlockToDataNodeIds["1"] = []uint64{1}
	testNameNodeService.FileNameToBlocks["/Test1/foo"] = []string{"0", "1"}
	testNameNodeService.DirectoryToFileName["/Test1/"] = []string{"foo"}
	testNameNodeService.FileNameSize["/Test1/foo"] = 10

	request := NameNodeListRequest{RemoteDirPath: "/Test1/"}
	var reply []ListMetaData
	// 在List过程中，我们只需要操作文件元数据，所以只需要调用NameNode的List方法即可
	err := testNameNodeService.List(&request, &reply)
	// 返回的数据通过Map形式返回
	for _, listMetaData := range reply {
		log.Println(listMetaData)
	}
	util.Check(err)
}

// TestNameNodeServiceReNameFile 重命名文件的测试
func TestNameNodeServiceReNameFile(t *testing.T) {
	testNameNodeService := Service{
		PrimaryPort:         "9000",
		BlockSize:           4,
		ReplicationFactor:   2,
		FileNameToBlocks:    make(map[string][]string),
		IdToDataNodes:       make(map[uint64]datanode.DataNodeInstance),
		BlockToDataNodeIds:  make(map[string][]uint64),
		FileNameSize:        make(map[string]uint64),
		DirectoryToFileName: make(map[string][]string),
	}

	testDataNodeInstance1 := datanode.DataNodeInstance{Host: "localhost", ServicePort: "1234"}
	testDataNodeInstance2 := datanode.DataNodeInstance{Host: "localhost", ServicePort: "4321"}
	testNameNodeService.IdToDataNodes[0] = testDataNodeInstance1
	testNameNodeService.IdToDataNodes[1] = testDataNodeInstance2
	testNameNodeService.BlockToDataNodeIds["0"] = []uint64{0}
	testNameNodeService.BlockToDataNodeIds["1"] = []uint64{1}
	testNameNodeService.FileNameToBlocks["/Test1/foo"] = []string{"0", "1"}
	testNameNodeService.DirectoryToFileName["/Test1/"] = []string{"foo"}
	testNameNodeService.FileNameSize["/Test1/foo"] = 10

	request := NameNodeReNameFileRequest{ReNameSrcFileName: "/Test1/foo", ReNameDestFileName: "/Test1/too"}
	var reply bool
	// rpc调用NameNode的ReName方法，传入重命名的ReNameSrcFileName和ReNameDestFileName
	// 修改NameNode的元数据信息并返回修改是否成功
	err := testNameNodeService.ReNameFile(&request, &reply)
	util.Check(err)
}

// TestNameNodeServiceReName 重命名路径的测试
func TestNameNodeServiceReName(t *testing.T) {
	testNameNodeService := Service{
		PrimaryPort:         "9000",
		BlockSize:           4,
		ReplicationFactor:   2,
		FileNameToBlocks:    make(map[string][]string),
		IdToDataNodes:       make(map[uint64]datanode.DataNodeInstance),
		BlockToDataNodeIds:  make(map[string][]uint64),
		FileNameSize:        make(map[string]uint64),
		DirectoryToFileName: make(map[string][]string),
	}

	testDataNodeInstance1 := datanode.DataNodeInstance{Host: "localhost", ServicePort: "1234"}
	testDataNodeInstance2 := datanode.DataNodeInstance{Host: "localhost", ServicePort: "4321"}
	testNameNodeService.IdToDataNodes[0] = testDataNodeInstance1
	testNameNodeService.IdToDataNodes[1] = testDataNodeInstance2
	testNameNodeService.BlockToDataNodeIds["0"] = []uint64{0}
	testNameNodeService.BlockToDataNodeIds["1"] = []uint64{1}
	testNameNodeService.FileNameToBlocks["/Test1/foo"] = []string{"0", "1"}
	testNameNodeService.DirectoryToFileName["/Test1/"] = []string{"foo"}
	testNameNodeService.FileNameSize["/Test1/foo"] = 10

	NameNodeRequest := NameNodeReNameRequest{ReNameSrcPath: "/Test1/", ReNameDestPath: "/Test2/"}
	var reply []datanode.DataNodeInstance
	// rpc调用NameNode的ReName方法，传入重命名的ReNameSrcFileName和ReNameDestFileName
	// 修改NameNode的元数据信息并返回修改是否成功
	err := testNameNodeService.ReName(&NameNodeRequest, &reply)
	util.Check(err)
}

// TestNameNodeServiceFileSize Stat的测试
func TestNameNodeServiceFileSize(t *testing.T) {
	testNameNodeService := Service{
		PrimaryPort:         "9000",
		BlockSize:           4,
		ReplicationFactor:   2,
		FileNameToBlocks:    make(map[string][]string),
		IdToDataNodes:       make(map[uint64]datanode.DataNodeInstance),
		BlockToDataNodeIds:  make(map[string][]uint64),
		FileNameSize:        make(map[string]uint64),
		DirectoryToFileName: make(map[string][]string),
	}

	testDataNodeInstance1 := datanode.DataNodeInstance{Host: "localhost", ServicePort: "1234"}
	testDataNodeInstance2 := datanode.DataNodeInstance{Host: "localhost", ServicePort: "4321"}
	testNameNodeService.IdToDataNodes[0] = testDataNodeInstance1
	testNameNodeService.IdToDataNodes[1] = testDataNodeInstance2
	testNameNodeService.BlockToDataNodeIds["0"] = []uint64{0}
	testNameNodeService.BlockToDataNodeIds["1"] = []uint64{1}
	testNameNodeService.FileNameToBlocks["/Test1/foo"] = []string{"0", "1"}
	testNameNodeService.DirectoryToFileName["/Test1/"] = []string{"foo"}
	testNameNodeService.FileNameSize["/Test1/foo"] = 10

	request := NameNodeReadRequest{FileName: "/Test1/foo"}
	var reply NameNodeFileSize
	err := testNameNodeService.FileSize(&request, &reply)
	if err == nil {
		log.Println(reply.FileSize)
		return
	}
	util.Check(err)
}

// TestNameNodeServiceDeleteFile DeleteFile的测试
func TestNameNodeServiceDeleteFile(t *testing.T) {
	testNameNodeService := Service{
		PrimaryPort:         "9000",
		BlockSize:           4,
		ReplicationFactor:   2,
		FileNameToBlocks:    make(map[string][]string),
		IdToDataNodes:       make(map[uint64]datanode.DataNodeInstance),
		BlockToDataNodeIds:  make(map[string][]uint64),
		FileNameSize:        make(map[string]uint64),
		DirectoryToFileName: make(map[string][]string),
	}

	testDataNodeInstance1 := datanode.DataNodeInstance{Host: "localhost", ServicePort: "1234"}
	testDataNodeInstance2 := datanode.DataNodeInstance{Host: "localhost", ServicePort: "4321"}
	testNameNodeService.IdToDataNodes[0] = testDataNodeInstance1
	testNameNodeService.IdToDataNodes[1] = testDataNodeInstance2
	testNameNodeService.BlockToDataNodeIds["0"] = []uint64{0}
	testNameNodeService.BlockToDataNodeIds["1"] = []uint64{1}
	testNameNodeService.FileNameToBlocks["/Test1/foo"] = []string{"0", "1"}
	testNameNodeService.DirectoryToFileName["/Test1/"] = []string{"foo"}
	testNameNodeService.FileNameSize["/Test1/foo"] = 10

	var reply bool
	var request = NameNodeDeleteRequest{RemoteFilePath: "/Test1/", FileName: "foo"}
	err := testNameNodeService.DeleteFileNameMetaData(&request, &reply)
	util.Check(err)
}

// TestNameNodeServiceDeletePath DeletePath的测试
func TestNameNodeServiceDeletePath(t *testing.T) {
	testNameNodeService := Service{
		PrimaryPort:         "9000",
		BlockSize:           4,
		ReplicationFactor:   2,
		FileNameToBlocks:    make(map[string][]string),
		IdToDataNodes:       make(map[uint64]datanode.DataNodeInstance),
		BlockToDataNodeIds:  make(map[string][]uint64),
		FileNameSize:        make(map[string]uint64),
		DirectoryToFileName: make(map[string][]string),
	}

	testDataNodeInstance1 := datanode.DataNodeInstance{Host: "localhost", ServicePort: "1234"}
	testDataNodeInstance2 := datanode.DataNodeInstance{Host: "localhost", ServicePort: "4321"}
	testNameNodeService.IdToDataNodes[0] = testDataNodeInstance1
	testNameNodeService.IdToDataNodes[1] = testDataNodeInstance2
	testNameNodeService.BlockToDataNodeIds["0"] = []uint64{0}
	testNameNodeService.BlockToDataNodeIds["1"] = []uint64{1}
	testNameNodeService.FileNameToBlocks["/Test1/foo"] = []string{"0", "1"}
	testNameNodeService.DirectoryToFileName["/Test1/"] = []string{"foo"}
	testNameNodeService.FileNameSize["/Test1/foo"] = 10

	var reply bool
	var request = NameNodeDeleteRequest{RemoteFilePath: "/Test1/"}
	//rpc 调用DeleteMetaData方法，删除相关元数据信息
	err := testNameNodeService.DeleteMetaData(&request, &reply)
	util.Check(err)
}
