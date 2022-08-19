package datanode

import (
	"strings"
	"testing"
)

// TestDataNodeServiceCreation 测试能否创建DataNode Service服务
func TestDataNodeServiceCreation(t *testing.T) {
	testDataNodeService := new(Service)
	testDataNodeService.DataDirectory = "./"
	testDataNodeService.ServicePort = 8000

	if testDataNodeService.DataDirectory != "./" {
		t.Error("Unable to set DataDirectory correctly")
	}
	if testDataNodeService.ServicePort != 8000 {
		t.Errorf("Unable to set ServicePort correctly")
	}
}

// TestDataNodeServiceWrite 测试能否写入数据到DataNode 节点
func TestDataNodeServiceWrite(t *testing.T) {
	testDataNodeService := new(Service)
	testDataNodeService.DataDirectory = "./"
	testDataNodeService.ServicePort = 8000
	request := DataNodePutRequest{RemoteFilePath: "Test/",
		BlockId: "1", Data: "Hello world", ReplicationNodes: nil}
	var reply DataNodeReplyStatus
	testDataNodeService.PutData(&request, &reply)

	if !reply.Status {
		t.Error("Unable to write data correctly")
	}
}

// TestDataNodeServiceRead 测试能否下载数据
func TestDataNodeServiceRead(t *testing.T) {
	testDataNodeService := new(Service)
	testDataNodeService.DataDirectory = "./"
	testDataNodeService.ServicePort = 8000

	request := DataNodeGetRequest{BlockId: "1", RemoteFilePath: "Test"}
	var replyPayload DataNodeData
	testDataNodeService.GetData(&request, &replyPayload)

	if strings.Compare(replyPayload.Data, "Hello world") != 0 {
		t.Error("Unable to read data correctly")
	}
}

// TestDataNodeServiceMkdir 测试能否创建目录
func TestDataNodeServiceMkdir(t *testing.T) {
	testDataNodeService := new(Service)
	testDataNodeService.DataDirectory = "./"
	testDataNodeService.ServicePort = 8000
	var reply DataNodeReplyStatus
	request := "Test/"
	testDataNodeService.MakeDir(request, &reply)
	if !reply.Status {
		t.Error("Unable to make directory")
	}
}

// TestDataNodeServiceDeletePath 测试能否删除路径
func TestDataNodeServiceDeletePath(t *testing.T) {
	testDataNodeService := new(Service)
	testDataNodeService.DataDirectory = "./"
	testDataNodeService.ServicePort = 8000
	var reply DataNodeReplyStatus
	request := DataNodeDeleteRequest{RemoteFilepath: "Test"}
	testDataNodeService.DeletePath(&request, &reply)
	if !reply.Status {
		t.Error("Unable to delete path")
	}
}

// TestDataNodeServiceDeleteFile 测试能否删除文件
func TestDataNodeServiceDeleteFile(t *testing.T) {
	testDataNodeService := new(Service)
	testDataNodeService.DataDirectory = "./"
	testDataNodeService.ServicePort = 8000
	var reply DataNodeReplyStatus
	request := DataNodeDeleteRequest{RemoteFilepath: "Test/", BlockId: "1"}
	testDataNodeService.DeleteFile(&request, &reply)
	if !reply.Status {
		t.Error("Unable to delete file")
	}
}

// TestDataNodeServiceReNameDir
func TestDataNodeServiceReNameDir(t *testing.T) {
	testDataNodeService := new(Service)
	testDataNodeService.DataDirectory = "./"
	testDataNodeService.ServicePort = 8000
	var reply DataNodeReplyStatus
	request := DataNodeReNameRequest{ReNameSrcPath: "Test/", ReNameDestPath: "Test1/"}
	testDataNodeService.ReNameDir(&request, &reply)
	if !reply.Status {
		t.Error("Unable to rename directory or filename")
	}
}
