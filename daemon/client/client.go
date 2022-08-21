package client

import (
	"github.com/liuzongzhou/GoDFS/client"
	"log"
	"net"
	"net/rpc"
)

// initializeClientUtil client与nameNode建立连接，生成一个client操作实例
func initializeClientUtil(nameNodeAddress string) (*rpc.Client, error) {
	//分割nameNodeAddress，得到host, port
	host, port, err := net.SplitHostPort(nameNodeAddress)
	//如果无法获得有效地址，打印错误日志，并返回空
	if err != nil {
		log.Println(err)
		return nil, err
	}
	// 连接失败，可能nameNode节点进程终止了，提示启用备份节点
	rpcClient, err := rpc.Dial("tcp", host+":"+port)
	if err != nil {
		log.Printf("NameNode to connect to is %s fail,please use replication namenode\n", nameNodeAddress)
		return rpcClient, err
	}
	log.Printf("NameNode to connect to is %s success\n", nameNodeAddress)
	return rpcClient, err
}

// PutHandler 给子进程发送put消息,返回是否put成功的结果
func PutHandler(nameNodeAddress string, sourcePath string, fileName string, remoteFilepath string) bool {
	rpcClient, err := initializeClientUtil(nameNodeAddress)
	if err != nil {
		log.Println(err)
		return false
	}
	//client与nameNode建立连接，生成一个client操作实例
	//未获得有效实例，返回false
	defer rpcClient.Close()
	return client.Put(rpcClient, sourcePath, fileName, remoteFilepath)
}

func GetHandler(nameNodeAddress string, remoteFilepath string, fileName string, localFilePath string) bool {
	rpcClient, err := initializeClientUtil(nameNodeAddress)
	if err != nil {
		log.Println(err)
		return false
	}
	defer rpcClient.Close()
	return client.Get(rpcClient, remoteFilepath, fileName, localFilePath)
}

func MkdirHandler(nameNodeAddress string, remoteFilePath string) bool {
	rpcClient, err := initializeClientUtil(nameNodeAddress)
	if err != nil {
		log.Println(err)
		return false
	}
	defer rpcClient.Close()
	return client.Mkdir(rpcClient, remoteFilePath)
}

func StatHandler(nameNodeAddress string, remoteFilePath string, fileName string) (filename string, filesize uint64) {
	rpcClient, err := initializeClientUtil(nameNodeAddress)
	if err != nil {
		log.Println(err)
		return
	}
	defer rpcClient.Close()
	return client.Stat(rpcClient, remoteFilePath, fileName)
}

func ReNameHandler(nameNodeAddress string, renameSrcPath string, renameDestPath string) bool {
	rpcClient, err := initializeClientUtil(nameNodeAddress)
	if err != nil {
		log.Println(err)
		return false
	}
	defer rpcClient.Close()
	return client.ReName(rpcClient, renameSrcPath, renameDestPath)
}

func ReNameFileHandler(nameNodeAddress string, renameSrcPath string, renameDestPath string) bool {
	rpcClient, err := initializeClientUtil(nameNodeAddress)
	if err != nil {
		log.Println(err)
		return false
	}
	defer rpcClient.Close()
	return client.ReNameFile(rpcClient, renameSrcPath, renameDestPath)
}

func ListHandler(nameNodeAddress string, remoteDirPath string) (fileInfo map[string]uint64) {
	rpcClient, err := initializeClientUtil(nameNodeAddress)
	if err != nil {
		log.Println(err)
		return
	}
	defer rpcClient.Close()
	return client.List(rpcClient, remoteDirPath)
}

func DeletePathHandler(nameNodeAddress string, remoteFilePath string) bool {
	rpcClient, err := initializeClientUtil(nameNodeAddress)
	if err != nil {
		log.Println(err)
		return false
	}
	defer rpcClient.Close()
	return client.DeletePath(rpcClient, remoteFilePath)
}
func DeleteFileHandler(nameNodeAddress string, remoteFilePath string, filename string) bool {
	rpcClient, err := initializeClientUtil(nameNodeAddress)
	if err != nil {
		log.Println(err)
		return false
	}
	defer rpcClient.Close()
	return client.DeleteFile(rpcClient, remoteFilePath, filename)
}
