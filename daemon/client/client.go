package client

import (
	"github.com/liuzongzhou/GoDFS/client"
	"github.com/liuzongzhou/GoDFS/util"
	"log"
	"net"
	"net/rpc"
)

func PutHandler(nameNodeAddress string, sourcePath string, fileName string, remotefilepath string) bool {
	rpcClient, err := initializeClientUtil(nameNodeAddress)
	util.Check(err)
	defer rpcClient.Close()
	return client.Put(rpcClient, sourcePath, fileName, remotefilepath)
}

func GetHandler(nameNodeAddress string, remotefilepath string, fileName string, local_file_path string) bool {
	rpcClient, err := initializeClientUtil(nameNodeAddress)
	util.Check(err)
	defer rpcClient.Close()
	return client.Get(rpcClient, remotefilepath, fileName, local_file_path)
}

func initializeClientUtil(nameNodeAddress string) (*rpc.Client, error) {
	host, port, err := net.SplitHostPort(nameNodeAddress)
	util.Check(err)

	log.Printf("NameNode to connect to is %s\n", nameNodeAddress)
	return rpc.Dial("tcp", host+":"+port)
}

func MkdirHandler(nameNodeAddress string, remote_file_path string) bool {
	rpcClient, err := initializeClientUtil(nameNodeAddress)
	util.Check(err)
	defer rpcClient.Close()
	return client.Mkdir(rpcClient, remote_file_path)
}

func Stat(nameNodeAddress string, remote_file_path string, fileName string) (filename string, filesize uint64) {
	rpcClient, err := initializeClientUtil(nameNodeAddress)
	util.Check(err)
	defer rpcClient.Close()
	return client.Stat(rpcClient, remote_file_path, fileName)
}
