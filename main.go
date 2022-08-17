package main

import (
	"flag"
	"github.com/liuzongzhou/GoDFS/daemon/client"
	"github.com/liuzongzhou/GoDFS/daemon/datanode"
	"github.com/liuzongzhou/GoDFS/daemon/namenode"
	"log"
	"os"
	"strings"
)

func main() {
	dataNodeCommand := flag.NewFlagSet("datanode", flag.ExitOnError)
	nameNodeCommand := flag.NewFlagSet("namenode", flag.ExitOnError)
	clientCommand := flag.NewFlagSet("client", flag.ExitOnError)

	dataNodePortPtr := dataNodeCommand.Int("port", 7000, "DataNode communication port")
	dataNodeDataLocationPtr := dataNodeCommand.String("data-location", ".", "DataNode data storage location")

	nameNodePortPtr := nameNodeCommand.Int("port", 9000, "NameNode communication port")
	nameNodeListPtr := nameNodeCommand.String("datanodes", "", "Comma-separated list of DataNodes to connect to")
	nameNodeBlockSizePtr := nameNodeCommand.Int("block-size", 32, "Block size to store")
	nameNodeReplicationFactorPtr := nameNodeCommand.Int("replication-factor", 1, "Replication factor of the system")

	clientNameNodePortPtr := clientCommand.String("namenode", "localhost:9000", "NameNode communication port")
	clientOperationPtr := clientCommand.String("operation", "", "Operation to perform")
	clientSourcePathPtr := clientCommand.String("source-path", "", "Source path of the file")
	clientFilenamePtr := clientCommand.String("filename", "", "File name")
	clientRemotefilepath := clientCommand.String("remotefilepath", "", "Remote_file_path")
	clientLocalfilepath := clientCommand.String("localfilepath", "", "Local_file_path")
	renameSrcPath := clientCommand.String("rename_src_name", "", "rename_src_name")
	renameDestPath := clientCommand.String("rename_dest_name", "", "rename_dest_name")
	remoteDirPath := clientCommand.String("remote_dir_path", "", "remote_dir_path")
	if len(os.Args) < 2 {
		log.Println("sub-command is required")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "datanode":
		_ = dataNodeCommand.Parse(os.Args[2:])
		datanode.InitializeDataNodeUtil(*dataNodePortPtr, *dataNodeDataLocationPtr)

	case "namenode":
		_ = nameNodeCommand.Parse(os.Args[2:])
		var listOfDataNodes []string
		if len(*nameNodeListPtr) > 1 {
			listOfDataNodes = strings.Split(*nameNodeListPtr, ",")
		} else {
			listOfDataNodes = []string{}
		}
		namenode.InitializeNameNodeUtil(*nameNodePortPtr, *nameNodeBlockSizePtr, *nameNodeReplicationFactorPtr, listOfDataNodes)

	case "client":
		_ = clientCommand.Parse(os.Args[2:])

		if *clientOperationPtr == "put" {
			status := client.PutHandler(*clientNameNodePortPtr, *clientSourcePathPtr, *clientFilenamePtr, *clientRemotefilepath)
			log.Printf("Put status: %t\n", status)

		} else if *clientOperationPtr == "get" {
			getHandler := client.GetHandler(*clientNameNodePortPtr, *clientRemotefilepath, *clientFilenamePtr, *clientLocalfilepath)
			log.Printf("Get status: %t\n", getHandler)

		} else if *clientOperationPtr == "mkdir" {
			mkdirHandler := client.MkdirHandler(*clientNameNodePortPtr, *clientRemotefilepath)
			log.Println(mkdirHandler)
		} else if *clientOperationPtr == "stat" {
			filename, filesize := client.Stat(*clientNameNodePortPtr, *clientRemotefilepath, *clientFilenamePtr)
			if filename == "" {
				log.Printf("%v :文件不存在\n", *clientFilenamePtr)
			} else {
				log.Printf("文件名:%v\t文件大小:%v bytes\n", filename, filesize)
			}
		} else if *clientOperationPtr == "rename" {
			var status bool
			if strings.HasSuffix(*renameSrcPath, "/") {
				status = client.ReNameHandler(*clientNameNodePortPtr, *renameSrcPath, *renameDestPath)
			} else {
				status = client.ReNameFileHandler(*clientNameNodePortPtr, *renameSrcPath, *renameDestPath)
			}
			log.Printf("ReName status: %t\n", status)
		} else if *clientOperationPtr == "list" {
			fileInfo := client.ListHandler(*clientNameNodePortPtr, *remoteDirPath)
			for fileName, fileSize := range fileInfo {
				log.Printf("文件名:%v\t文件大小:%v bytes\n", fileName, fileSize)
			}
		} else if *clientOperationPtr == "deletepath" {
			status := client.DeletePathHandler(*clientNameNodePortPtr, *clientRemotefilepath)
			log.Printf("DeletePath status: %t\n", status)
		} else if *clientOperationPtr == "deletefile" {
			status := client.DeleteFileHandler(*clientNameNodePortPtr, *clientRemotefilepath, *clientFilenamePtr)
			log.Printf("DeleteFile status: %t\n", status)
		}
	}
}
