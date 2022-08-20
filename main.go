package main

import (
	"flag"
	"fmt"
	"github.com/liuzongzhou/GoDFS/daemon/client"
	"github.com/liuzongzhou/GoDFS/daemon/datanode"
	"github.com/liuzongzhou/GoDFS/daemon/namenode"
	"os"
	"strings"
)

func main() {
	// 启动命令的标志区分位：datanode，namenode，client
	dataNodeCommand := flag.NewFlagSet("datanode", flag.ExitOnError)
	nameNodeCommand := flag.NewFlagSet("namenode", flag.ExitOnError)
	clientCommand := flag.NewFlagSet("client", flag.ExitOnError)
	//dataNode相关参数：端口，根目录
	dataNodePortPtr := dataNodeCommand.Int("port", 7000, "DataNode communication port")
	dataNodeDataLocationPtr := dataNodeCommand.String("data-location", ".", "DataNode data storage location")
	//nameNode相关参数：地址，端口，根目录，管理的dataNodes列表，文件块大小，备份总数（主+备），当前主端口
	nameNodeHostPtr := nameNodeCommand.String("host", "localhost", "NameNode communication host")
	nameNodePortPtr := nameNodeCommand.Int("port", 9000, "NameNode communication port")
	nameNodeListPtr := nameNodeCommand.String("datanodes", "", "Comma-separated list of DataNodes to connect to")
	nameNodeBlockSizePtr := nameNodeCommand.Int("block-size", 32, "Block size to store")
	nameNodeReplicationFactorPtr := nameNodeCommand.Int("replication-factor", 1, "Replication factor of the system")
	nameNodePrimaryPortPtr := nameNodeCommand.String("primary-port", "9000", "Primary NameNode port")
	//client相关参数：通过哪个nameNode端口操作，操作行为分类，本地文件路径，文件名，远端文件路径，下载文件路径，重命名原始路径，重命名目标路径，list目标路径
	clientNameNodePortPtr := clientCommand.String("namenode", "localhost:9000", "NameNode communication port")
	clientOperationPtr := clientCommand.String("operation", "", "Operation to perform")
	clientSourcePathPtr := clientCommand.String("source-path", "", "Source path of the file")
	clientFilenamePtr := clientCommand.String("filename", "", "File name")
	clientRemotefilepath := clientCommand.String("remotefilepath", "", "Remote_file_path")
	clientLocalfilepath := clientCommand.String("localfilepath", "", "Local_file_path")
	renameSrcPath := clientCommand.String("rename_src_name", "", "rename_src_name")
	renameDestPath := clientCommand.String("rename_dest_name", "", "rename_dest_name")
	remoteDirPath := clientCommand.String("remote_dir_path", "", "remote_dir_path")

	//判断命令参数的传入，至少要2个参数，不然非法
	if len(os.Args) < 2 {
		fmt.Println("==> sub-command is required")
		os.Exit(1)
	}
	//匹配第一个参数，区分是哪种命令
	switch os.Args[1] {
	case "datanode":
		_ = dataNodeCommand.Parse(os.Args[2:])
		//建立dataNode节点进程，当不指定端口时默认7000，当端口被占用，自动+1，直到有空的端口可以被使用
		datanode.InitializeDataNodeUtil(*dataNodePortPtr, *dataNodeDataLocationPtr)

	case "namenode":
		_ = nameNodeCommand.Parse(os.Args[2:])
		var listOfDataNodes []string
		//判断是否有输入dataNodes list,如果没有，则自己进行网络发现，然后加入管理
		if len(*nameNodeListPtr) > 1 {
			listOfDataNodes = strings.Split(*nameNodeListPtr, ",")
		} else {
			listOfDataNodes = []string{}

		}
		namenode.InitializeNameNodeUtil(*nameNodePrimaryPortPtr, *nameNodeHostPtr, *nameNodePortPtr, *nameNodeBlockSizePtr, *nameNodeReplicationFactorPtr, listOfDataNodes)

	case "client":
		_ = clientCommand.Parse(os.Args[2:])
		//上传文件，返回操作结果
		if *clientOperationPtr == "put" {
			status := client.PutHandler(*clientNameNodePortPtr, *clientSourcePathPtr, *clientFilenamePtr, *clientRemotefilepath)
			fmt.Printf("==> Put status: %t\n", status)
			//下载文件，返回操作结果
		} else if *clientOperationPtr == "get" {
			getHandler := client.GetHandler(*clientNameNodePortPtr, *clientRemotefilepath, *clientFilenamePtr, *clientLocalfilepath)
			fmt.Printf("==> Get status: %t\n", getHandler)
			//创建远端目录，返回操作结果
		} else if *clientOperationPtr == "mkdir" {
			mkdirHandler := client.MkdirHandler(*clientNameNodePortPtr, *clientRemotefilepath)
			fmt.Printf("==> Mkdir status: %t\n", mkdirHandler)
			// 查看文件元数据：文件名+文件大小，并打印输出，如不存在打印
		} else if *clientOperationPtr == "stat" {
			filename, filesize := client.StatHandler(*clientNameNodePortPtr, *clientRemotefilepath, *clientFilenamePtr)
			if filename == "" {
				fmt.Printf("==> %v :File does not exist\n", *clientFilenamePtr)
			} else {
				fmt.Printf("==> FileName:%v\tFileSize:%v bytes\n", filename, filesize)
			}
			// 重命名文件目录或者文件名都可以，返回操作结果
		} else if *clientOperationPtr == "rename" {
			var status bool
			if strings.HasSuffix(*renameSrcPath, "/") {
				status = client.ReNameHandler(*clientNameNodePortPtr, *renameSrcPath, *renameDestPath)
			} else {
				status = client.ReNameFileHandler(*clientNameNodePortPtr, *renameSrcPath, *renameDestPath)
			}
			fmt.Printf("==> ReName status: %t\n", status)
			//打印指定目录下的文件元数据信息：文件名+文件大小，空目录则显示为空
		} else if *clientOperationPtr == "list" {
			fileInfo := client.ListHandler(*clientNameNodePortPtr, *remoteDirPath)
			for fileName, fileSize := range fileInfo {
				fmt.Printf("==> FileName:%v\tFileSize:%v bytes\n", fileName, fileSize)
			}
			//删除路径以及路径下所有子文件和文件夹，返回操作结果
		} else if *clientOperationPtr == "deletepath" {
			status := client.DeletePathHandler(*clientNameNodePortPtr, *clientRemotefilepath)
			fmt.Printf("==> DeletePath status: %t\n", status)
			//删除指定路径下的指定文件，返回操作结果
		} else if *clientOperationPtr == "deletefile" {
			status := client.DeleteFileHandler(*clientNameNodePortPtr, *clientRemotefilepath, *clientFilenamePtr)
			fmt.Printf("==> DeleteFile status: %t\n", status)
		}
	}
}
