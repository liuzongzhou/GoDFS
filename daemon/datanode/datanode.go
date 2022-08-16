package datanode

import (
	"errors"
	"github.com/liuzongzhou/GoDFS/datanode"
	"github.com/liuzongzhou/GoDFS/util"
	"log"
	"net"
	"net/rpc"
	"strconv"
)

func InitializeDataNodeUtil(serverPort int, dataLocation string) {
	dataNodeInstance := new(datanode.Service)
	dataNodeInstance.DataDirectory = dataLocation
	dataNodeInstance.ServicePort = uint16(serverPort)

	log.Printf("Data storage location is %s\n", dataLocation)

	err := rpc.Register(dataNodeInstance)
	util.Check(err)

	rpc.HandleHTTP()

	var listener net.Listener
	initErr := errors.New("init")

	for initErr != nil {
		listener, initErr = net.Listen("tcp", ":"+strconv.Itoa(serverPort))
		serverPort += 1
	}
	log.Printf("DataNode port is %d\n", serverPort-1)
	defer listener.Close()

	rpc.Accept(listener)

	log.Println("DataNode daemon started on port: " + strconv.Itoa(serverPort))
}
