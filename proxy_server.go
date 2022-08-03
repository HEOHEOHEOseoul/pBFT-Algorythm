package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

type Server struct {
	url  string
	node *Node
}

func NewServer(nodeID string) *Server {
	node := NewNode(nodeID)
	server := &Server{node.NodeAddressTable[nodeID], node}

	server.setRoute()

	return server
}

func (server *Server) Start() {
	fmt.Printf("Server will be started at %s...\n", server.url)
	if err := http.ListenAndServe(server.url, nil); err != nil {
		fmt.Println(err)
		return
	}
}

func (server *Server) setRoute() {
	http.HandleFunc("/req", server.getReq) //Leader Node
	http.HandleFunc("/preprepare", server.getPrePrepare)
	http.HandleFunc("/prepare", server.getPrepare)
	http.HandleFunc("/commit", server.getCommit)
	http.HandleFunc("/reply", server.getReply)    //Leader Node ==> Http Server 주소변경
	http.HandleFunc("/addNode", server.addNode)   //노드어드레드테이블에 추가 요청 경로 - 리더노드만 받음
	http.HandleFunc("/getTable", server.getTable) //리더노드가 각각 노드에게 nodeaddresstable 줄 때 쓰는 경로 - 리더 제외 나머지가 받음
}

func (server *Server) getReq(writer http.ResponseWriter, request *http.Request) {

	var msg RequestMsg
	err := json.NewDecoder(request.Body).Decode(&msg)
	if err != nil {
		fmt.Println(err)
		return
	}

	if server.node.CurrentState != nil {
		fmt.Println("@@@@@@@@@@@@@@@@@@@@@@@")
		qq.Enqueue(&msg)
	} else {
		server.node.CurrentState = nil
		server.node.MsgEntrance <- &msg
	}

}

// 리더노드가 새로 업데이트한 nodeaddresstable 넘겨줄 떄 실행되는 함수
func (server *Server) getTable(writer http.ResponseWriter, request *http.Request) {
	var data map[string]string
	json.NewDecoder(request.Body).Decode(&data)
	fmt.Println("Node Has been Sucessfully Registereds")
	server.node.NodeAddressTable = data
}

// 새로운 노드가 리더노드에게 nodeaddresstable 추가 요청 함수
func (server *Server) addNode(writer http.ResponseWriter, request *http.Request) {
	var newNodeAddress Add

	err := json.NewDecoder(request.Body).Decode(&newNodeAddress)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(&newNodeAddress)

	server.node.NodeAddressTable[newNodeAddress.Port] =
		newNodeAddress.Ip + ":" + newNodeAddress.Port

	doc, _ := json.Marshal(server.node.NodeAddressTable)
	server.node.NewNodeAlarm <- doc
	// func() {
	// 	for nodeID, _ := range server.node.NodeAddressTable {
	// 		if nodeID == server.node.NodeID {
	// 			continue
	// 		}
	// 		buff := bytes.NewBuffer(doc)
	// 		time.Sleep(time.Second * 3)
	// 		http.Post("http://192.168.10.35:3000/hi", "application/json", buff)
	// 		fmt.Println("asdfasdfasdf")
	// 	}
	// }()
	// server.node.Broadcast(server.node.NodeAddressTable, "/hi")

}

func (server *Server) getPrePrepare(writer http.ResponseWriter, request *http.Request) {

	server.node.CurrentState = nil

	var msg PrePrepareMsg
	err := json.NewDecoder(request.Body).Decode(&msg)
	if err != nil {
		fmt.Println(err)
		return
	}
	go func() {
		server.node.MsgEntrance <- &msg
	}()
}

func (server *Server) getPrepare(writer http.ResponseWriter, request *http.Request) {
	var msg VoteMsg
	err := json.NewDecoder(request.Body).Decode(&msg)
	if err != nil {
		fmt.Println(err)
		return
	}
	go func() {

		server.node.MsgEntrance <- &msg
	}()
}

func (server *Server) getCommit(writer http.ResponseWriter, request *http.Request) {
	var msg VoteMsg
	err := json.NewDecoder(request.Body).Decode(&msg)
	if err != nil {
		fmt.Println(err)
		return
	}
	go func() {

		server.node.MsgEntrance <- &msg
	}()
}

func (server *Server) getReply(writer http.ResponseWriter, request *http.Request) {
	var msg ReplyMsg
	err := json.NewDecoder(request.Body).Decode(&msg)
	if err != nil {
		fmt.Println(err)
		return
	}

	server.node.GetReply(&msg)
}

func send(url string, msg []byte) {
	buff := bytes.NewBuffer(msg)
	http.Post("http://"+url, "application/json", buff)

}
