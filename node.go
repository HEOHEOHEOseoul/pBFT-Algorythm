package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"
)

type Node struct {
	NodeID           string
	NodeAddressTable map[string]string // key=nodeID, value=url
	View             *View
	CurrentState     *State
	CommittedMsgs    []*RequestMsg // kinda block.
	MsgBuffer        *MsgBuffer
	MsgEntrance      chan interface{}
	MsgDelivery      chan interface{}
	Alarm            chan bool
	NewNodeAlarm     chan []byte
	Next             chan bool
}

type MsgBuffer struct {
	ReqMsgs        []*RequestMsg
	PrePrepareMsgs []*PrePrepareMsg
	PrepareMsgs    []*VoteMsg
	CommitMsgs     []*VoteMsg
}

type View struct {
	ID      int64
	Primary string
}
type Add struct {
	Ip   string `json:"Ip"`
	Port string `json:"Port"`
}

type Queue []interface{} //큐스택에 쌓기위함

var qq Queue = Queue{}

var freshman string

const ResolvingTimeDuration = time.Millisecond * 1000 // 1 second.

//큐가 비어있는지 확인하는 함수.
func (q *Queue) IsEmpty() bool {
	return len(*q) == 0
}

//큐에 값을 추가하는 함수.
func (q *Queue) Enqueue(data interface{}) {
	*q = append(*q, data) // 큐 끝에 값을 추가함.
	fmt.Printf("Enqueue: %v\n", data)
}

//큐에 첫번째 요소를 반환하고 제거하는 함수.
func (q *Queue) Dequeue() interface{} {
	if q.IsEmpty() {
		fmt.Println("queue is empty")
		return nil
	}
	data := (*q)[0] // 큐에 첫번째 값을 가져옴.
	*q = (*q)[1:]   // 큐에 첫번째 데이터를 제거함.
	fmt.Printf("Dequeue: %v\n", data)
	return data
}

// cmd> exe 5000 [enter] <---- 5000 = nodeID
func NewNode(nodeID string) *Node {
	var viewID int64 = 0 // temporary.
	ip := GetIP()

	node := &Node{
		// Hard-coded for test.
		NodeID: nodeID,
		NodeAddressTable: map[string]string{
			"10000":        "localhost:10000",
			string(nodeID): ip + ":" + nodeID,
		},
		View: &View{
			ID:      viewID,
			Primary: "10000",
		},

		// Consensus-related struct
		CurrentState:  nil,
		CommittedMsgs: make([]*RequestMsg, 0),
		MsgBuffer: &MsgBuffer{
			ReqMsgs:        make([]*RequestMsg, 0),
			PrePrepareMsgs: make([]*PrePrepareMsg, 0),
			PrepareMsgs:    make([]*VoteMsg, 0),
			CommitMsgs:     make([]*VoteMsg, 0),
		},

		// Channels
		MsgEntrance:  make(chan interface{}, 40000),
		MsgDelivery:  make(chan interface{}, 40000),
		NewNodeAlarm: make(chan []byte, 40000),
		Alarm:        make(chan bool),
		Next:         make(chan bool, 40000),
	}

	// Start message dispatcher
	go node.dispatchMsg()

	// Start alarm trigger
	go node.alarmToDispatcher()

	// Start message resolver
	go node.resolveMsg()

	go node.selectNode() //노드 추가 위한 고루틴

	addNodeJson := &Add{Ip: ip, Port: nodeID}
	e, err := json.Marshal(addNodeJson)
	if err != nil {
		fmt.Println(err)

	}

	buff := bytes.NewBuffer(e)
	fmt.Println(buff)
	http.Post("http://192.168.10.35:10000/addNode", "application/json", buff) //리더노드 아이피

	return node
}

//내부망 아이피 가져오는 함수
func GetIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP.String()
}

func (node *Node) Broadcast(msg interface{}, path string) map[string]error {
	errorMap := make(map[string]error)

	for nodeID, url := range node.NodeAddressTable {
		if nodeID == node.NodeID {
			continue
		}

		jsonMsg, err := json.Marshal(msg)
		if err != nil {
			errorMap[nodeID] = err
			continue
		}

		send(url+path, jsonMsg)
	}

	if len(errorMap) == 0 {
		return nil
	} else {
		return errorMap
	}
}

func (node *Node) Reply(msg *ReplyMsg) error {
	// Print all committed messages.
	for _, value := range node.CommittedMsgs {
		fmt.Printf("Committed value: %s, %d, %s, %d", value.ClientID, value.Timestamp, value.Operation, value.SequenceID)
	}
	fmt.Print("\n")

	jsonMsg, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	// Client가 없으므로, 일단 Primary에게 보내는 걸로 처리.
	send(node.NodeAddressTable[node.View.Primary]+"/reply", jsonMsg)
	// node.CurrentState = nil
	// node.CommittedMsgs = make([]*RequestMsg, 0)
	return nil
}

// GetReq can be called when the node's CurrentState is nil.
// Consensus start procedure for the Primary.
func (node *Node) GetReq(reqMsg *RequestMsg) error {
	LogMsg(reqMsg)

	// Create a new state for the new
	err := node.createStateForNewConsensus()
	if err != nil {
		return err
	}

	// Start the consensus process.
	prePrepareMsg, err := node.CurrentState.StartConsensus(reqMsg)
	if err != nil {
		return err
	}

	LogStage(fmt.Sprintf("Consensus Process (ViewID:%d)", node.CurrentState.ViewID), false)

	// Send getPrePrepare message
	if prePrepareMsg != nil {
		node.Broadcast(prePrepareMsg, "/preprepare")
		LogStage("Pre-prepare", true)
	}

	return nil
}

// GetPrePrepare can be called when the node's CurrentState is nil.
// Consensus start procedure for normal participants.
func (node *Node) GetPrePrepare(prePrepareMsg *PrePrepareMsg) error {
	LogMsg(prePrepareMsg)

	// Create a new state for the new
	err := node.createStateForNewConsensus()
	if err != nil {
		return err
	}

	prePareMsg, err := node.CurrentState.PrePrepare(prePrepareMsg)
	if err != nil {
		return err
	}

	if prePareMsg != nil {
		// Attach node ID to the message
		prePareMsg.NodeID = node.NodeID

		LogStage("Pre-prepare", true)
		node.Broadcast(prePareMsg, "/prepare")
		LogStage("Prepare", false)
	}

	return nil
}

func (node *Node) GetPrepare(prepareMsg *VoteMsg) error {
	LogMsg(prepareMsg)

	commitMsg, err := node.CurrentState.Prepare(prepareMsg)
	if err != nil {
		return err
	}

	if commitMsg != nil {
		// Attach node ID to the message
		commitMsg.NodeID = node.NodeID

		LogStage("Prepare", true)
		node.Broadcast(commitMsg, "/commit")
		LogStage("Commit", false)
	}

	return nil
}

func (node *Node) GetCommit(commitMsg *VoteMsg) error {
	LogMsg(commitMsg)

	replyMsg, committedMsg, err := node.CurrentState.Commit(commitMsg)
	if err != nil {
		return err
	}

	if replyMsg != nil {
		if committedMsg == nil {

			return errors.New("committed message is nil, even though the reply message is not nil")
		}

		// Attach node ID to the message
		replyMsg.NodeID = node.NodeID

		// Save the last version of committed messages to node.
		node.CommittedMsgs = append(node.CommittedMsgs, committedMsg)

		LogStage("Commit", true)
		node.Reply(replyMsg)
		LogStage("Reply", true)
		node.CurrentState = nil
		if !qq.IsEmpty() {

			node.Next <- true
		}
		// node.CommittedMsgs = make([]*RequestMsg, 0)
		// node.CurrentState.CurrentStage = 0

	}

	return nil
}

func (node *Node) GetReply(msg *ReplyMsg) {
	fmt.Printf("Result: %s by %s\n", msg.Result, msg.NodeID)

	// node.CurrentState = nil
	// node.CommittedMsgs = make([]*RequestMsg, 0)
}

func (node *Node) createStateForNewConsensus() error {
	// Check if there is an ongoing consensus process.
	if node.CurrentState != nil {
		return errors.New("another consensus is ongoing")
	}

	// Get the last sequence ID
	var lastSequenceID int64
	if len(node.CommittedMsgs) == 0 {
		lastSequenceID = -1
	} else {
		lastSequenceID = node.CommittedMsgs[len(node.CommittedMsgs)-1].SequenceID
	}
	node.View.ID++
	// Create a new state for this new consensus process in the Primary
	node.CurrentState = CreateState(node.View.ID, lastSequenceID)

	LogStage("Create the replica status", true)

	return nil
}

//채널에 데이터 들어왔을 때 처리해줄 함수 - 고루틴으로 실행중
func (node *Node) selectNode() {
	for {
		select {
		case doc := <-node.NewNodeAlarm:
			time.Sleep(time.Millisecond * 100)

			for nodeID, url := range node.NodeAddressTable {
				if nodeID == node.NodeID {
					continue
				}
				buff := bytes.NewBuffer(doc)
				// time.Sleep(time.Millisecond * 200)
				http.Post("http://"+url+"/getTable", "application/json", buff)
				fmt.Println("NodeAddress Table Has been Updated")
			}
			http.Post("http://"+freshman+"/getView", "application/json",
				bytes.NewBuffer([]byte(`{ "view" : "`+fmt.Sprint(node.View.ID)+`" }`)))
		}
	}
}

func (node *Node) dispatchMsg() {
	for {
		select {
		case msg := <-node.MsgEntrance:
			err := node.routeMsg(msg)
			if err != nil {
				fmt.Println(err)
				// TODO: send err to ErrorChannel
			}
		case <-node.Alarm:
			err := node.routeMsgWhenAlarmed()
			if err != nil {
				fmt.Println(err)
				// TODO: send err to ErrorChannel
			}
		case <-node.Next:

			node.MsgEntrance <- qq.Dequeue()
		}
	}
}

func (node *Node) routeMsg(msg interface{}) []error {
	switch msg.(type) {
	case *RequestMsg:
		if node.CurrentState == nil {
			// Copy buffered messages first.
			msgs := make([]*RequestMsg, len(node.MsgBuffer.ReqMsgs))
			copy(msgs, node.MsgBuffer.ReqMsgs)

			// Append a newly arrived message.
			msgs = append(msgs, msg.(*RequestMsg))

			// Empty the buffer.
			node.MsgBuffer.ReqMsgs = make([]*RequestMsg, 0)

			// Send messages.
			go func() {
				node.MsgDelivery <- msgs
			}()
		} else {
			fmt.Println("@req")
			qq.Enqueue(msg)
			node.MsgBuffer.ReqMsgs = append(node.MsgBuffer.ReqMsgs, msg.(*RequestMsg))
		}
	case *PrePrepareMsg:
		if node.CurrentState == nil {
			// Copy buffered messages first.
			msgs := make([]*PrePrepareMsg, len(node.MsgBuffer.PrePrepareMsgs))
			copy(msgs, node.MsgBuffer.PrePrepareMsgs)

			// Append a newly arrived message.
			msgs = append(msgs, msg.(*PrePrepareMsg))

			// Empty the buffer.
			node.MsgBuffer.PrePrepareMsgs = make([]*PrePrepareMsg, 0)

			// Send messages.
			go func() {

				node.MsgDelivery <- msgs
			}()
		} else {
			fmt.Println("@prepre ")
			node.MsgBuffer.PrePrepareMsgs = append(node.MsgBuffer.PrePrepareMsgs, msg.(*PrePrepareMsg))
		}
	case *VoteMsg:
		if msg.(*VoteMsg).MsgType == PrepareMsg {
			if node.CurrentState == nil || node.CurrentState.CurrentStage != PrePrepared {
				fmt.Println("@pre")
				node.MsgBuffer.PrepareMsgs = append(node.MsgBuffer.PrepareMsgs, msg.(*VoteMsg))
			} else {
				// Copy buffered messages first.
				msgs := make([]*VoteMsg, len(node.MsgBuffer.PrepareMsgs))
				copy(msgs, node.MsgBuffer.PrepareMsgs)

				// Append a newly arrived message.
				msgs = append(msgs, msg.(*VoteMsg))

				// Empty the buffer.
				node.MsgBuffer.PrepareMsgs = make([]*VoteMsg, 0)

				// Send messages.
				go func() {

					node.MsgDelivery <- msgs
				}()
			}
		} else if msg.(*VoteMsg).MsgType == CommitMsg {
			if node.CurrentState == nil || node.CurrentState.CurrentStage != Prepared {
				fmt.Println("@commit")
				node.MsgBuffer.CommitMsgs = append(node.MsgBuffer.CommitMsgs, msg.(*VoteMsg))
			} else {
				// Copy buffered messages first.
				msgs := make([]*VoteMsg, len(node.MsgBuffer.CommitMsgs))
				copy(msgs, node.MsgBuffer.CommitMsgs)

				// Append a newly arrived message.
				msgs = append(msgs, msg.(*VoteMsg))

				// Empty the buffer.
				node.MsgBuffer.CommitMsgs = make([]*VoteMsg, 0)

				// Send messages.
				go func() {

					node.MsgDelivery <- msgs
				}()
			}
		}
	}

	return nil
}

func (node *Node) routeMsgWhenAlarmed() []error {
	if node.CurrentState == nil {
		// Check ReqMsgs, send them.
		if len(node.MsgBuffer.ReqMsgs) != 0 {
			msgs := make([]*RequestMsg, len(node.MsgBuffer.ReqMsgs))
			copy(msgs, node.MsgBuffer.ReqMsgs)

			node.MsgDelivery <- msgs
		}

		// Check PrePrepareMsgs, send them.
		if len(node.MsgBuffer.PrePrepareMsgs) != 0 {
			msgs := make([]*PrePrepareMsg, len(node.MsgBuffer.PrePrepareMsgs))
			copy(msgs, node.MsgBuffer.PrePrepareMsgs)

			node.MsgDelivery <- msgs
		}
	} else {
		switch node.CurrentState.CurrentStage {
		case PrePrepared:
			// Check PrepareMsgs, send them.
			if len(node.MsgBuffer.PrepareMsgs) != 0 {
				msgs := make([]*VoteMsg, len(node.MsgBuffer.PrepareMsgs))
				copy(msgs, node.MsgBuffer.PrepareMsgs)

				node.MsgDelivery <- msgs
			}
		case Prepared:
			// Check CommitMsgs, send them.
			if len(node.MsgBuffer.CommitMsgs) != 0 {
				msgs := make([]*VoteMsg, len(node.MsgBuffer.CommitMsgs))
				copy(msgs, node.MsgBuffer.CommitMsgs)

				node.MsgDelivery <- msgs
			}
		}
	}

	return nil
}

func (node *Node) resolveMsg() {
	for {
		// Get buffered messages from the dispatcher.
		msgs := <-node.MsgDelivery
		switch msgs.(type) {
		case []*RequestMsg:
			errs := node.resolveRequestMsg(msgs.([]*RequestMsg))
			if len(errs) != 0 {
				for _, err := range errs {
					fmt.Println(err)
				}
				// TODO: send err to ErrorChannel
			}
		case []*PrePrepareMsg:
			errs := node.resolvePrePrepareMsg(msgs.([]*PrePrepareMsg))
			if len(errs) != 0 {
				for _, err := range errs {
					fmt.Println(err)
				}
				// TODO: send err to ErrorChannel
			}
		case []*VoteMsg:
			voteMsgs := msgs.([]*VoteMsg)
			if len(voteMsgs) == 0 {
				break
			}

			if voteMsgs[0].MsgType == PrepareMsg {
				errs := node.resolvePrepareMsg(voteMsgs)
				if len(errs) != 0 {
					for _, err := range errs {
						fmt.Println(err)
					}
					// TODO: send err to ErrorChannel
				}
			} else if voteMsgs[0].MsgType == CommitMsg {
				errs := node.resolveCommitMsg(voteMsgs)
				if len(errs) != 0 {
					for _, err := range errs {
						fmt.Println(err)
					}
					// TODO: send err to ErrorChannel
				}
			}
		}
	}
}

func (node *Node) alarmToDispatcher() {
	for {
		time.Sleep(ResolvingTimeDuration)
		node.Alarm <- true
	}
}

func (node *Node) resolveRequestMsg(msgs []*RequestMsg) []error {
	errs := make([]error, 0)

	// Resolve messages
	for _, reqMsg := range msgs {
		err := node.GetReq(reqMsg)
		if err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) != 0 {
		return errs
	}

	return nil
}

func (node *Node) resolvePrePrepareMsg(msgs []*PrePrepareMsg) []error {
	errs := make([]error, 0)

	// Resolve messages
	for _, prePrepareMsg := range msgs {
		err := node.GetPrePrepare(prePrepareMsg)
		if err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) != 0 {
		return errs
	}

	return nil
}

func (node *Node) resolvePrepareMsg(msgs []*VoteMsg) []error {
	errs := make([]error, 0)

	// Resolve messages
	for _, prepareMsg := range msgs {
		err := node.GetPrepare(prepareMsg)
		if err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) != 0 {
		return errs
	}

	return nil
}

func (node *Node) resolveCommitMsg(msgs []*VoteMsg) []error {
	errs := make([]error, 0)

	// Resolve messages
	for _, commitMsg := range msgs {
		err := node.GetCommit(commitMsg)
		if err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) != 0 {
		return errs
	}

	return nil
}
