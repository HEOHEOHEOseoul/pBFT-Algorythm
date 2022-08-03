package main

import (
	"fmt"
	"os"
)

func main() {
	// runtime.GOMAXPROCS(runtime.NumCPU())
	nodeID := os.Args[1] /// cmd] pbft 5000 [enter]
	server := NewServer(nodeID)

	server.Start()
	defer func() {
		s := recover()
		fmt.Println(s)
	}()
}
