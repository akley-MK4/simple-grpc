package main

import (
	"fmt"
	"github.com/akley-MK4/simple-grpc/example"
	"os"
)

func main() {
	for _, port := range example.GetPortList() {
		go func(inPort int) {
			if err := example.RunBasicServerExample(inPort); err != nil {
				fmt.Printf("Failed to run Basic Server example, %v\n", err)
				os.Exit(1)
			}
		}(port)
	}

	waitChan := make(chan bool)
	<-waitChan
	os.Exit(0)
}
