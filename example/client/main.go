package main

import (
	"fmt"
	"github.com/akley-MK4/simple-grpc/example"
	"os"
)

func main() {
	if err := example.RunBasicClientExample(); err != nil {
		fmt.Printf("Failed to run Basic Client example, %v\n", err)
		os.Exit(1)
	}

	os.Exit(0)
}
