package main

import (
	"fmt"

	ouroboroskv "github.com/i5heu/ouroboros-kv"
)

func main() {
	fmt.Println("Hello, World!")
	ouroboroskv.Init(nil, &ouroboroskv.Config{
		Paths:            []string{"./tmp/ouroboros-kv"},
		MinimumFreeSpace: 10,
		Logger:           nil,
	})
	fmt.Println("Ouroboros KV Initialized")
}
