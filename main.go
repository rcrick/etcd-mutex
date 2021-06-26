package main

import (
	"context"
	"example/elock"
	"fmt"
	"log"
	"sync"
	"time"

	v3 "go.etcd.io/etcd/client/v3"
)

func main() {
	var wg sync.WaitGroup
	client, err := v3.New(v3.Config{Endpoints: []string{"127.0.0.1:2379"}})
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lock := elock.NewMutex(client, "/etcdlock", 10)
			err := lock.Acquire(context.Background())
			if err != nil {
				fmt.Println(err)
				return
			}
			time.Sleep(3 * time.Second)
			err = lock.Release(context.Background())
			if err != nil {
				fmt.Println(err)
				return
			}
		}()
	}
	wg.Wait()
}
