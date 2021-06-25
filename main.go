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
	// lock := elock.NewMutex(client, "/etcdlock", 2000)
	// lock2 := elock.NewMutex(client, "/etcdlock", 2000)
	// err = lock.Lock(context.Background())
	// if err != nil {
	// 	fmt.Println(err)
	// 	return
	// }
	// lock2.Lock(context.Background())
	// time.Sleep(3 * time.Second)
	// err = lock.Unlock(context.Background())


	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			lock := elock.NewMutex(client, "/etcdlock", 20)
			err := lock.Lock(context.Background())
			if err != nil {
				fmt.Println(err)
				return
			}
			time.Sleep(3 * time.Second)
			err = lock.Unlock(context.Background())
			if err != nil {
				fmt.Println(err)
				return
			}
			wg.Done()
		}()
	}
	wg.Wait()
}
