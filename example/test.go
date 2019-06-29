package main

import (
	"time"
	"fmt"
)

func main() {
	var task int = 1
	c := make(chan int)

	go func() {
		for {
			select {
			case c <- task:
				fmt.Println("task int")
				goto end1
			case <-time.After(time.Second * 1):
				fmt.Println("timeout")
			}
		}
		end1:
			fmt.Println("end")
	}()


	time.Sleep(time.Second * 2)

	t := <-c
	fmt.Println(t)
	time.Sleep(time.Microsecond * 10)
}
