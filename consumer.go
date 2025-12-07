package main

import (
	"fmt"
	"sync"
)

func emailWorker(id int, ch chan Recipient, wg *sync.WaitGroup) {
	defer wg.Done()

	fmt.Printf("[Worker %d] Started\n", id)
	for recipient := range ch {
		fmt.Printf(
			"[Worker %d] Processing recipient: Name=%s, Email=%s\n",
			id, recipient.Name, recipient.Email,
		)
	}
}
