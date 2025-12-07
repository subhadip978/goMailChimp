package main

import "sync"

type Recipient struct {
	Name  string
	Email string
}

func main() {

	recipientChannel := make(chan Recipient)
	// read csv file
	// producer read the mail and generate data

	go func() {

		loadRecipient("./user.csv", recipientChannel)
	}()

	var wg sync.WaitGroup
	workerCount := 5

	for i := 1; i <= workerCount; i++ {
		wg.Add(1)
		go emailWorker(i, recipientChannel, &wg)
	}
	wg.Wait()

	// 1. we remain blocked in this step as we know in unbuffer channel sender and consumer must need to ready
	// 2. we never teach to emailworker
}
