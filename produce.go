package main

import (
	"encoding/csv"

	"os"
)

func loadRecipient(filePath string, ch chan Recipient) error {
	defer close(ch)
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	r := csv.NewReader(f)
	records, err := r.ReadAll()
	if err != nil {
		return err
	}

	for _, record := range records[1:] {
		ch <- Recipient{
			Name:  record[0],
			Email: record[1],
		}
		// above line is block untill any reciver ready to recieve it (unbuffer channel)
		// send -> consumer

	}
	return nil
}
