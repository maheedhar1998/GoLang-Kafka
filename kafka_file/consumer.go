package main

import (
	"bufio"
	"fmt"
	"os"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"time"
)

func getFromKafka(cons *kafka.Consumer) (string, bool) {
	fmt.Println("Reading From Kafka....")
	tm, tmErr := time.ParseDuration("10s")
	fmt.Println(tmErr)
	line, err := cons.ReadMessage(tm)
	fmt.Printf("%v, %T, %v, %T", line, line, err, err)
	if err == nil {
		return string(line.Value), true
	} else {
		fmt.Println(err)
		return "", false
	}
}

func printToFile(fileWriter *bufio.Writer, line string) bool {
	len, err := fileWriter.WriteString(line)
	if err != nil {
		fmt.Println("Couldn't write to file", err)
		return false
	} else {
		fmt.Printf("Wrote %v bytes to file\n", len)
		return true
	}
}

func main() {
	file, err := os.OpenFile("copiedData.tsv", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	// file, err = os.Open("copiedData.tsv")
	c, kafErr := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"auto.offset.reset": "earliest",
		"group.id":          "myGroup",
	})
	if err != nil {
		fmt.Println("File doesn't exist or can't be created")
	}
	if kafErr != nil {
		fmt.Println("Consumer wasn't initialized")
	}
	fileWriter := bufio.NewWriter(file)
	c.SubscribeTopics([]string{"file-transfer"}, nil)
	defer file.Close()
	defer c.Close()
	for {
		line, success := getFromKafka(c)
		// fmt.Printf("%v\n", success)
		if success {
			done := printToFile(fileWriter, line)
			// fmt.Printf("%v %v\n", success, done)
			if done {
				fmt.Println("Wrote to File", line[:10])
			} else if !done {
				break
			}
		} else {
			fileWriter.Flush()
			break
		}
	}
	fileWriter.Flush()
}
