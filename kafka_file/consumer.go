package main

import (
	"fmt";
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka";
	"bufio";
	"os";
)

func getFromKafka(cons *kafka.Consumer) (string, bool) {
	cons.SubscribeTopics([]string{"file-transfer"}, nil)
	line, err := cons.ReadMessage(-1)
	// fmt.Printf("%v, %T, %v, %T", line, line, err, err)
	if err == nil {
		return string(line.Value), true
	} else {
		fmt.Println(err)
		return "", false
	}
}

func printToFile(f *os.File, line string) bool {
	fileWriter := bufio.NewWriter(f)
	len, err := fileWriter.WriteString(line)
	if err != nil {
		fmt.Println("Couldn't write to file")
		return false
	} else {
		fmt.Printf("Wrote %v bytes to file\n", len)
		return true
	}
}

func main() {
	file, err := os.OpenFile("copiedData.tsv", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	file, err = os.Open("copiedData.tsv")
	c, kafErr := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"auto.offset.reset": "earliest",
		"group.id": "myGroup",
	})
	if err != nil {
		fmt.Println("File doesn't exist or can't be created")
	}
	if kafErr != nil {
		fmt.Println("Consumer wasn't initialized")
	}
	defer file.Close()
	defer c.Close()
	for {
		line, success := getFromKafka(c)
		if success {
			done := printToFile(file, line)
			if done {
				fmt.Println("Wrote to File",line)
			} else {
				break
			}
		}
	}
}