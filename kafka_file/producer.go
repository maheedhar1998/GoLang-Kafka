package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	// "bytes"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func getNextBlock(fileReader *bufio.Reader) ([]byte, bool) {
	line, err := fileReader.Peek(4096)
	leng, err1 := fileReader.Read(line)
	// buff := bytes.NewBuffer(line)
	// fmt.Printf("%v %v\n", err, err1)
	if err == nil && err != io.EOF  && err1 == nil{
		fmt.Printf("Sending %v Bytes....\n", leng)
	} else if err != nil && err == io.EOF {
		fmt.Println("End of File Reached")
		return line, true
	}
	return line, false
}

func sendToKafka(prod *kafka.Producer, line []byte) {
	topic := "file-transfer"
	prod.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          line,
	}, nil)
	fmt.Printf("Sent %v Bytes\n", len(line))
	prod.Flush(10)
}

func main() {
	// fmt.Println("Running")
	file, err := os.Open("data.tsv")
	p, kafErr := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		fmt.Println("File doesn't exist in the given directory")
	}
	if kafErr != nil {
		panic(err)
	}
	fileReader := bufio.NewReader(file)
	defer p.Close()
	defer file.Close()
	for {
		line, eof := getNextBlock(fileReader)
		if !eof {
			// fmt.Println(string(line))
			sendToKafka(p, line)
		} else {
			sendToKafka(p, line)
			break
		}
	}
}
