package main

import (
	"fmt";
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka";
	"io";
	"bufio";
	"os";
)

func getNextLine(fileReader *bufio.Reader) ([]byte, bool) {
	line, err := fileReader.ReadBytes(byte('\n'))
	if err != nil && err != io.EOF {
		fmt.Println(line)
	} else if err != nil &&  err == io.EOF {
		fmt.Println("End of File Reached")
		return line, true
	}
	return line, false
}

func sendToKafka(prod *kafka.Producer, line []byte) {
	topic := "file-transfer"
	prod.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value: line,
	}, nil)
	fmt.Println("Sent")
	prod.Flush(1000)
}

func main() {
	fmt.Println("Running")
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
		line, eof := getNextLine(fileReader)
		if(!eof) {
			fmt.Println(string(line))
			sendToKafka(p, line)
		}
	}
}