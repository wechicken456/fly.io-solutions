package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Node struct {
	*maelstrom.Node
	mu sync.RWMutex
}

var kv *maelstrom.KV

func (n *Node) handleRead(msg maelstrom.Message) error {
	var body map[string]any
	var err error
	if err = json.Unmarshal(msg.Body, &body); err != nil {
		log.Printf("[!] handleRead: %v\n", err)
		return err
	}
	body["value"], err = kv.Read(context.Background(), "counter")
	if err != nil {
		log.Printf("[!] handleRead: %v\n", err)
		return err
	}
	body["value"] = body["value"].(int)
	body["type"] = "read_ok"
	return n.Reply(msg, body)
}

func (n *Node) handleAdd(msg maelstrom.Message) error {
	var (
		body          map[string]any
		err           error
		tmp           any
		counter_val   = 0
		retry_timeout = 10 * time.Millisecond
	)

	if err = json.Unmarshal(msg.Body, &body); err != nil {
		log.Printf("[!] handleAdd: %v\n", err)
		return err
	}
	for {

		tmp, err = kv.Read(context.Background(), "counter")
		if err != nil {
			time.Sleep(retry_timeout)
			log.Printf("[!] handleAdd: %v\n", err)
			return err
		}
		counter_val = tmp.(int)
		log.Printf("[!] handleAdd: %v\n", err)
		if err = kv.CompareAndSwap(context.Background(), "counter", counter_val, counter_val+int(body["delta"].(float64)), true); err != nil {
			time.Sleep(retry_timeout)
			continue
		}
		clear(body) // to remove the "delta" field in our response
		body["type"] = "add_ok"
		return n.Reply(msg, body)
	}
}

func main() {
	n := Node{
		Node: maelstrom.NewNode(),
	}
	kv = maelstrom.NewSeqKV(n.Node)
	n.Handle("init", func(msg maelstrom.Message) error {
		return kv.CompareAndSwap(context.Background(), "counter", 0, 0, true)
	})
	n.Handle("add", n.handleAdd)
	n.Handle("read", n.handleRead)
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
