package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync/atomic"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var (
	node_id string
	counter atomic.Uint64
)

type Node struct {
	*maelstrom.Node
}

func (n *Node) handle_init(msg maelstrom.Message) error {
	var body map[string]any
	var ok bool
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	if node_id, ok = body["node_id"].(string); !ok {
		log.Fatalf("failed to extract node_id from init message: %v", body)
	}
	return nil
}

func (n *Node) handle_generate(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	id := fmt.Sprintf("[%v]-[%v]", node_id, counter.Add(1))
	body["id"] = id
	body["type"] = "generate_ok"
	return n.Reply(msg, body)
}

func main() {
	n := Node{maelstrom.NewNode()}
	n.Handle("init", n.handle_init)
	n.Handle("generate", n.handle_generate)
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
