package main

import (
	"encoding/json"
	"log"
	"maps"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type inflight struct {
	msg      []int
	sentTime time.Time
}

type neighborInfo struct {
	NodeId       string    `json:"node_id"`
	MaxAdd       int       `json:"max_add"`
	lastSentTime time.Time `json:"-"`
}

type Node struct {
	*maelstrom.Node
	mu        sync.RWMutex
	nodeId    string
	neighbors map[string]*neighborInfo
	maxAdd    map[string]int
}

type gossipMsg struct {
	Type          string                   `json:"type"`
	NeighborsInfo map[string]*neighborInfo `json:"neighbor_info"`
}

type gossipRes struct {
	Type       string `json:"type"`
	FromNodeID string `json:"from_node_id"`
}

type initMsg struct {
	Type    string   `json:"type"`
	MsgID   int      `json:"msg_id"`
	NodeID  string   `json:"node_id"`
	NodeIDs []string `json:"node_ids"`
}

// var kv *maelstrom.KV

var timeout time.Duration = 30 * time.Millisecond

func (n *Node) handleGossip(msg maelstrom.Message) error {
	// in case this node receives a gossip before it receives an init
	if n.nodeId == "" {
		return nil
	}
	var body gossipMsg
	var res gossipRes
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	n.mu.Lock()
	for id, m := range body.NeighborsInfo {
		n.neighbors[id].MaxAdd = max(n.neighbors[id].MaxAdd, m.MaxAdd)
	}
	n.mu.Unlock()
	res.Type = "gossip_ack"
	res.FromNodeID = n.nodeId
	return n.Reply(msg, res)
}

func (n *Node) handleGossipAck(msg maelstrom.Message) error {
	return nil
}

func (n *Node) gossipLoop() {
	for {
		n.mu.Lock()
		for id, neighbor := range n.neighbors {
			if time.Since(neighbor.lastSentTime) < timeout {
				continue
			}
			msgs := maps.Clone(n.neighbors)
			n.Send(neighbor.NodeId, gossipMsg{Type: "gossip", NeighborsInfo: msgs})
			n.neighbors[id].lastSentTime = time.Now()
		}
		n.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (n *Node) handleRead(msg maelstrom.Message) error {
	var body map[string]any
	var err error
	if err = json.Unmarshal(msg.Body, &body); err != nil {
		log.Printf("[!] handleRead: %v\n", err)
		return err
	}
	//body["value"], err = kv.Read(context.Background(), "counter")
	//if err != nil {
	//	log.Printf("[!] handleRead: %v\n", err)
	//	return err
	//}
	counter := 0
	for _, neighbor := range n.neighbors {
		counter += neighbor.MaxAdd
	}
	body["value"] = counter
	body["type"] = "read_ok"
	return n.Reply(msg, body)
}

func (n *Node) handleAdd(msg maelstrom.Message) error {
	var (
		body map[string]any
		err  error
		// tmp          any
		// counterVal   = 0
		// retryTimeout = 50 * time.Millisecond
	)

	if err = json.Unmarshal(msg.Body, &body); err != nil {
		log.Printf("[!] handleAdd: %v\n", err)
		return err
	}

	delta := int(body["delta"].(float64))
	n.neighbors[n.nodeId].MaxAdd += delta
	clear(body)
	body["type"] = "add_ok"
	return n.Reply(msg, body)
	// for {

	//	tmp, err = kv.Read(context.Background(), "counter")
	//	if err != nil {
	//		time.Sleep(retryTimeout)
	//		log.Printf("[!] handleAdd: %v\n", err)
	//		return err
	//	}
	//	counterVal = tmp.(int)
	//	log.Printf("[!] handleAdd: %v\n", err)
	//	if err = kv.CompareAndSwap(context.Background(), "counter", counterVal, counterVal+int(body["delta"].(float64)), true); err != nil {
	//		time.Sleep(retryTimeout)
	//		continue
	//	}
	//	clear(body) // to remove the "delta" field in our response
	//	body["type"] = "add_ok"
	//	return n.Reply(msg, body)
	//}
}

func (n *Node) handleInit(msg maelstrom.Message) error {
	var body initMsg
	var err error
	if err = json.Unmarshal(msg.Body, &body); err != nil {
		log.Printf("[!] handleRead: %v\n", err)
		return err
	}
	n.nodeId = body.NodeID
	n.maxAdd[body.NodeID] = 0
	for _, nodeId := range body.NodeIDs {
		n.maxAdd[nodeId] = 0
		n.neighbors[nodeId] = &neighborInfo{
			NodeId:       nodeId,
			MaxAdd:       0,
			lastSentTime: time.Now(),
		}
	}

	res := make(map[string]any)
	res["type"] = "init_ok"
	return n.Reply(msg, res)
}

func main() {
	n := Node{
		Node:      maelstrom.NewNode(),
		maxAdd:    make(map[string]int),
		nodeId:    "",
		neighbors: make(map[string]*neighborInfo),
	}
	n.Handle("init", n.handleInit)
	n.Handle("add", n.handleAdd)
	n.Handle("read", n.handleRead)
	n.Handle("gossip", n.handleGossip)
	n.Handle("gossip_ack", n.handleGossipAck)
	go func() {
		for n.nodeId == "" {
		}
		n.gossipLoop()
	}()
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
