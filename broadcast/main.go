package main

import (
	"encoding/json"
	"log"
	"slices"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type inflight struct {
	msg       []int
	sent_time time.Time
}

type Node struct {
	*maelstrom.Node
	mu                sync.RWMutex
	node_id           string
	broadcast_msgs    []int
	broadcast_chan    chan int
	done_chan         chan struct{}
	neighbor_ids      []string
	neighbor_inflight map[string]*inflight // messages that have been sent and waiting on ack
	neighbor_pending  map[string][]int     // messages that haven't been sent yet
	seen              map[int]bool
}

type topology_msg struct {
	Type     string              `json:"type"`
	Topology map[string][]string `json:"topology"`
	Msg_id   int                 `json:"msg_id"`
}

type gossip_msg struct {
	Type     string `json:"type"`
	Messages []int  `json:"messages"`
}

type gossip_res struct {
	Type       string `json:"type"`
	FromNodeId string `json:"from_node_id"`
}

type broadcast_req struct {
	Type    string `json:"type"`
	Message int    `json:"message"`
	Msg_id  int    `json:"msg_id"`
}

var timeout time.Duration = 100 * time.Millisecond

func (n *Node) handle_init(msg maelstrom.Message) error {
	var body map[string]any
	var ok bool
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	if n.node_id, ok = body["node_id"].(string); !ok {
		log.Fatalf("failed to extract node_id from init message: %v", body)
	}
	return nil
}

func (n *Node) handle_topology(msg maelstrom.Message) error {
	var body topology_msg
	res := make(map[string]any)
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	for node, neighbors := range body.Topology {
		if node == n.node_id {
			// deep copy of neighbor list
			n.neighbor_ids = slices.Clone(neighbors)
			// initialize empty ack buf for each neighbor
			for _, neighbor := range neighbors {
				n.neighbor_pending[neighbor] = make([]int, 0)
				n.neighbor_inflight[neighbor] = &inflight{
					msg:       make([]int, 0),
					sent_time: time.Now(),
				}
			}
		}
	}
	res["type"] = "topology_ok"
	return n.Reply(msg, res)
}

func (n *Node) handle_broadcast(msg maelstrom.Message) error {
	var body broadcast_req
	res := make(map[string]any)
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	res["type"] = "broadcast_ok"
	n.mu.Lock()
	n.broadcast_msgs = append(n.broadcast_msgs, body.Message)
	if !n.seen[body.Message] {
		n.seen[body.Message] = true
		for neighbor := range n.neighbor_pending {
			n.neighbor_pending[neighbor] = append(n.neighbor_pending[neighbor], body.Message)
		}
	}
	n.mu.Unlock()
	return n.Reply(msg, res)
}

func (n *Node) handle_gossip(msg maelstrom.Message) error {
	var body gossip_msg
	var res gossip_res
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	newMsgs := make([]int, 0)
	n.mu.Lock()
	for _, m := range body.Messages {
		if !n.seen[m] {
			n.seen[m] = true
			newMsgs = append(newMsgs, m)
			n.broadcast_msgs = append(n.broadcast_msgs, m)
		}
	}
	for neighbor := range n.neighbor_pending {
		n.neighbor_pending[neighbor] = append(n.neighbor_pending[neighbor], newMsgs...)
	}
	n.mu.Unlock()
	res.Type = "gossip_ack"
	res.FromNodeId = n.node_id
	return n.Reply(msg, res)
}

func (n *Node) handle_gossip_ack(msg maelstrom.Message) error {
	var body gossip_res
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	n.mu.Lock()
	n.neighbor_inflight[body.FromNodeId] = nil
	n.mu.Unlock()
	return nil
}

func (n *Node) gossipLoop() {
	for {
		n.mu.Lock()
		for _, neighbor := range n.neighbor_ids {
			if inf := n.neighbor_inflight[neighbor]; inf != nil {
				if time.Since(inf.sent_time) < timeout {
					continue
				}
				n.neighbor_pending[neighbor] = append(slices.Clone(inf.msg), n.neighbor_pending[neighbor]...)
				n.neighbor_inflight[neighbor] = nil
			}
			if len(n.neighbor_pending[neighbor]) == 0 {
				continue
			}
			msgs := slices.Clone(n.neighbor_pending[neighbor])
			n.Send(neighbor, gossip_msg{Type: "gossip", Messages: msgs})
			n.neighbor_inflight[neighbor] = &inflight{msg: msgs, sent_time: time.Now()}
			n.neighbor_pending[neighbor] = make([]int, 0)
		}
		n.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (n *Node) handle_read(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	n.mu.RLock()
	msgs := slices.Clone(n.broadcast_msgs)
	n.mu.RUnlock()
	body["type"] = "read_ok"
	body["messages"] = msgs
	return n.Reply(msg, body)
}

func main() {
	n := Node{
		Node:    maelstrom.NewNode(),
		node_id: "", broadcast_chan: make(chan int),
		broadcast_msgs:    make([]int, 0, 5000),
		done_chan:         make(chan struct{}),
		neighbor_ids:      make([]string, 0),
		neighbor_inflight: make(map[string]*inflight),
		neighbor_pending:  make(map[string][]int),
		seen:              make(map[int]bool),
	}
	n.Handle("init", n.handle_init)
	n.Handle("topology", n.handle_topology)
	n.Handle("broadcast", n.handle_broadcast)
	n.Handle("read", n.handle_read)
	n.Handle("gossip", n.handle_gossip)
	n.Handle("gossip_ack", n.handle_gossip_ack)
	go func() {
		time.Sleep(1 * time.Second)
		n.gossipLoop()
	}()
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
