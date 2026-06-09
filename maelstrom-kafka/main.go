package main

import (
	"encoding/json"
	"log"
	"slices"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Node struct {
	*maelstrom.Node
	committed map[string]int
	logs      map[string][]any
	counter   map[string]int
}

type sendRequest struct {
	Type string `json:"type"`
	Key  string `json:"key"`
	Msg  any    `json:"msg"`
}

type sendResponse struct {
	Type   string `json:"type"`
	Offset int    `json:"offset"`
}

type pollRequest struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

type pollResponse struct {
	Type string           `json:"type"`
	Msgs map[string][]any `json:"msgs"`
}

type commitOffsetsRequest struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

type commitOffsetsResponse struct {
	Type string `json:"type"`
}

type listCommittedOffsetsRequest struct {
	Type string   `json:"type"`
	Keys []string `json:"keys"`
}

type listCommittedOffsetsResponse struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

var mu sync.Mutex

func (n *Node) handleSend(msg maelstrom.Message) error {
	var body sendRequest
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	mu.Lock()
	defer mu.Unlock()
	n.logs[body.Key] = append(n.logs[body.Key], body.Msg)
	resp := sendResponse{
		Type:   "send_ok",
		Offset: n.counter[body.Key] - 1,
	}
	n.counter[body.Key]++
	return n.Reply(msg, resp)
}

func (n *Node) handlePoll(msg maelstrom.Message) error {
	var body pollRequest
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	mu.Lock()
	defer mu.Unlock()

	msgs := make(map[string][]any)
	for key, offset := range body.Offsets {
		if _, ok := n.logs[key]; ok { // ignore unknown keys
			if offset < n.committed[key] || offset > n.counter[key] {
				continue
			}
			msgs[key] = slices.Clone(msgs[key])
		}
	}
	return n.Reply(msg, pollResponse{
		Type: "poll_ok",
		Msgs: msgs,
	})
}

func (n *Node) handleCommitOffsets(msg maelstrom.Message) error {
	var body commitOffsetsRequest
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	return n.Reply(msg, commitOffsetsResponse{
		Type: "commit_offsets_ok",
	})
}

func (n *Node) handleListCommittedOffsets(msg maelstrom.Message) error {
	var body listCommittedOffsetsRequest
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	return n.Reply(msg, listCommittedOffsetsResponse{
		Type:    "list_committed_offsets_ok",
		Offsets: make(map[string]int),
	})
}

func main() {
	n := &Node{
		Node:      maelstrom.NewNode(),
		committed: make(map[string]int),
		logs:      make(map[string][]any),
		counter:   make(map[string]int),
	}

	n.Handle("send", n.handleSend)
	n.Handle("poll", n.handlePoll)
	n.Handle("commit_offsets", n.handleCommitOffsets)
	n.Handle("list_committed_offsets", n.handleListCommittedOffsets)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
