package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Node struct {
	*maelstrom.Node
}

var (
	linKV *maelstrom.KV
	seqKV *maelstrom.KV
)

type sendRequest struct {
	Type string `json:"type"`
	Key  string `json:"key"`
	Msg  any    `json:"msg"`
}

type sendResponse struct {
	Type   string `json:"type"`
	Offset int64  `json:"offset"`
}

type pollRequest struct {
	Type    string           `json:"type"`
	Offsets map[string]int64 `json:"offsets"`
}

type pollResponse struct {
	Type string             `json:"type"`
	Msgs map[string][][]any `json:"msgs"`
}

type commitOffsetsRequest struct {
	Type    string           `json:"type"`
	Offsets map[string]int64 `json:"offsets"`
}

type commitOffsetsResponse struct {
	Type string `json:"type"`
}

type listCommittedOffsetsRequest struct {
	Type string   `json:"type"`
	Keys []string `json:"keys"`
}

type listCommittedOffsetsResponse struct {
	Type    string           `json:"type"`
	Offsets map[string]int64 `json:"offsets"`
}

func toInt64(val any) int64 {
	switch v := val.(type) {
	case float64:
		return int64(v)
	case int:
		return int64(v)
	case int64:
		return v
	default:
		return 0
	}
}

func (n *Node) handleSend(msg maelstrom.Message) error {
	var body sendRequest
	var offset int64
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	for {
		keyStr := fmt.Sprintf("counter_%v", body.Key)
		val, err := linKV.Read(context.Background(), keyStr)
		if err != nil {
			if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
				if err := linKV.CompareAndSwap(context.Background(), keyStr, 0, 1, true); err == nil {
					offset = 0
					break
				}
			}
			time.Sleep(5 * time.Millisecond)
			continue
		}

		offset = toInt64(val)
		if err := linKV.CompareAndSwap(context.Background(), keyStr, offset, offset+1, true); err != nil {
			continue
		}
		break
	}

	msgKey := fmt.Sprintf("%s_%d", body.Key, offset)
	for {
		if err := linKV.Write(context.Background(), msgKey, body.Msg); err == nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	resp := sendResponse{
		Type:   "send_ok",
		Offset: offset,
	}
	return n.Reply(msg, resp)
}

func (n *Node) handlePoll(msg maelstrom.Message) error {
	var body pollRequest
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	msgs := make(map[string][][]any)
	for key, reqOffset := range body.Offsets {
		//val, err := linKV.Read(context.Background(), "counter_"+key)
		//if err != nil {
		//	continue
		//}
		//counter := toInt64(val)
		var batch [][]any
		for i := reqOffset; i <= reqOffset; i++ {
			msgKey := fmt.Sprintf("%s_%d", key, i)
			msgVal, err := linKV.Read(context.Background(), msgKey)
			if err == nil {
				batch = append(batch, []any{i, msgVal})
			}
		}

		if len(batch) > 0 {
			msgs[key] = batch
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

	for key, reqOffset := range body.Offsets {
		commitKey := "commit_" + key
		for {
			val, err := seqKV.Read(context.Background(), commitKey)
			if err != nil {
				if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
					if err := seqKV.CompareAndSwap(context.Background(), commitKey, 0, reqOffset, true); err == nil {
						break
					}
				}
				continue
			}

			current := toInt64(val)
			if reqOffset <= current {
				break
			}

			if err := seqKV.CompareAndSwap(context.Background(), commitKey, current, reqOffset, true); err == nil {
				break
			}
		}
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
	msgs := listCommittedOffsetsResponse{
		Type:    "list_committed_offsets_ok",
		Offsets: make(map[string]int64),
	}
	for _, key := range body.Keys {
		val, err := seqKV.Read(context.Background(), "commit_"+key)
		if err != nil {
			continue
		}
		offset := toInt64(val)
		msgs.Offsets[key] = offset
	}
	return n.Reply(msg, msgs)
}

func main() {
	n := &Node{
		Node: maelstrom.NewNode(),
	}

	linKV = maelstrom.NewLinKV(n.Node)
	seqKV = maelstrom.NewSeqKV(n.Node)

	n.Handle("send", n.handleSend)
	n.Handle("poll", n.handlePoll)
	n.Handle("commit_offsets", n.handleCommitOffsets)
	n.Handle("list_committed_offsets", n.handleListCommittedOffsets)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
