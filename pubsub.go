package pubsub

import (
	"context"

	"encoding/json"
	"github.com/libp2p/go-libp2p-core/peer"
	b58 "github.com/mr-tron/base58/base58"
	"io"
)

type PubsubPeer interface {
	Peer(peer string)
}

type pubsubMessage struct {
	Topic string
	From  string
	Seqno string
}

func (n *Node) Subscribe(name string) error {
	cctx := context.Background()
	var err error

	topic, err := n.PubSub.Join(name)
	if err != nil {
		return err
	}
	sub, err := topic.Subscribe()
	if err != nil {
		return err
	}

	for {
		msg, err := sub.Next(cctx)
		if err == io.EOF || err == context.Canceled {
			return nil
		} else if err != nil {
			return err
		}

		res, err := json.Marshal(pubsubMessage{
			From:  peer.ID(msg.From).Pretty(),
			Seqno: b58.Encode(msg.Seqno),
			Topic: *msg.Topic,
		})
		if err != nil {
			return err
		}
		n.Listener.Pubsub(string(res), msg.Data)

	}
}

func (n *Node) Publish(topic string, data []byte) error {
	return n.PubSub.Publish(topic, data)

}

func (n *Node) TopicPeers(info PubsubPeer, topic string) error {

	var err error

	peers, err := n.GetPeers(topic)
	if err != nil {
		return err
	}

	for _, peer := range peers {
		info.Peer(peer.Pretty())
	}
	return nil
}

func (n *Node) GetTopics() ([]string, error) {

	return n.PubSub.GetTopics(), nil
}

func (n *Node) GetPeers(topic string) ([]peer.ID, error) {

	return n.PubSub.ListPeers(topic), nil
}
