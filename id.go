package pubsub

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
	pstore "github.com/libp2p/go-libp2p-core/peerstore"
	kb "github.com/libp2p/go-libp2p-kbucket"
	"github.com/libp2p/go-libp2p/p2p/protocol/identify"
)

func (n *Node) Id() (string, error) {
	output, err := n.printSelf()
	res, err := json.Marshal(output)
	if err != nil {
		return "", err
	}

	return string(res), nil

}

func (n *Node) PidCheck(pid string) error {

	var err error
	_, err = peer.Decode(pid)
	if err != nil {
		return fmt.Errorf("invalid peer id")
	}
	return nil
}

func (n *Node) PidInfo(pid string) (string, error) {

	var id peer.ID

	var err error
	id, err = peer.Decode(pid)
	if err != nil {
		return "", fmt.Errorf("invalid peer id")
	}

	if pid == n.PeerID {
		output, err := n.printSelf()
		res, err := json.Marshal(output)
		if err != nil {
			return "", err
		}

		return string(res), nil
	}

	output, err := printPeer(n.PeerStore, id)
	if err != nil {
		return "", err
	}
	res, err := json.Marshal(output)
	if err != nil {
		return "", err
	}

	return string(res), nil

}

func (n *Node) IdWithTimeout(pid string, timeout int32) (string, error) {
	dnsTimeout := time.Duration(timeout) * time.Second

	cctx, cancel := context.WithTimeout(context.Background(), dnsTimeout)
	defer cancel()
	var id peer.ID

	var err error
	id, err = peer.Decode(pid)
	if err != nil {
		return "", fmt.Errorf("invalid peer id")
	}

	if pid == n.PeerID {
		output, err := n.printSelf()
		res, err := json.Marshal(output)
		if err != nil {
			return "", err
		}

		return string(res), nil
	}

	p, err := n.Routing.FindPeer(cctx, id)
	if err == kb.ErrLookupFailure {
		return "", errors.New(offlineIdErrorMessage)
	}
	if err != nil {
		return "", err
	}

	output, err := printPeer(n.PeerStore, p.ID)
	if err != nil {
		return "", err
	}
	res, err := json.Marshal(output)
	if err != nil {
		return "", err
	}

	return string(res), nil

}

const offlineIdErrorMessage = `'ipfs id' currently cannot query information on remote
peers without a running daemon; we are working to fix this.`

type IdOutput struct {
	ID              string
	PublicKey       string
	Addresses       []string
	AgentVersion    string
	ProtocolVersion string
}

func printPeer(ps pstore.Peerstore, p peer.ID) (interface{}, error) {
	if p == "" {
		return nil, errors.New("attempted to print nil peer")
	}

	info := new(IdOutput)
	info.ID = p.Pretty()

	if pk := ps.PubKey(p); pk != nil {
		pkbytes, err := pk.Raw()
		if err != nil {
			return nil, err
		}
		info.PublicKey = base64.StdEncoding.EncodeToString(pkbytes)
	}

	for _, a := range ps.Addrs(p) {
		info.Addresses = append(info.Addresses, a.String())
	}

	if v, err := ps.Get(p, "ProtocolVersion"); err == nil {
		if vs, ok := v.(string); ok {
			info.ProtocolVersion = vs
		}
	}
	if v, err := ps.Get(p, "AgentVersion"); err == nil {
		if vs, ok := v.(string); ok {
			info.AgentVersion = vs
		}
	}

	return info, nil
}

// printing self is special cased as we get values differently.
func (n *Node) printSelf() (interface{}, error) {
	info := new(IdOutput)
	info.ID = n.PeerID

	info.PublicKey = n.PublicKey

	if n.Host != nil {
		for _, a := range n.Host.Addrs() {
			info.Addresses = append(info.Addresses, a.String())
		}
	}
	info.ProtocolVersion = identify.LibP2PVersion
	info.AgentVersion = "n.a."
	return info, nil
}
