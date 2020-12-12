package pubsub

import (
	"context"
	"encoding/json"
	coreiface "github.com/ipfs/interface-go-ipfs-core"
	"github.com/libp2p/go-libp2p-core/network"
	inet "github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	pstore "github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/libp2p/go-libp2p-core/protocol"
	swarm "github.com/libp2p/go-libp2p-swarm"
	ma "github.com/multiformats/go-multiaddr"
	"sort"
	"time"
)

type ConnInfos struct {
	Peers []ConnInfo
}

type StreamInfo struct {
	Protocol string
}

type ConnInfo struct {
	Addr      string
	Peer      string
	Latency   int64
	Muxer     string
	Direction inet.Direction
	Streams   []StreamInfo
}

func (n *Node) SwarmPeers() int {
	return len(n.Host.Network().Conns())
}

func (n *Node) SwarmPeer(pid string) (string, error) {
	ctx := context.Background()
	var err error

	conns, err := n.Peers(ctx)
	if err != nil {
		return "", err
	}

	for _, c := range conns {

		if c.ID().Pretty() == pid {
			ci := ConnInfo{
				Addr: c.Address().String(),
				Peer: c.ID().Pretty(),
			}

			lat, err := c.Latency()
			if err != nil {
				return "", err
			}

			ci.Latency = lat.Nanoseconds()

			strs, err := c.Streams()
			if err != nil {
				return "", err
			}

			for _, s := range strs {
				ci.Streams = append(ci.Streams, StreamInfo{Protocol: string(s)})
			}

			res, err := json.Marshal(ci)
			if err != nil {
				return "", err
			}
			return string(res), nil
		}

	}

	return "", nil
}

func (n *Node) IsConnected(pid string) (bool, error) {

	id, err := peer.Decode(pid)
	if err != nil {
		return false, err
	}

	net := n.Host.Network()
	connected := net.Connectedness(id) == network.Connected
	return connected, nil

}

func (n *Node) SwarmDisconnect(addr string) (bool, error) {
	ctx := context.Background()
	var err error

	addrs, err := parseAddresses(addr)
	if err != nil {
		return false, err
	}
	output := make([]string, 0, len(addrs))
	for _, ainfo := range addrs {
		maddrs, err := peer.AddrInfoToP2pAddrs(&ainfo)
		if err != nil {
			return false, err
		}
		// FIXME: This will print:
		//
		//   disconnect QmFoo success
		//   disconnect QmFoo success
		//   ...
		//
		// Once per address specified. However, I'm not sure of
		// a good backwards compat solution. Right now, I'm just
		// preserving the current behavior.
		for _, addr := range maddrs {
			msg := "disconnect " + ainfo.ID.Pretty()
			if err := n.Disconnect(ctx, addr); err != nil {
				msg += " failure: " + err.Error()
			} else {
				msg += " success"
			}
			output = append(output, msg)
		}
	}
	return false, nil
}

func (n *Node) SwarmConnect(addr string, timeout int32) (bool, error) {
	dnsTimeout := time.Duration(timeout) * time.Second

	cctx, cancel := context.WithTimeout(context.Background(), dnsTimeout)
	defer cancel()
	var err error

	pis, err := parseAddresses(addr)
	if err != nil {
		return false, err
	}

	for _, pi := range pis {

		err := n.Connect(cctx, pi)
		if err != nil {
			return false, err
		}
		return true, nil
	}

	return false, nil
}

func parseAddresses(addrs string) ([]peer.AddrInfo, error) {

	maddrs, err := resolve(addrs)

	if err != nil {
		return nil, err
	}

	return peer.AddrInfosFromP2pAddrs(maddrs)
}

func resolve(addrs string) (ma.Multiaddr, error) {
	maddr, err := ma.NewMultiaddr(addrs)
	if err != nil {
		return nil, err
	}

	return maddr, nil
}

type connInfo struct {
	peerstore pstore.Peerstore
	conn      inet.Conn
	dir       inet.Direction

	addr ma.Multiaddr
	peer peer.ID
}

// tag used in the connection manager when explicitly connecting to a peer.
const connectionManagerTag = "user-connect"
const connectionManagerWeight = 100

func (n *Node) Connect(ctx context.Context, pi peer.AddrInfo) error {
	if n.Host == nil {
		return coreiface.ErrOffline
	}

	if swrm, ok := n.Host.Network().(*swarm.Swarm); ok {
		swrm.Backoff().Clear(pi.ID)
	}

	if err := n.Host.Connect(ctx, pi); err != nil {
		return err
	}

	n.Host.ConnManager().TagPeer(pi.ID, connectionManagerTag, connectionManagerWeight)
	n.Host.ConnManager().Protect(pi.ID, connectionManagerTag)
	return nil
}

func (n *Node) Disconnect(ctx context.Context, addr ma.Multiaddr) error {
	if n.Host == nil {
		return coreiface.ErrOffline
	}

	taddr, id := peer.SplitAddr(addr)
	if id == "" {
		return peer.ErrInvalidAddr
	}

	net := n.Host.Network()
	if taddr == nil {
		if net.Connectedness(id) != inet.Connected {
			return coreiface.ErrNotConnected
		}
		if err := net.ClosePeer(id); err != nil {
			return err
		}
		return nil
	}
	for _, conn := range net.ConnsToPeer(id) {
		if !conn.RemoteMultiaddr().Equal(taddr) {
			continue
		}

		return conn.Close()
	}
	return coreiface.ErrConnNotFound
}

func (n *Node) KnownAddrs(context.Context) (map[peer.ID][]ma.Multiaddr, error) {
	if n.Host == nil {
		return nil, coreiface.ErrOffline
	}

	addrs := make(map[peer.ID][]ma.Multiaddr)
	ps := n.Host.Network().Peerstore()
	for _, p := range ps.Peers() {
		addrs[p] = append(addrs[p], ps.Addrs(p)...)
		sort.Slice(addrs[p], func(i, j int) bool {
			return addrs[p][i].String() < addrs[p][j].String()
		})
	}

	return addrs, nil
}

func (n *Node) LocalAddrs(context.Context) ([]ma.Multiaddr, error) {
	if n.Host == nil {
		return nil, coreiface.ErrOffline
	}

	return n.Host.Addrs(), nil
}

func (n *Node) ListenAddrs(context.Context) ([]ma.Multiaddr, error) {
	if n.Host == nil {
		return nil, coreiface.ErrOffline
	}

	return n.Host.Network().InterfaceListenAddresses()
}

func (n *Node) Peers(context.Context) ([]coreiface.ConnectionInfo, error) {
	if n.Host == nil {
		return nil, coreiface.ErrOffline
	}

	conns := n.Host.Network().Conns()

	var out []coreiface.ConnectionInfo
	for _, c := range conns {
		pid := c.RemotePeer()
		addr := c.RemoteMultiaddr()

		ci := &connInfo{
			peerstore: n.PeerStore,
			conn:      c,
			dir:       c.Stat().Direction,

			addr: addr,
			peer: pid,
		}

		/*
			// FIXME(steb):
			swcon, ok := c.(*swarm.Conn)
			if ok {
				ci.muxer = fmt.Sprintf("%T", swcon.StreamConn().Conn())
			}
		*/

		out = append(out, ci)
	}

	return out, nil
}

func (ci *connInfo) ID() peer.ID {
	return ci.peer
}

func (ci *connInfo) Address() ma.Multiaddr {
	return ci.addr
}

func (ci *connInfo) Direction() inet.Direction {
	return ci.dir
}

func (ci *connInfo) Latency() (time.Duration, error) {
	return ci.peerstore.LatencyEWMA(peer.ID(ci.ID())), nil
}

func (ci *connInfo) Streams() ([]protocol.ID, error) {
	streams := ci.conn.GetStreams()

	out := make([]protocol.ID, len(streams))
	for i, s := range streams {
		out[i] = s.Protocol()
	}

	return out, nil
}
