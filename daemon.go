package pubsub

import (
	"context"
	_ "expvar"
	"fmt"
	"github.com/ipfs/go-bitswap"
	bsnet "github.com/ipfs/go-bitswap/network"
	"github.com/ipfs/go-blockservice"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipfs/go-merkledag"
	"github.com/libp2p/go-libp2p"
	connmgr "github.com/libp2p/go-libp2p-connmgr"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/event"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/libp2p/go-libp2p-core/routing"
	disc "github.com/libp2p/go-libp2p-discovery"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	noise "github.com/libp2p/go-libp2p-noise"
	"github.com/libp2p/go-libp2p-peerstore/pstoremem"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	libp2pquic "github.com/libp2p/go-libp2p-quic-transport"
	tls "github.com/libp2p/go-libp2p-tls"
	ma "github.com/multiformats/go-multiaddr"
	"math/rand"
	_ "net/http/pprof"
	"sort"
	"time"
)

func daemon(n *Node, ctx context.Context) error {

	// let the user know we're going.
	n.Listener.Info("Initializing daemon...")

	// The node will also close the repo but there are many places we could
	// fail before we get to that. It can't hurt to close it twice.
	defer n.DataStore.Close()

	var Swarm []string

	Swarm = append(Swarm, fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", n.Port))
	Swarm = append(Swarm, fmt.Sprintf("/ip6/::/tcp/%d", n.Port))
	Swarm = append(Swarm, fmt.Sprintf("/ip4/0.0.0.0/udp/%d/quic", n.Port))
	Swarm = append(Swarm, fmt.Sprintf("/ip6/::/udp/%d/quic", n.Port))

	var err error
	mAddresses, err := listenAddresses(Swarm)
	if err != nil {
		return err
	}

	id, err := peer.Decode(n.PeerID)
	if err != nil {
		return fmt.Errorf("invalid peer id")
	}

	sk, err := DecodePrivateKey(n.PrivateKey)
	if err != nil {
		return err
	}

	n.PeerStore = pstoremem.NewPeerstore()

	err = pstoreAddSelfKeys(id, sk, n.PeerStore)
	if err != nil {
		return err
	}

	// hash security
	bs := blockstore.NewBlockstore(n.DataStore)
	bs = &VerifBS{Blockstore: bs}

	bs = blockstore.NewIdStore(bs)
	gcLocker := blockstore.NewGCLocker()
	n.BlockStore = blockstore.NewGCBlockstore(bs, gcLocker)

	grace, err := time.ParseDuration(n.GracePeriod)
	if err != nil {
		return fmt.Errorf("parsing Swarm.ConnMgr.GracePeriod: %s", err)
	}
	n.ConnectionManager = connmgr.NewConnManager(n.LowWater, n.HighWater, grace)

	// HOST and Routing
	var opts []libp2p.Option
	opts = append(opts, libp2p.ListenAddrs(mAddresses...))
	opts = append(opts, libp2p.UserAgent(n.Agent))
	opts = append(opts, libp2p.ChainOptions(libp2p.Security(tls.ID, tls.New), libp2p.Security(noise.ID, noise.New)))
	opts = append(opts, libp2p.ConnectionManager(n.ConnectionManager))
	opts = append(opts, libp2p.Transport(libp2pquic.NewTransport))
	opts = append(opts, libp2p.DefaultTransports)
	opts = append(opts, libp2p.Ping(false))
	opts = append(opts, libp2p.ChainOptions(libp2p.EnableAutoRelay(), libp2p.DefaultStaticRelays()))

	// Let this host use the DHT to find other hosts
	opts = append(opts, libp2p.Routing(func(host host.Host) (routing.PeerRouting, error) {

		n.Routing, err = dht.New(
			ctx, host, dht.Concurrency(10),
			dht.Mode(dht.ModeClient),
			dht.DisableAutoRefresh())

		n.PubSub, _ = GossipSub(ctx, host, n.Routing)

		bitSwapNetwork := bsnet.NewFromIpfsHost(host, n.Routing)
		exchange := bitswap.New(ctx, bitSwapNetwork, bs,
			bitswap.ProvideEnabled(false))

		n.BlockService = blockservice.New(n.BlockStore, exchange)
		n.DagService = merkledag.NewDAGService(n.BlockService)
		return n.Routing, err
	}))

	n.Host, err = constructPeerHost(ctx, id, n.PeerStore, opts)
	if err != nil {
		return fmt.Errorf("constructPeerHost: %s", err)
	}

	n.Listener.Info("New Node...")

	printSwarmAddrs(n)

	defer func() {
		// We wait for the node to close first, as the node has children
		// that it will wait for before closing, such as the API server.
		n.DataStore.Close()

		//case <-req.Context.Done():
		select {
		case <-ctx.Done():
			n.Listener.Info("Gracefully shut down daemon")
		default:
		}
	}()

	n.Listener.Info("Daemon is ready")

	n.Running = true

	go reachable(n, ctx)

	for {
		n.Listener.Verbose("Daemon still running ...")
		time.Sleep(10 * time.Second)
	}

	return nil
}

func reachable(n *Node, ctx context.Context) {
	subReachability, _ := n.Host.EventBus().Subscribe(new(event.EvtLocalReachabilityChanged))
	defer subReachability.Close()

	for {
		select {
		case ev, ok := <-subReachability.Out():
			if !ok {
				return
			}
			evt, ok := ev.(event.EvtLocalReachabilityChanged)
			if !ok {
				return
			}
			if evt.Reachability == network.ReachabilityPrivate {
				n.Listener.ReachablePrivate()
			} else if evt.Reachability == network.ReachabilityPublic {
				n.Listener.ReachablePublic()
			} else {
				n.Listener.ReachableUnknown()
			}

		case <-ctx.Done():
			return
		}
	}
}

func GossipSub(ctx context.Context, host host.Host,
	router routing.ContentRouting, pubsubOptions ...pubsub.Option) (*pubsub.PubSub, error) {

	baseDisc := disc.NewRoutingDiscovery(router)
	minBackoff, maxBackoff := time.Second*60, time.Hour
	rng := rand.New(rand.NewSource(rand.Int63()))
	d, err := disc.NewBackoffDiscovery(
		baseDisc,
		disc.NewExponentialBackoff(minBackoff, maxBackoff, disc.FullJitter, time.Second, 5.0, 0, rng),
	)
	if err != nil {
		return nil, err
	}

	return pubsub.NewGossipSub(ctx, host, append(pubsubOptions, pubsub.WithDiscovery(d))...)

}

func listenAddresses(addresses []string) ([]ma.Multiaddr, error) {
	var listen []ma.Multiaddr
	for _, addr := range addresses {
		maddr, err := ma.NewMultiaddr(addr)
		if err != nil {
			return nil, fmt.Errorf("failure to parse config.Addresses.Swarm: %s", addresses)
		}
		listen = append(listen, maddr)
	}

	return listen, nil
}

func pstoreAddSelfKeys(id peer.ID, sk crypto.PrivKey, ps peerstore.Peerstore) error {
	if err := ps.AddPubKey(id, sk.GetPublic()); err != nil {
		return err
	}

	return ps.AddPrivKey(id, sk)
}

func constructPeerHost(ctx context.Context, id peer.ID, ps peerstore.Peerstore, options []libp2p.Option) (host.Host, error) {
	pkey := ps.PrivKey(id)
	if pkey == nil {
		return nil, fmt.Errorf("missing private key for node ID: %s", id.Pretty())
	}
	options = append([]libp2p.Option{libp2p.Identity(pkey), libp2p.Peerstore(ps)}, options...)

	return libp2p.New(ctx, options...)
}

func printSwarmAddrs(n *Node) {

	var lisAddrs []string
	ifaceAddrs, err := n.Host.Network().InterfaceListenAddresses()
	if err != nil {
		n.Listener.Error(fmt.Sprintf("failed to read listening addresses: %s", err))
	}
	for _, addr := range ifaceAddrs {
		lisAddrs = append(lisAddrs, addr.String())
	}
	sort.Strings(lisAddrs)
	for _, addr := range lisAddrs {
		n.Listener.Info(fmt.Sprintf("Swarm listening on %s", addr))
	}

	var addrs []string
	for _, addr := range n.Host.Addrs() {
		addrs = append(addrs, addr.String())
	}
	sort.Strings(addrs)
	for _, addr := range addrs {
		n.Listener.Info(fmt.Sprintf("Swarm announcing %s", addr))
	}

}
