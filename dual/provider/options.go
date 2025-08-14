package provider

import (
	"errors"
	"fmt"
	"time"

	ds "github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p-kad-dht/amino"
	"github.com/libp2p/go-libp2p-kad-dht/dual"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	"github.com/libp2p/go-libp2p-kad-dht/provider"
	"github.com/libp2p/go-libp2p-kad-dht/provider/datastore"
)

const (
	lanID uint8 = iota
	wanID
)

type config struct {
	keyStore *datastore.KeyStore

	reprovideInterval [2]time.Duration // [0] = LAN, [1] = WAN
	maxReprovideDelay [2]time.Duration

	connectivityCheckOnlineInterval  [2]time.Duration
	connectivityCheckOfflineInterval [2]time.Duration

	maxWorkers               [2]int
	dedicatedPeriodicWorkers [2]int
	dedicatedBurstWorkers    [2]int
	maxProvideConnsPerWorker [2]int

	msgSenders [2]pb.MessageSender
}

type Option func(opt *config) error

func (cfg *config) apply(opts ...Option) error {
	for i, o := range opts {
		if err := o(cfg); err != nil {
			return fmt.Errorf("dual dht provider option %d failed: %w", i, err)
		}
	}
	return nil
}

func (cfg *config) resolveDefaults(d *dual.DHT) {
	if cfg.msgSenders[lanID] == nil {
		cfg.msgSenders[lanID] = d.LAN.MessageSender()
	}
	if cfg.msgSenders[wanID] == nil {
		cfg.msgSenders[wanID] = d.WAN.MessageSender()
	}
}

func (c *config) validate() error {
	if c.dedicatedPeriodicWorkers[lanID]+c.dedicatedBurstWorkers[lanID] > c.maxWorkers[lanID] {
		return errors.New("provider config: total dedicated workers exceed max workers")
	}
	if c.dedicatedPeriodicWorkers[wanID]+c.dedicatedBurstWorkers[wanID] > c.maxWorkers[wanID] {
		return errors.New("provider config: total dedicated workers exceed max workers")
	}
	return nil
}

var DefaultConfig = func(cfg *config) error {
	var err error
	cfg.keyStore, err = datastore.NewKeyStore(ds.NewMapDatastore())
	if err != nil {
		return err
	}

	cfg.reprovideInterval = [2]time.Duration{amino.DefaultReprovideInterval, amino.DefaultReprovideInterval}
	cfg.maxReprovideDelay = [2]time.Duration{provider.DefaultMaxReprovideDelay, provider.DefaultMaxReprovideDelay}

	cfg.connectivityCheckOnlineInterval = [2]time.Duration{provider.DefaultConnectivityCheckOnlineInterval, provider.DefaultConnectivityCheckOnlineInterval}
	cfg.connectivityCheckOfflineInterval = [2]time.Duration{provider.DefaultConnectivityCheckOfflineInterval, provider.DefaultConnectivityCheckOfflineInterval}

	cfg.maxWorkers = [2]int{4, 4}
	cfg.dedicatedPeriodicWorkers = [2]int{2, 2}
	cfg.dedicatedBurstWorkers = [2]int{1, 1}
	cfg.maxProvideConnsPerWorker = [2]int{20, 20}

	return nil
}

func WithKeyStore(keyStore *datastore.KeyStore) Option {
	return func(cfg *config) error {
		if keyStore == nil {
			return errors.New("provider config: keyStore cannot be nil")
		}
		cfg.keyStore = keyStore
		return nil
	}
}

func WithReprovideInterval(reprovideInterval time.Duration) Option {
	return func(cfg *config) error {
		if reprovideInterval <= 0 {
			return fmt.Errorf("invalid reprovide interval %s", reprovideInterval)
		}
		for i := range cfg.reprovideInterval {
			cfg.reprovideInterval[i] = reprovideInterval
		}
		return nil
	}
}

func WithReprovideIntervalLAN(reprovideInterval time.Duration) Option {
	return func(cfg *config) error {
		if reprovideInterval <= 0 {
			return fmt.Errorf("invalid LAN reprovide interval %s", reprovideInterval)
		}
		cfg.reprovideInterval[lanID] = reprovideInterval
		return nil
	}
}

func WithReprovideIntervalWAN(reprovideInterval time.Duration) Option {
	return func(cfg *config) error {
		if reprovideInterval <= 0 {
			return fmt.Errorf("invalid WAN reprovide interval %s", reprovideInterval)
		}
		cfg.reprovideInterval[wanID] = reprovideInterval
		return nil
	}
}

func WithMaxReprovideDelay(maxReprovideDelay time.Duration) Option {
	return func(cfg *config) error {
		if maxReprovideDelay <= 0 {
			return fmt.Errorf("invalid max reprovide delay %s", maxReprovideDelay)
		}
		for i := range cfg.maxReprovideDelay {
			cfg.maxReprovideDelay[i] = maxReprovideDelay
		}
		return nil
	}
}

func WithMaxReprovideDelayLAN(maxReprovideDelay time.Duration) Option {
	return func(cfg *config) error {
		if maxReprovideDelay <= 0 {
			return fmt.Errorf("invalid LAN max reprovide delay %s", maxReprovideDelay)
		}
		cfg.maxReprovideDelay[lanID] = maxReprovideDelay
		return nil
	}
}

func WithMaxReprovideDelayWAN(maxReprovideDelay time.Duration) Option {
	return func(cfg *config) error {
		if maxReprovideDelay <= 0 {
			return fmt.Errorf("invalid WAN max reprovide delay %s", maxReprovideDelay)
		}
		cfg.maxReprovideDelay[wanID] = maxReprovideDelay
		return nil
	}
}

func WithConnectivityCheckOnlineInterval(onlineInterval time.Duration) Option {
	return func(cfg *config) error {
		if onlineInterval <= 0 {
			return fmt.Errorf("invalid connectivity check online interval %s", onlineInterval)
		}
		for i := range cfg.connectivityCheckOnlineInterval {
			cfg.connectivityCheckOnlineInterval[i] = onlineInterval
		}
		return nil
	}
}

func WithConnectivityCheckOnlineIntervalLAN(onlineInterval time.Duration) Option {
	return func(cfg *config) error {
		if onlineInterval <= 0 {
			return fmt.Errorf("invalid LAN connectivity check online interval %s", onlineInterval)
		}
		cfg.connectivityCheckOnlineInterval[lanID] = onlineInterval
		return nil
	}
}

func WithConnectivityCheckOnlineIntervalWAN(onlineInterval time.Duration) Option {
	return func(cfg *config) error {
		if onlineInterval <= 0 {
			return fmt.Errorf("invalid WAN connectivity check online interval %s", onlineInterval)
		}
		cfg.connectivityCheckOnlineInterval[wanID] = onlineInterval
		return nil
	}
}

func WithConnectivityCheckOfflineInterval(offlineInterval time.Duration) Option {
	return func(cfg *config) error {
		if offlineInterval <= 0 {
			return fmt.Errorf("invalid connectivity check offline interval %s", offlineInterval)
		}
		for i := range cfg.connectivityCheckOfflineInterval {
			cfg.connectivityCheckOfflineInterval[i] = offlineInterval
		}
		return nil
	}
}

func WithConnectivityCheckOfflineIntervalLAN(offlineInterval time.Duration) Option {
	return func(cfg *config) error {
		if offlineInterval <= 0 {
			return fmt.Errorf("invalid LAN connectivity check offline interval %s", offlineInterval)
		}
		cfg.connectivityCheckOfflineInterval[lanID] = offlineInterval
		return nil
	}
}

func WithConnectivityCheckOfflineIntervalWAN(offlineInterval time.Duration) Option {
	return func(cfg *config) error {
		if offlineInterval <= 0 {
			return fmt.Errorf("invalid WAN connectivity check offline interval %s", offlineInterval)
		}
		cfg.connectivityCheckOfflineInterval[wanID] = offlineInterval
		return nil
	}
}

func WithMaxWorkers(maxWorkers int) Option {
	return func(cfg *config) error {
		if maxWorkers <= 0 {
			return fmt.Errorf("invalid max workers %d", maxWorkers)
		}
		for i := range cfg.maxWorkers {
			cfg.maxWorkers[i] = maxWorkers
		}
		return nil
	}
}

func WithMaxWorkersLAN(maxWorkers int) Option {
	return func(cfg *config) error {
		if maxWorkers <= 0 {
			return fmt.Errorf("invalid LAN max workers %d", maxWorkers)
		}
		cfg.maxWorkers[lanID] = maxWorkers
		return nil
	}
}

func WithMaxWorkersWAN(maxWorkers int) Option {
	return func(cfg *config) error {
		if maxWorkers <= 0 {
			return fmt.Errorf("invalid WAN max workers %d", maxWorkers)
		}
		cfg.maxWorkers[wanID] = maxWorkers
		return nil
	}
}

func WithDedicatedPeriodicWorkers(dedicatedPeriodicWorkers int) Option {
	return func(cfg *config) error {
		if dedicatedPeriodicWorkers < 0 {
			return fmt.Errorf("invalid dedicated periodic workers %d", dedicatedPeriodicWorkers)
		}
		for i := range cfg.dedicatedPeriodicWorkers {
			cfg.dedicatedPeriodicWorkers[i] = dedicatedPeriodicWorkers
		}
		return nil
	}
}

func WithDedicatedPeriodicWorkersLAN(dedicatedPeriodicWorkers int) Option {
	return func(cfg *config) error {
		if dedicatedPeriodicWorkers < 0 {
			return fmt.Errorf("invalid LAN dedicated periodic workers %d", dedicatedPeriodicWorkers)
		}
		cfg.dedicatedPeriodicWorkers[lanID] = dedicatedPeriodicWorkers
		return nil
	}
}

func WithDedicatedPeriodicWorkersWAN(dedicatedPeriodicWorkers int) Option {
	return func(cfg *config) error {
		if dedicatedPeriodicWorkers < 0 {
			return fmt.Errorf("invalid WAN dedicated periodic workers %d", dedicatedPeriodicWorkers)
		}
		cfg.dedicatedPeriodicWorkers[wanID] = dedicatedPeriodicWorkers
		return nil
	}
}

func WithDedicatedBurstWorkers(dedicatedBurstWorkers int) Option {
	return func(cfg *config) error {
		if dedicatedBurstWorkers < 0 {
			return fmt.Errorf("invalid dedicated burst workers %d", dedicatedBurstWorkers)
		}
		for i := range cfg.dedicatedBurstWorkers {
			cfg.dedicatedBurstWorkers[i] = dedicatedBurstWorkers
		}
		return nil
	}
}

func WithDedicatedBurstWorkersLAN(dedicatedBurstWorkers int) Option {
	return func(cfg *config) error {
		if dedicatedBurstWorkers < 0 {
			return fmt.Errorf("invalid LAN dedicated burst workers %d", dedicatedBurstWorkers)
		}
		cfg.dedicatedBurstWorkers[lanID] = dedicatedBurstWorkers
		return nil
	}
}

func WithDedicatedBurstWorkersWAN(dedicatedBurstWorkers int) Option {
	return func(cfg *config) error {
		if dedicatedBurstWorkers < 0 {
			return fmt.Errorf("invalid WAN dedicated burst workers %d", dedicatedBurstWorkers)
		}
		cfg.dedicatedBurstWorkers[wanID] = dedicatedBurstWorkers
		return nil
	}
}

func WithMaxProvideConnsPerWorker(maxProvideConnsPerWorker int) Option {
	return func(cfg *config) error {
		if maxProvideConnsPerWorker <= 0 {
			return fmt.Errorf("invalid max provide conns per worker %d", maxProvideConnsPerWorker)
		}
		for i := range cfg.maxProvideConnsPerWorker {
			cfg.maxProvideConnsPerWorker[i] = maxProvideConnsPerWorker
		}
		return nil
	}
}

func WithMaxProvideConnsPerWorkerLAN(maxProvideConnsPerWorker int) Option {
	return func(cfg *config) error {
		if maxProvideConnsPerWorker <= 0 {
			return fmt.Errorf("invalid LAN max provide conns per worker %d", maxProvideConnsPerWorker)
		}
		cfg.maxProvideConnsPerWorker[lanID] = maxProvideConnsPerWorker
		return nil
	}
}

func WithMaxProvideConnsPerWorkerWAN(maxProvideConnsPerWorker int) Option {
	return func(cfg *config) error {
		if maxProvideConnsPerWorker <= 0 {
			return fmt.Errorf("invalid WAN max provide conns per worker %d", maxProvideConnsPerWorker)
		}
		cfg.maxProvideConnsPerWorker[wanID] = maxProvideConnsPerWorker
		return nil
	}
}

func WithMessageSenderLAN(msgSender pb.MessageSender) Option {
	return func(cfg *config) error {
		if msgSender == nil {
			return errors.New("provider config: LAN message sender cannot be nil")
		}
		cfg.msgSenders[lanID] = msgSender
		return nil
	}
}

func WithMessageSenderWAN(msgSender pb.MessageSender) Option {
	return func(cfg *config) error {
		if msgSender == nil {
			return errors.New("provider config: WAN message sender cannot be nil")
		}
		cfg.msgSenders[wanID] = msgSender
		return nil
	}
}
