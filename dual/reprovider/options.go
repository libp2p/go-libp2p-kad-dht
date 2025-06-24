package reprovider

import (
	"context"
	"errors"
	"fmt"
	"time"

	ds "github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p-kad-dht/amino"
	"github.com/libp2p/go-libp2p-kad-dht/reprovider"
	"github.com/libp2p/go-libp2p-kad-dht/reprovider/datastore"
)

const (
	lanID uint8 = iota
	wanID
)

type reproviderConfig struct {
	mhStore *datastore.MHStore

	reprovideInterval [2]time.Duration // [0] = LAN, [1] = WAN
	maxReprovideDelay [2]time.Duration

	connectivityCheckOnlineInterval  [2]time.Duration
	connectivityCheckOfflineInterval [2]time.Duration

	maxWorkers               [2]int
	dedicatedPeriodicWorkers [2]int
	dedicatedBurstWorkers    [2]int
	maxProvideConnsPerWorker [2]int
}

type ReproviderOption func(opt *reproviderConfig) error

func (cfg *reproviderConfig) apply(opts ...ReproviderOption) error {
	for i, o := range opts {
		if err := o(cfg); err != nil {
			return fmt.Errorf("reprovider dual dht option %d failed: %w", i, err)
		}
	}
	return nil
}

func (c *reproviderConfig) validate() error {
	if c.dedicatedPeriodicWorkers[lanID]+c.dedicatedBurstWorkers[lanID] > c.maxWorkers[lanID] {
		return errors.New("reprovider config: total dedicated workers exceed max workers")
	}
	if c.dedicatedPeriodicWorkers[wanID]+c.dedicatedBurstWorkers[wanID] > c.maxWorkers[wanID] {
		return errors.New("reprovider config: total dedicated workers exceed max workers")
	}
	return nil
}

var DefaultReproviderConfig = func(cfg *reproviderConfig) error {
	var err error
	cfg.mhStore, err = datastore.NewMHStore(context.Background(), ds.NewMapDatastore())
	if err != nil {
		return err
	}

	cfg.reprovideInterval = [2]time.Duration{amino.DefaultReprovideInterval, amino.DefaultReprovideInterval}
	cfg.maxReprovideDelay = [2]time.Duration{reprovider.DefaultMaxReprovideDelay, reprovider.DefaultMaxReprovideDelay}

	cfg.connectivityCheckOnlineInterval = [2]time.Duration{reprovider.DefaultConnectivityCheckOnlineInterval, reprovider.DefaultConnectivityCheckOnlineInterval}
	cfg.connectivityCheckOfflineInterval = [2]time.Duration{reprovider.DefaultConnectivityCheckOfflineInterval, reprovider.DefaultConnectivityCheckOfflineInterval}

	cfg.maxWorkers = [2]int{4, 4}
	cfg.dedicatedPeriodicWorkers = [2]int{2, 2}
	cfg.dedicatedBurstWorkers = [2]int{1, 1}
	cfg.maxProvideConnsPerWorker = [2]int{20, 20}

	return nil
}

func WithMHStore(mhStore *datastore.MHStore) ReproviderOption {
	return func(cfg *reproviderConfig) error {
		if mhStore == nil {
			return errors.New("reprovider config: mhStore cannot be nil")
		}
		cfg.mhStore = mhStore
		return nil
	}
}

func WithReprovideInterval(reprovideInterval time.Duration) ReproviderOption {
	return func(cfg *reproviderConfig) error {
		if reprovideInterval <= 0 {
			return fmt.Errorf("invalid reprovide interval %s", reprovideInterval)
		}
		for i := range cfg.reprovideInterval {
			cfg.reprovideInterval[i] = reprovideInterval
		}
		return nil
	}
}

func WithReprovideIntervalLAN(reprovideInterval time.Duration) ReproviderOption {
	return func(cfg *reproviderConfig) error {
		if reprovideInterval <= 0 {
			return fmt.Errorf("invalid LAN reprovide interval %s", reprovideInterval)
		}
		cfg.reprovideInterval[lanID] = reprovideInterval
		return nil
	}
}

func WithReprovideIntervalWAN(reprovideInterval time.Duration) ReproviderOption {
	return func(cfg *reproviderConfig) error {
		if reprovideInterval <= 0 {
			return fmt.Errorf("invalid WAN reprovide interval %s", reprovideInterval)
		}
		cfg.reprovideInterval[wanID] = reprovideInterval
		return nil
	}
}

func WithMaxReprovideDelay(maxReprovideDelay time.Duration) ReproviderOption {
	return func(cfg *reproviderConfig) error {
		if maxReprovideDelay <= 0 {
			return fmt.Errorf("invalid max reprovide delay %s", maxReprovideDelay)
		}
		for i := range cfg.maxReprovideDelay {
			cfg.maxReprovideDelay[i] = maxReprovideDelay
		}
		return nil
	}
}

func WithMaxReprovideDelayLAN(maxReprovideDelay time.Duration) ReproviderOption {
	return func(cfg *reproviderConfig) error {
		if maxReprovideDelay <= 0 {
			return fmt.Errorf("invalid LAN max reprovide delay %s", maxReprovideDelay)
		}
		cfg.maxReprovideDelay[lanID] = maxReprovideDelay
		return nil
	}
}

func WithMaxReprovideDelayWAN(maxReprovideDelay time.Duration) ReproviderOption {
	return func(cfg *reproviderConfig) error {
		if maxReprovideDelay <= 0 {
			return fmt.Errorf("invalid WAN max reprovide delay %s", maxReprovideDelay)
		}
		cfg.maxReprovideDelay[wanID] = maxReprovideDelay
		return nil
	}
}

func WithConnectivityCheckOnlineInterval(onlineInterval time.Duration) ReproviderOption {
	return func(cfg *reproviderConfig) error {
		if onlineInterval <= 0 {
			return fmt.Errorf("invalid connectivity check online interval %s", onlineInterval)
		}
		for i := range cfg.connectivityCheckOnlineInterval {
			cfg.connectivityCheckOnlineInterval[i] = onlineInterval
		}
		return nil
	}
}

func WithConnectivityCheckOnlineIntervalLAN(onlineInterval time.Duration) ReproviderOption {
	return func(cfg *reproviderConfig) error {
		if onlineInterval <= 0 {
			return fmt.Errorf("invalid LAN connectivity check online interval %s", onlineInterval)
		}
		cfg.connectivityCheckOnlineInterval[lanID] = onlineInterval
		return nil
	}
}

func WithConnectivityCheckOnlineIntervalWAN(onlineInterval time.Duration) ReproviderOption {
	return func(cfg *reproviderConfig) error {
		if onlineInterval <= 0 {
			return fmt.Errorf("invalid WAN connectivity check online interval %s", onlineInterval)
		}
		cfg.connectivityCheckOnlineInterval[wanID] = onlineInterval
		return nil
	}
}

func WithConnectivityCheckOfflineInterval(offlineInterval time.Duration) ReproviderOption {
	return func(cfg *reproviderConfig) error {
		if offlineInterval <= 0 {
			return fmt.Errorf("invalid connectivity check offline interval %s", offlineInterval)
		}
		for i := range cfg.connectivityCheckOfflineInterval {
			cfg.connectivityCheckOfflineInterval[i] = offlineInterval
		}
		return nil
	}
}

func WithConnectivityCheckOfflineIntervalLAN(offlineInterval time.Duration) ReproviderOption {
	return func(cfg *reproviderConfig) error {
		if offlineInterval <= 0 {
			return fmt.Errorf("invalid LAN connectivity check offline interval %s", offlineInterval)
		}
		cfg.connectivityCheckOfflineInterval[lanID] = offlineInterval
		return nil
	}
}

func WithConnectivityCheckOfflineIntervalWAN(offlineInterval time.Duration) ReproviderOption {
	return func(cfg *reproviderConfig) error {
		if offlineInterval <= 0 {
			return fmt.Errorf("invalid WAN connectivity check offline interval %s", offlineInterval)
		}
		cfg.connectivityCheckOfflineInterval[wanID] = offlineInterval
		return nil
	}
}

func WithMaxWorkers(maxWorkers int) ReproviderOption {
	return func(cfg *reproviderConfig) error {
		if maxWorkers <= 0 {
			return fmt.Errorf("invalid max workers %d", maxWorkers)
		}
		for i := range cfg.maxWorkers {
			cfg.maxWorkers[i] = maxWorkers
		}
		return nil
	}
}

func WithMaxWorkersLAN(maxWorkers int) ReproviderOption {
	return func(cfg *reproviderConfig) error {
		if maxWorkers <= 0 {
			return fmt.Errorf("invalid LAN max workers %d", maxWorkers)
		}
		cfg.maxWorkers[lanID] = maxWorkers
		return nil
	}
}

func WithMaxWorkersWAN(maxWorkers int) ReproviderOption {
	return func(cfg *reproviderConfig) error {
		if maxWorkers <= 0 {
			return fmt.Errorf("invalid WAN max workers %d", maxWorkers)
		}
		cfg.maxWorkers[wanID] = maxWorkers
		return nil
	}
}

func WithDedicatedPeriodicWorkers(dedicatedPeriodicWorkers int) ReproviderOption {
	return func(cfg *reproviderConfig) error {
		if dedicatedPeriodicWorkers < 0 {
			return fmt.Errorf("invalid dedicated periodic workers %d", dedicatedPeriodicWorkers)
		}
		for i := range cfg.dedicatedPeriodicWorkers {
			cfg.dedicatedPeriodicWorkers[i] = dedicatedPeriodicWorkers
		}
		return nil
	}
}

func WithDedicatedPeriodicWorkersLAN(dedicatedPeriodicWorkers int) ReproviderOption {
	return func(cfg *reproviderConfig) error {
		if dedicatedPeriodicWorkers < 0 {
			return fmt.Errorf("invalid LAN dedicated periodic workers %d", dedicatedPeriodicWorkers)
		}
		cfg.dedicatedPeriodicWorkers[lanID] = dedicatedPeriodicWorkers
		return nil
	}
}

func WithDedicatedPeriodicWorkersWAN(dedicatedPeriodicWorkers int) ReproviderOption {
	return func(cfg *reproviderConfig) error {
		if dedicatedPeriodicWorkers < 0 {
			return fmt.Errorf("invalid WAN dedicated periodic workers %d", dedicatedPeriodicWorkers)
		}
		cfg.dedicatedPeriodicWorkers[wanID] = dedicatedPeriodicWorkers
		return nil
	}
}

func WithDedicatedBurstWorkers(dedicatedBurstWorkers int) ReproviderOption {
	return func(cfg *reproviderConfig) error {
		if dedicatedBurstWorkers < 0 {
			return fmt.Errorf("invalid dedicated burst workers %d", dedicatedBurstWorkers)
		}
		for i := range cfg.dedicatedBurstWorkers {
			cfg.dedicatedBurstWorkers[i] = dedicatedBurstWorkers
		}
		return nil
	}
}

func WithDedicatedBurstWorkersLAN(dedicatedBurstWorkers int) ReproviderOption {
	return func(cfg *reproviderConfig) error {
		if dedicatedBurstWorkers < 0 {
			return fmt.Errorf("invalid LAN dedicated burst workers %d", dedicatedBurstWorkers)
		}
		cfg.dedicatedBurstWorkers[lanID] = dedicatedBurstWorkers
		return nil
	}
}

func WithDedicatedBurstWorkersWAN(dedicatedBurstWorkers int) ReproviderOption {
	return func(cfg *reproviderConfig) error {
		if dedicatedBurstWorkers < 0 {
			return fmt.Errorf("invalid WAN dedicated burst workers %d", dedicatedBurstWorkers)
		}
		cfg.dedicatedBurstWorkers[wanID] = dedicatedBurstWorkers
		return nil
	}
}

func WithMaxProvideConnsPerWorker(maxProvideConnsPerWorker int) ReproviderOption {
	return func(cfg *reproviderConfig) error {
		if maxProvideConnsPerWorker <= 0 {
			return fmt.Errorf("invalid max provide conns per worker %d", maxProvideConnsPerWorker)
		}
		for i := range cfg.maxProvideConnsPerWorker {
			cfg.maxProvideConnsPerWorker[i] = maxProvideConnsPerWorker
		}
		return nil
	}
}

func WithMaxProvideConnsPerWorkerLAN(maxProvideConnsPerWorker int) ReproviderOption {
	return func(cfg *reproviderConfig) error {
		if maxProvideConnsPerWorker <= 0 {
			return fmt.Errorf("invalid LAN max provide conns per worker %d", maxProvideConnsPerWorker)
		}
		cfg.maxProvideConnsPerWorker[lanID] = maxProvideConnsPerWorker
		return nil
	}
}

func WithMaxProvideConnsPerWorkerWAN(maxProvideConnsPerWorker int) ReproviderOption {
	return func(cfg *reproviderConfig) error {
		if maxProvideConnsPerWorker <= 0 {
			return fmt.Errorf("invalid WAN max provide conns per worker %d", maxProvideConnsPerWorker)
		}
		cfg.maxProvideConnsPerWorker[wanID] = maxProvideConnsPerWorker
		return nil
	}
}
