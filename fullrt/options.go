package fullrt

import (
	"fmt"
	"time"

	kaddht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p-kad-dht/crawler"
	"github.com/libp2p/go-libp2p-kad-dht/records"
)

type config struct {
	dhtOpts []kaddht.Option

	crawlInterval          time.Duration
	waitFrac               float64
	bulkSendParallelism    int
	timeoutPerOp           time.Duration
	crawler                crawler.Crawler
	pmOpts                 []records.Option
	ipDiversityFilterLimit int
	routeTableFilter       kaddht.RouteTableFilterFunc
}

func (cfg *config) apply(opts ...Option) error {
	for i, o := range opts {
		if err := o(cfg); err != nil {
			return fmt.Errorf("fullrt dht option %d failed: %w", i, err)
		}
	}
	return nil
}

type Option func(opt *config) error

func DHTOption(opts ...kaddht.Option) Option {
	return func(c *config) error {
		c.dhtOpts = append(c.dhtOpts, opts...)
		return nil
	}
}

// WithCrawler sets the crawler.Crawler to use in order to crawl the DHT network.
// Defaults to crawler.DefaultCrawler with parallelism of 200.
func WithCrawler(c crawler.Crawler) Option {
	return func(opt *config) error {
		opt.crawler = c
		return nil
	}
}

// WithCrawlInterval sets the interval at which the DHT is crawled to refresh
// peer store. Defaults to 1 hour if unspecified.
func WithCrawlInterval(i time.Duration) Option {
	return func(opt *config) error {
		opt.crawlInterval = i
		return nil
	}
}

// WithSuccessWaitFraction sets the fraction of peers to wait for before
// considering an operation a success defined as a number between (0, 1].
// Defaults to 30% if unspecified.
func WithSuccessWaitFraction(f float64) Option {
	return func(opt *config) error {
		if f <= 0 || f > 1 {
			return fmt.Errorf("success wait fraction must be larger than 0 and smaller or equal to 1; got: %f", f)
		}
		opt.waitFrac = f
		return nil
	}
}

// WithBulkSendParallelism sets the maximum degree of parallelism at which
// messages are sent to other peers. It must be at least 1. Defaults to 20 if
// unspecified.
func WithBulkSendParallelism(b int) Option {
	return func(opt *config) error {
		if b < 1 {
			return fmt.Errorf("bulk send parallelism must be at least 1; got: %d", b)
		}
		opt.bulkSendParallelism = b
		return nil
	}
}

// WithTimeoutPerOperation sets the timeout per operation, where operations
// include putting providers and querying the DHT. Defaults to 5 seconds if
// unspecified.
func WithTimeoutPerOperation(t time.Duration) Option {
	return func(opt *config) error {
		opt.timeoutPerOp = t
		return nil
	}
}

// WithProviderManagerOptions sets the options to use when instantiating
// records.ProviderManager.
func WithProviderManagerOptions(pmOpts ...records.Option) Option {
	return func(opt *config) error {
		opt.pmOpts = pmOpts
		return nil
	}
}

// WithIPDiversityFilterLimit sets the maximum number of peers with addresses
// in the same IP group returned by GetClosestPeers.
func WithIPDiversityFilterLimit(ipDiversityFilterLimit int) Option {
	return func(opt *config) error {
		opt.ipDiversityFilterLimit = ipDiversityFilterLimit
		return nil
	}
}

// WithRouteTableFilter sets the filter fullrt applies to every peer the crawler
// reports, before that peer enters the routing table.
//
// The default, kaddht.PublicRoutingTableFilter, keeps a peer only while the host
// has an open connection to it. That holds for a crawler that has just dialled
// the peer itself, but a crawler which reports peers it did not dial - one
// replaying a persisted routing table, say - can never satisfy it, and every
// peer it reports is dropped.
//
// The filter receives the *FullRT as its first argument, so a custom filter can
// delegate to kaddht.PublicRoutingTableFilter and widen it rather than replace
// it. Returning true for everything trusts the crawler completely.
func WithRouteTableFilter(f kaddht.RouteTableFilterFunc) Option {
	return func(opt *config) error {
		if f == nil {
			return fmt.Errorf("route table filter must not be nil")
		}
		opt.routeTableFilter = f
		return nil
	}
}
