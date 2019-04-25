package graph

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/a-h/pregel"
)

// FromContext returns the node loader from the context.
func FromContext(ctx context.Context) *NodeLoader {
	return ctx.Value(nodeLoaderKey).(*NodeLoader)
}

type dataLoaderMiddlewareKey string

const nodeLoaderKey = dataLoaderMiddlewareKey("dataloaderNode")

// NodeGetter can retrieve a node.
type NodeGetter interface {
	Get(id string) (n pregel.Node, ok bool, err error)
}

// NodeDataLoaderStats contains stats about the operation.
type NodeDataLoaderStats struct {
	FetchesMade int64
	NodesLoaded int64
	StartTime   time.Time
	TimeTaken   time.Duration
}

// NewNodeDataLoaderStats creates a new data loader.
func NewNodeDataLoaderStats(startTime time.Time) NodeDataLoaderStats {
	return NodeDataLoaderStats{
		StartTime: startTime,
	}
}

// NodeDataLoaderMiddlware is middleware which loads nodes using the NodeGetter.
type NodeDataLoaderMiddlware struct {
	Next       http.Handler
	NodeGetter NodeGetter
	Now        func() time.Time
	Stats      func(s NodeDataLoaderStats)
}

func (ndlm *NodeDataLoaderMiddlware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	stats := NewNodeDataLoaderStats(ndlm.Now().UTC())
	l := NewNodeLoader(NodeLoaderConfig{
		Fetch: func(ids []string) (nodes []*pregel.Node, errs []error) {
			stats.FetchesMade++

			nodes = make([]*pregel.Node, len(ids))
			errs = make([]error, len(ids))

			var wg sync.WaitGroup
			wg.Add(len(ids))
			for i, id := range ids {
				go func(index int, nodeID string) {
					defer func() {
						stats.NodesLoaded++
						wg.Done()
					}()
					n, ok, err := ndlm.NodeGetter.Get(nodeID)
					if err != nil {
						errs[index] = err
						return
					}
					if !ok {
						return
					}
					nodes[index] = &n
					return
				}(i, id)
			}

			wg.Wait()
			return
		},
		MaxBatch: 10,
		Wait:     time.Millisecond,
	})
	ctx := context.WithValue(r.Context(), nodeLoaderKey, l)
	r = r.WithContext(ctx)
	ndlm.Next.ServeHTTP(w, r)
	stats.TimeTaken = ndlm.Now().Sub(stats.StartTime)
	if ndlm.Stats != nil {
		ndlm.Stats(stats)
	}
}

// WithNodeDataloaderMiddleware populates the Data Loader middleware for loading nodes.
func WithNodeDataloaderMiddleware(nodeGetter NodeGetter, statsLogger func(NodeDataLoaderStats), next http.Handler) *NodeDataLoaderMiddlware {
	return &NodeDataLoaderMiddlware{
		NodeGetter: nodeGetter,
		Next:       next,
		Now:        time.Now,
		Stats:      statsLogger,
	}
}
