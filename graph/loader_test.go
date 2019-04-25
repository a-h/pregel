package graph

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/a-h/pregel"
)

type inMemoryNodeGetter struct {
	nodes map[string]pregel.Node
}

var errNodeGetFailure = errors.New("node get failure")

func (imng *inMemoryNodeGetter) Get(id string) (n pregel.Node, ok bool, err error) {
	if id == "error" {
		err = errNodeGetFailure
		return
	}
	n, ok = imng.nodes[id]
	return
}

func Test(t *testing.T) {
	nodeA := pregel.NewNode("a")
	nodeB := pregel.NewNode("b")
	nodeC := pregel.NewNode("c")
	nodeD := pregel.NewNode("d")
	nodeE := pregel.NewNode("e")
	nodeF := pregel.NewNode("f")
	nodeG := pregel.NewNode("g")
	nodeH := pregel.NewNode("h")
	nodeI := pregel.NewNode("i")
	nodeJ := pregel.NewNode("j")
	nodeK := pregel.NewNode("k")
	nodeL := pregel.NewNode("l")

	tests := []struct {
		name             string
		inputs           []string
		expectedNodes    []*pregel.Node
		expectedErrors   []error
		expectedNodeGets int64
		expectedFetches  int64
	}{
		{
			name:            "no inputs, no nodes or errors",
			inputs:          []string{},
			expectedFetches: 0,
		},
		{
			name:   "one valid input, one node",
			inputs: []string{"a"},
			expectedNodes: []*pregel.Node{
				&nodeA,
			},
			expectedErrors: []error{
				nil,
			},
			expectedNodeGets: 1,
			expectedFetches:  1,
		},
		{
			name:   "two valid inputs, two nodes",
			inputs: []string{"a", "b"},
			expectedNodes: []*pregel.Node{
				&nodeA,
				&nodeB,
			},
			expectedErrors: []error{
				nil,
				nil,
			},
			expectedNodeGets: 2,
			expectedFetches:  1,
		},
		{
			name:   "one valid and one invalid input results in a node and an error",
			inputs: []string{"a", "error"},
			expectedNodes: []*pregel.Node{
				&nodeA,
				nil,
			},
			expectedErrors: []error{
				nil,
				errNodeGetFailure,
			},
			expectedNodeGets: 2,
			expectedFetches:  1,
		},
		{
			name:   "one valid and one not found results in a node an a nil entry",
			inputs: []string{"a", "unknown"},
			expectedNodes: []*pregel.Node{
				&nodeA,
				nil,
			},
			expectedErrors: []error{
				nil,
				nil,
			},
			expectedNodeGets: 2,
			expectedFetches:  1,
		},
		{
			name:   "duplicate requests are not made by the middleware",
			inputs: []string{"a", "a"},
			expectedNodes: []*pregel.Node{
				&nodeA,
				&nodeA,
			},
			expectedErrors: []error{
				nil,
				nil,
			},
			expectedNodeGets: 1,
			expectedFetches:  1,
		},
		{
			name:   "batches of 10 are executed by the middleware",
			inputs: []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"},
			expectedNodes: []*pregel.Node{
				&nodeA,
				&nodeB,
				&nodeC,
				&nodeD,
				&nodeE,
				&nodeF,
				&nodeG,
				&nodeH,
				&nodeI,
				&nodeJ,
				&nodeK,
				&nodeL,
			},
			expectedErrors:   make([]error, 12),
			expectedNodeGets: 12,
			expectedFetches:  2,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			th := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				loader := FromContext(r.Context())
				actualNodes, actualErrors := loader.LoadAll(test.inputs)
				if len(actualNodes) != len(test.expectedNodes) {
					t.Fatalf("expected %d nodes, got %d nodes", len(test.expectedNodes), len(actualNodes))
				}
				if len(actualErrors) != len(test.expectedErrors) {
					t.Fatalf("expected %d errors, got %d errors", len(test.expectedErrors), len(actualErrors))
				}
				for i, an := range actualNodes {
					en := test.expectedNodes[i]
					if an == nil && en == nil {
						continue
					}
					if an == nil && en != nil {
						t.Errorf("expected node %d to be %s, but was nil", i, en.ID)
						continue
					}
					if an != nil && en == nil {
						t.Errorf("expected node %d to be nil, but was %s", i, an.ID)
						continue
					}
					if an.ID != en.ID {
						t.Errorf("expected node %d to be %s, but was %s", i, en.ID, an.ID)
					}
				}
				for i, ae := range actualErrors {
					ee := test.expectedErrors[i]
					if ae == nil && ee == nil {
						continue
					}
					if ae != ee {
						t.Errorf("expected error %d to be %v, but was %v", i, ee, ae)
					}
				}
			})
			ng := &inMemoryNodeGetter{
				nodes: map[string]pregel.Node{
					"a": nodeA,
					"b": nodeB,
					"c": nodeC,
					"d": nodeD,
					"e": nodeE,
					"f": nodeF,
					"g": nodeG,
					"h": nodeH,
					"i": nodeI,
					"j": nodeJ,
					"k": nodeK,
					"l": nodeL,
				},
			}
			var stats NodeDataLoaderStats
			statsLogger := func(s NodeDataLoaderStats) {
				stats = s
			}
			h := WithNodeDataloaderMiddleware(ng, statsLogger, th)

			w := httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodPost, "/query", nil)
			h.ServeHTTP(w, r)

			if stats.NodesLoaded != test.expectedNodeGets {
				t.Errorf("expected %d node gets, got %d", test.expectedNodeGets, stats.NodesLoaded)
			}
			if stats.FetchesMade != test.expectedFetches {
				t.Errorf("expected %d fetches, got %d", test.expectedFetches, stats.FetchesMade)
			}
		})
	}
}
