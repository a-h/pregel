package graph

import (
	"testing"

	"github.com/a-h/pregel/graph/gqlid"

	"github.com/a-h/pregel"
)

func TestFilterEdges(t *testing.T) {
	edges := []pregel.Edge{
		pregel.NewEdge("a"),
		pregel.NewEdge("b"),
		pregel.NewEdge("c"),
		pregel.NewEdge("d"),
		pregel.NewEdge("e"),
		pregel.NewEdge("f"),
	}

	tests := []struct {
		name                    string
		edges                   []pregel.Edge
		first                   int
		after                   string
		expectedIDs             []string
		expectedHasPreviousPage bool
		expectedHasNextPage     bool
	}{
		{
			name: "no filtering, no edges",
		},
		{
			name:  "no edges, with first",
			first: 100,
		},
		{
			name:  "no edges, with after",
			after: gqlid.Encode("a"),
		},
		{
			name:  "no edges, with first and after",
			first: 100,
			after: gqlid.Encode("a"),
		},
		{
			name:                    "take the first edge",
			first:                   1,
			after:                   "",
			edges:                   edges,
			expectedIDs:             []string{"a"},
			expectedHasPreviousPage: false,
			expectedHasNextPage:     true,
		},
		{
			name:                    "skip first edge, take the next",
			first:                   1,
			after:                   gqlid.Encode("a"),
			edges:                   edges,
			expectedIDs:             []string{"b"},
			expectedHasPreviousPage: true,
			expectedHasNextPage:     true,
		},
		{
			name:                    "skip first 2 edges, take several after",
			first:                   2,
			after:                   gqlid.Encode("b"),
			edges:                   edges,
			expectedIDs:             []string{"c", "d"},
			expectedHasPreviousPage: true,
			expectedHasNextPage:     true,
		},
		{
			name:                    "skip to the end, try and take one",
			first:                   2,
			after:                   gqlid.Encode("f"),
			edges:                   edges,
			expectedIDs:             []string{},
			expectedHasPreviousPage: true,
			expectedHasNextPage:     false,
		},
		{
			name:                    "try to take more than are available",
			first:                   100,
			edges:                   edges,
			expectedIDs:             []string{"a", "b", "c", "d", "e", "f"},
			expectedHasPreviousPage: false,
			expectedHasNextPage:     false,
		},
		{
			name:                    "try to take less than are available",
			first:                   5,
			edges:                   edges,
			expectedIDs:             []string{"a", "b", "c", "d", "e"},
			expectedHasPreviousPage: false,
			expectedHasNextPage:     true,
		},
		{
			name:                    "paging can be ignored",
			first:                   0,
			after:                   gqlid.Encode("a"),
			edges:                   edges,
			expectedIDs:             []string{"b", "c", "d", "e", "f"},
			expectedHasPreviousPage: true,
			expectedHasNextPage:     false,
		},
		{
			name:                    "paging and skipping be ignored",
			first:                   0,
			after:                   "",
			edges:                   edges,
			expectedIDs:             []string{"a", "b", "c", "d", "e", "f"},
			expectedHasPreviousPage: false,
			expectedHasNextPage:     false,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			var after *string
			if test.after != "" {
				after = &test.after
			}
			filtered, pageInfo := filterEdges(test.edges, test.first, after)
			if len(filtered) != len(test.expectedIDs) {
				t.Fatalf("expected %d ids, got %d", len(test.expectedIDs), len(filtered))
			}
			for i, expectedID := range test.expectedIDs {
				actualID := filtered[i].ID
				if actualID != expectedID {
					t.Errorf("expected ID %d to be %s, but was %s", i, expectedID, actualID)
				}
			}
			if pageInfo.StartCursor != nil && len(test.expectedIDs) == 0 {
				t.Errorf("expected no edges, but got a start cursor of %s", *pageInfo.StartCursor)
			}
			if len(test.expectedIDs) > 0 {
				expectedStartCursor := gqlid.Encode(test.expectedIDs[0])
				expectedEndCursor := gqlid.Encode(test.expectedIDs[len(test.expectedIDs)-1])
				if *pageInfo.StartCursor != expectedStartCursor {
					t.Errorf("expected start cursor of %v (%v), got %v", expectedStartCursor, test.expectedIDs[0], *pageInfo.StartCursor)
				}
				if *pageInfo.EndCursor != expectedEndCursor {
					t.Errorf("expected end cursor %v (%v), got %v", expectedEndCursor, test.expectedIDs[len(test.expectedIDs)-1], *pageInfo.EndCursor)
				}
			}
			if pageInfo.EndCursor != nil && len(test.expectedIDs) == 0 {
				t.Errorf("expected no edges, but got an end cursor of %s", *pageInfo.EndCursor)
			}
			if test.expectedHasPreviousPage != pageInfo.HasPreviousPage {
				t.Errorf("execpted to have a previous page = %v, but was %v", test.expectedHasPreviousPage, pageInfo.HasPreviousPage)
			}
			if test.expectedHasNextPage != pageInfo.HasNextPage {
				t.Errorf("execpted to have a next page = %v, but was %v", test.expectedHasNextPage, pageInfo.HasNextPage)
			}
		})
	}
}
