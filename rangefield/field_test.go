package rangefield

import (
	"reflect"
	"testing"
)

func TestDecodeNoParts(t *testing.T) {
	inputs := []string{
		"",
		"dead dog",
		"parent/",
		"node/",
		"child/of/a/dead/rat",
		"parent/of/a/dead/rat",
		"node/of/a/dead/rat",
		"node//missed",
		"node/%%/invalidencoding",
	}
	for _, input := range inputs {
		actual, actualOK := Decode(input)
		if actualOK {
			t.Errorf("%q should not decode", input)
		}
		if actual != nil {
			t.Errorf("%q isn't valid and should return nil", input)
		}
	}
}

func TestRoundTrip(t *testing.T) {
	tests := []struct {
		input   RangeField
		encoded string
	}{
		{
			input:   Node{},
			encoded: "node",
		},
		{
			input:   NodeData{DataType: "nodedatatype"},
			encoded: "node/data/nodedatatype",
		},
		{
			input:   Child{Child: "childid"},
			encoded: "child/childid",
		},
		{
			input:   Child{Child: "中文"},
			encoded: "child/%E4%B8%AD%E6%96%87",
		},
		{
			input:   ChildData{Child: "childid", DataType: "childdatatype"},
			encoded: "child/childid/data/childdatatype",
		},
		{
			input:   Parent{Parent: "parentid"},
			encoded: "parent/parentid",
		},
		{
			input:   ParentData{Parent: "parentid", DataType: "parentdatatype"},
			encoded: "parent/parentid/data/parentdatatype",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(reflect.TypeOf(test.input).Name(), func(t *testing.T) {
			t.Parallel()

			actualEncoded := test.input.Encode()
			if actualEncoded != test.encoded {
				t.Fatalf("expected encoded output '%v', got '%v'", test.encoded, actualEncoded)
			}

			decoded, ok := Decode(test.encoded)
			if !ok {
				t.Fatalf("unable to decode '%v' into expected type %T", test.encoded, test.input)
			}
			if !reflect.DeepEqual(decoded, test.input) {
				t.Fatalf("decoded value '%v' did not equal expected '%v'", decoded, test.input)
			}
		})
	}
}

func TestRangeFieldDecode(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		expected   RangeField
		expectedOK bool
	}{
		{
			name:       "invalid inputs return OK=false",
			input:      "garbage",
			expectedOK: false,
		},
		{
			name:       "unknown node results in OK=false",
			input:      "unknown/node",
			expectedOK: false,
		},
		{
			name:       "node input becomes a Node",
			input:      "node",
			expected:   Node{},
			expectedOK: true,
		},
		{
			name:  "node data becomes a NodeData",
			input: "node/data/datatype",
			expected: NodeData{
				DataType: "datatype",
			},
			expectedOK: true,
		},
		{
			name:       "child becomes a Child",
			input:      "child/childid",
			expected:   Child{Child: "childid"},
			expectedOK: true,
		},
		{
			name:       "child with data becomes a ChildData",
			input:      "child/childid/data/childdatatype",
			expected:   ChildData{Child: "childid", DataType: "childdatatype"},
			expectedOK: true,
		},
		{
			name:       "parent becomes a Parent",
			input:      "parent/parentid",
			expected:   Parent{Parent: "parentid"},
			expectedOK: true,
		},
		{
			name:       "parent with data becomes a ParentData",
			input:      "parent/parentid/data/parentdatatype",
			expected:   ParentData{Parent: "parentid", DataType: "parentdatatype"},
			expectedOK: true,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			actual, actualOK := Decode(test.input)
			if actualOK != test.expectedOK {
				t.Fatalf("expected OK of %v, got %v", test.expectedOK, actualOK)
				return
			}
			if !test.expectedOK {
				return
			}
			if actual != test.expected {
				t.Errorf("expected %v, got %v", test.expected, actual)
				return
			}
		})
	}
}
