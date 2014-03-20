package setop

import (
	"bytes"
	"fmt"
	"math/big"
	"reflect"
	"sort"
	"testing"
)

type testSkipper struct {
	pairs []tP
	index int
}
type tP [2]string

func (self *testSkipper) Skip(min []byte, inc bool) (result *SetOpResult, err error) {
	lt := 1
	if inc {
		lt = 0
	}
	for self.index < len(self.pairs) && bytes.Compare([]byte(self.pairs[self.index][0]), min) < lt {
		self.index++
	}
	if self.index < len(self.pairs) {
		return &SetOpResult{
			Key:    []byte(self.pairs[self.index][0]),
			Values: [][]byte{[]byte(self.pairs[self.index][1])},
		}, nil
	}
	return nil, nil
}

var testSets = map[string]*testSkipper{
	"a": &testSkipper{
		pairs: []tP{
			tP{"a", "a"},
			tP{"b", "b"},
			tP{"c", "c"},
		},
	},
	"b": &testSkipper{
		pairs: []tP{
			tP{"a", "a"},
			tP{"c", "c"},
			tP{"d", "d"},
		},
	},
	"c": &testSkipper{
		pairs: []tP{
			tP{"a", "a"},
			tP{"b", "b"},
			tP{"c", "c"},
		},
	},
	"d": &testSkipper{
		pairs: []tP{
			tP{"c", "c"},
		},
	},
	"e": &testSkipper{
		pairs: []tP{
			tP{"c", "c"},
			tP{"d", "d"},
		},
	},
}

func resetSets() {
	for _, set := range testSets {
		set.index = 0
	}
}

func findTestSet(b []byte) Skipper {
	set, ok := testSets[string(b)]
	if !ok {
		panic(fmt.Errorf("couldn't find test set %s", string(b)))
	}
	return set
}

func collect(t *testing.T, expr string) []*SetOpResult {
	s, err := NewSetOpParser(expr).Parse()
	if err != nil {
		t.Fatal(err)
	}
	se := &SetExpression{
		Op: s,
	}
	var collector []*SetOpResult
	se.Each(findTestSet, func(res *SetOpResult) {
		collector = append(collector, res)
	})
	return collector
}

type testResults []*SetOpResult

func (self testResults) Len() int {
	return len(self)
}
func (self testResults) Swap(i, j int) {
	self[i], self[j] = self[j], self[i]
}
func (self testResults) Less(i, j int) bool {
	return bytes.Compare(self[i].Key, self[j].Key) < 0
}

func diff(merger mergeFunc, sets [][]tP, weights []float64) (result []*SetOpResult) {
	hashes := make([]map[string][]byte, len(sets))
	for index, set := range sets {
		hashes[index] = make(map[string][]byte)
		for _, pair := range set {
			hashes[index][pair[0]] = []byte(pair[1])
		}
	}
	resultMap := make(map[string][][]byte)
	for k, v := range hashes[0] {
		resultMap[k] = merger(resultMap[k], [][]byte{v}, weights[0])
	}
	for _, m := range hashes[1:] {
		for k, _ := range m {
			delete(resultMap, k)
		}
	}
	for k, v := range resultMap {
		result = append(result, &SetOpResult{
			Key:    []byte(k),
			Values: v,
		})
	}
	sort.Sort(testResults(result))
	return
}

func inter(merger mergeFunc, sets [][]tP, weights []float64) (result []*SetOpResult) {
	hashes := make([]map[string][]byte, len(sets))
	for index, set := range sets {
		hashes[index] = make(map[string][]byte)
		for _, pair := range set {
			hashes[index][pair[0]] = []byte(pair[1])
		}
	}
	resultMap := make(map[string][][]byte)
	for index, m := range hashes {
		for k, v := range m {
			isOk := true
			for _, m2 := range hashes {
				_, ex := m2[k]
				isOk = isOk && ex
			}
			if isOk {
				resultMap[k] = merger(resultMap[k], [][]byte{v}, weights[index])
			}
		}
	}
	for k, v := range resultMap {
		result = append(result, &SetOpResult{
			Key:    []byte(k),
			Values: v,
		})
	}
	sort.Sort(testResults(result))
	return
}

func xor(merger mergeFunc, sets [][]tP, weights []float64) (result []*SetOpResult) {
	hashes := make([]map[string][]byte, len(sets))
	for index, set := range sets {
		hashes[index] = make(map[string][]byte)
		for _, pair := range set {
			hashes[index][pair[0]] = []byte(pair[1])
		}
	}
	matchMap := make(map[string]int)
	resultMap := make(map[string][][]byte)
	for index, m := range hashes {
		for k, v := range m {
			resultMap[k] = merger(resultMap[k], [][]byte{v}, weights[index])
			matchMap[k] += 1
		}
	}
	for k, v := range resultMap {
		if matchMap[k] == 1 {
			result = append(result, &SetOpResult{
				Key:    []byte(k),
				Values: v,
			})
		}
	}
	sort.Sort(testResults(result))
	return
}

func union(merger mergeFunc, sets [][]tP, weights []float64) (result []*SetOpResult) {
	hashes := make([]map[string][]byte, len(sets))
	for index, set := range sets {
		hashes[index] = make(map[string][]byte)
		for _, pair := range set {
			hashes[index][pair[0]] = []byte(pair[1])
		}
	}
	resultMap := make(map[string][][]byte)
	for index, m := range hashes {
		for k, v := range m {
			resultMap[k] = merger(resultMap[k], [][]byte{v}, weights[index])
		}
	}
	for k, v := range resultMap {
		result = append(result, &SetOpResult{
			Key:    []byte(k),
			Values: v,
		})
	}
	sort.Sort(testResults(result))
	return
}

func TestBigIntXor(t *testing.T) {
	found := bigIntXor([][]byte{EncodeBigInt(big.NewInt(15))}, [][]byte{EncodeBigInt(big.NewInt(1)), EncodeBigInt(big.NewInt(2)), EncodeBigInt(big.NewInt(4))}, 1)
	expected := [][]byte{EncodeBigInt(big.NewInt(8))}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", fmt.Sprint(DecodeBigInt(found[0])), fmt.Sprint(DecodeBigInt(expected[0])))
	}
	found = bigIntXor(nil, [][]byte{EncodeBigInt(big.NewInt(15)), EncodeBigInt(big.NewInt(1)), EncodeBigInt(big.NewInt(2)), EncodeBigInt(big.NewInt(4))}, 1)
	expected = [][]byte{EncodeBigInt(big.NewInt(8))}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", fmt.Sprint(DecodeBigInt(found[0])), fmt.Sprint(DecodeBigInt(expected[0])))
	}
	found = bigIntXor([][]byte{EncodeBigInt(big.NewInt(15))}, [][]byte{EncodeBigInt(big.NewInt(3))}, 2)
	expected = [][]byte{EncodeBigInt(big.NewInt(9))}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", fmt.Sprint(DecodeBigInt(found[0])), fmt.Sprint(DecodeBigInt(expected[0])))
	}
}

func TestBigIntRem(t *testing.T) {
	found := bigIntRem([][]byte{EncodeBigInt(big.NewInt(50))}, [][]byte{EncodeBigInt(big.NewInt(30)), EncodeBigInt(big.NewInt(11)), EncodeBigInt(big.NewInt(7))}, 1)
	expected := [][]byte{EncodeBigInt(big.NewInt(2))}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", fmt.Sprint(DecodeBigInt(found[0])), fmt.Sprint(DecodeBigInt(expected[0])))
	}
	found = bigIntRem(nil, [][]byte{EncodeBigInt(big.NewInt(50)), EncodeBigInt(big.NewInt(30)), EncodeBigInt(big.NewInt(11)), EncodeBigInt(big.NewInt(7))}, 1)
	expected = [][]byte{EncodeBigInt(big.NewInt(2))}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", fmt.Sprint(DecodeBigInt(found[0])), fmt.Sprint(DecodeBigInt(expected[0])))
	}
	found = bigIntRem([][]byte{EncodeBigInt(big.NewInt(50))}, [][]byte{EncodeBigInt(big.NewInt(7))}, 2)
	expected = [][]byte{EncodeBigInt(big.NewInt(8))}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", fmt.Sprint(DecodeBigInt(found[0])), fmt.Sprint(DecodeBigInt(expected[0])))
	}
}

func TestBigIntMul(t *testing.T) {
	found := bigIntMul([][]byte{EncodeBigInt(big.NewInt(1))}, [][]byte{EncodeBigInt(big.NewInt(2)), EncodeBigInt(big.NewInt(3)), EncodeBigInt(big.NewInt(4))}, 1)
	expected := [][]byte{EncodeBigInt(big.NewInt(24))}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", fmt.Sprint(DecodeBigInt(found[0])), fmt.Sprint(DecodeBigInt(expected[0])))
	}
	found = bigIntMul(nil, [][]byte{EncodeBigInt(big.NewInt(1)), EncodeBigInt(big.NewInt(2)), EncodeBigInt(big.NewInt(3)), EncodeBigInt(big.NewInt(4))}, 1)
	expected = [][]byte{EncodeBigInt(big.NewInt(24))}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", fmt.Sprint(DecodeBigInt(found[0])), fmt.Sprint(DecodeBigInt(expected[0])))
	}
	found = bigIntMul([][]byte{EncodeBigInt(big.NewInt(1))}, [][]byte{EncodeBigInt(big.NewInt(2))}, 2)
	expected = [][]byte{EncodeBigInt(big.NewInt(4))}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", fmt.Sprint(DecodeBigInt(found[0])), fmt.Sprint(DecodeBigInt(expected[0])))
	}
}

func TestBigIntOr(t *testing.T) {
	found := bigIntOr([][]byte{EncodeBigInt(big.NewInt(1))}, [][]byte{EncodeBigInt(big.NewInt(2)), EncodeBigInt(big.NewInt(3)), EncodeBigInt(big.NewInt(4))}, 1)
	expected := [][]byte{EncodeBigInt(big.NewInt(7))}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", fmt.Sprint(DecodeBigInt(found[0])), fmt.Sprint(DecodeBigInt(expected[0])))
	}
	found = bigIntOr(nil, [][]byte{EncodeBigInt(big.NewInt(1)), EncodeBigInt(big.NewInt(2)), EncodeBigInt(big.NewInt(3)), EncodeBigInt(big.NewInt(4))}, 1)
	expected = [][]byte{EncodeBigInt(big.NewInt(7))}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", fmt.Sprint(DecodeBigInt(found[0])), fmt.Sprint(DecodeBigInt(expected[0])))
	}
	found = bigIntOr([][]byte{EncodeBigInt(big.NewInt(1))}, [][]byte{EncodeBigInt(big.NewInt(2))}, 2)
	expected = [][]byte{EncodeBigInt(big.NewInt(5))}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", fmt.Sprint(DecodeBigInt(found[0])), fmt.Sprint(DecodeBigInt(expected[0])))
	}
}

func TestBigMod(t *testing.T) {
	found := bigIntMod([][]byte{EncodeBigInt(big.NewInt(50))}, [][]byte{EncodeBigInt(big.NewInt(30)), EncodeBigInt(big.NewInt(7)), EncodeBigInt(big.NewInt(4))}, 1)
	expected := [][]byte{EncodeBigInt(big.NewInt(2))}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", fmt.Sprint(DecodeBigInt(found[0])), fmt.Sprint(DecodeBigInt(expected[0])))
	}
	found = bigIntMod(nil, [][]byte{EncodeBigInt(big.NewInt(50)), EncodeBigInt(big.NewInt(30)), EncodeBigInt(big.NewInt(7)), EncodeBigInt(big.NewInt(4))}, 1)
	expected = [][]byte{EncodeBigInt(big.NewInt(2))}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", fmt.Sprint(DecodeBigInt(found[0])), fmt.Sprint(DecodeBigInt(expected[0])))
	}
	found = bigIntMod([][]byte{EncodeBigInt(big.NewInt(50))}, [][]byte{EncodeBigInt(big.NewInt(15))}, 2)
	expected = [][]byte{EncodeBigInt(big.NewInt(20))}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", fmt.Sprint(DecodeBigInt(found[0])), fmt.Sprint(DecodeBigInt(expected[0])))
	}
}

func TestBigIntDiv(t *testing.T) {
	found := bigIntDiv([][]byte{EncodeBigInt(big.NewInt(48))}, [][]byte{EncodeBigInt(big.NewInt(2)), EncodeBigInt(big.NewInt(3)), EncodeBigInt(big.NewInt(4))}, 1)
	expected := [][]byte{EncodeBigInt(big.NewInt(2))}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", fmt.Sprint(DecodeBigInt(found[0])), fmt.Sprint(DecodeBigInt(expected[0])))
	}
	found = bigIntDiv(nil, [][]byte{EncodeBigInt(big.NewInt(48)), EncodeBigInt(big.NewInt(2)), EncodeBigInt(big.NewInt(3)), EncodeBigInt(big.NewInt(4))}, 1)
	expected = [][]byte{EncodeBigInt(big.NewInt(2))}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", fmt.Sprint(DecodeBigInt(found[0])), fmt.Sprint(DecodeBigInt(expected[0])))
	}
	found = bigIntDiv([][]byte{EncodeBigInt(big.NewInt(48))}, [][]byte{EncodeBigInt(big.NewInt(2))}, 2)
	expected = [][]byte{EncodeBigInt(big.NewInt(12))}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", fmt.Sprint(DecodeBigInt(found[0])), fmt.Sprint(DecodeBigInt(expected[0])))
	}
}

func TestBigIntAndNot(t *testing.T) {
	found := bigIntAndNot([][]byte{EncodeBigInt(big.NewInt(15))}, [][]byte{EncodeBigInt(big.NewInt(1)), EncodeBigInt(big.NewInt(2)), EncodeBigInt(big.NewInt(4))}, 1)
	expected := [][]byte{EncodeBigInt(big.NewInt(8))}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", fmt.Sprint(DecodeBigInt(found[0])), fmt.Sprint(DecodeBigInt(expected[0])))
	}
	found = bigIntAndNot(nil, [][]byte{EncodeBigInt(big.NewInt(15)), EncodeBigInt(big.NewInt(1)), EncodeBigInt(big.NewInt(2)), EncodeBigInt(big.NewInt(4))}, 1)
	expected = [][]byte{EncodeBigInt(big.NewInt(8))}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", fmt.Sprint(DecodeBigInt(found[0])), fmt.Sprint(DecodeBigInt(expected[0])))
	}
	found = bigIntAndNot([][]byte{EncodeBigInt(big.NewInt(15))}, [][]byte{EncodeBigInt(big.NewInt(2))}, 2)
	expected = [][]byte{EncodeBigInt(big.NewInt(11))}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", fmt.Sprint(DecodeBigInt(found[0])), fmt.Sprint(DecodeBigInt(expected[0])))
	}
}

func TestBigIntAdd(t *testing.T) {
	found := bigIntAdd([][]byte{EncodeBigInt(big.NewInt(1))}, [][]byte{EncodeBigInt(big.NewInt(2)), EncodeBigInt(big.NewInt(3)), EncodeBigInt(big.NewInt(4))}, 1)
	expected := [][]byte{EncodeBigInt(big.NewInt(10))}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", fmt.Sprint(DecodeBigInt(found[0])), fmt.Sprint(DecodeBigInt(expected[0])))
	}
	found = bigIntAdd(nil, [][]byte{EncodeBigInt(big.NewInt(1)), EncodeBigInt(big.NewInt(2)), EncodeBigInt(big.NewInt(3)), EncodeBigInt(big.NewInt(4))}, 1)
	expected = [][]byte{EncodeBigInt(big.NewInt(10))}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", fmt.Sprint(DecodeBigInt(found[0])), fmt.Sprint(DecodeBigInt(expected[0])))
	}
	found = bigIntAdd([][]byte{EncodeBigInt(big.NewInt(1))}, [][]byte{EncodeBigInt(big.NewInt(2))}, 2)
	expected = [][]byte{EncodeBigInt(big.NewInt(5))}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", fmt.Sprint(DecodeBigInt(found[0])), fmt.Sprint(DecodeBigInt(expected[0])))
	}
}

func TestBigIntAnd(t *testing.T) {
	found := bigIntAnd([][]byte{EncodeBigInt(big.NewInt(1))}, [][]byte{EncodeBigInt(big.NewInt(2)), EncodeBigInt(big.NewInt(3)), EncodeBigInt(big.NewInt(4))}, 1)
	expected := [][]byte{EncodeBigInt(big.NewInt(0))}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", fmt.Sprint(DecodeBigInt(found[0])), fmt.Sprint(DecodeBigInt(expected[0])))
	}
	found = bigIntAnd([][]byte{EncodeBigInt(big.NewInt(1)), EncodeBigInt(big.NewInt(3))}, [][]byte{EncodeBigInt(big.NewInt(3)), EncodeBigInt(big.NewInt(5))}, 1)
	expected = [][]byte{EncodeBigInt(big.NewInt(1))}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", fmt.Sprint(DecodeBigInt(found[0])), fmt.Sprint(DecodeBigInt(expected[0])))
	}
	found = bigIntAnd(nil, [][]byte{EncodeBigInt(big.NewInt(1)), EncodeBigInt(big.NewInt(2)), EncodeBigInt(big.NewInt(3)), EncodeBigInt(big.NewInt(4))}, 1)
	expected = [][]byte{EncodeBigInt(big.NewInt(0))}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", fmt.Sprint(DecodeBigInt(found[0])), fmt.Sprint(DecodeBigInt(expected[0])))
	}
	found = bigIntAnd(nil, [][]byte{EncodeBigInt(big.NewInt(1)), EncodeBigInt(big.NewInt(3)), EncodeBigInt(big.NewInt(3)), EncodeBigInt(big.NewInt(5))}, 1)
	expected = [][]byte{EncodeBigInt(big.NewInt(1))}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", fmt.Sprint(DecodeBigInt(found[0])), fmt.Sprint(DecodeBigInt(expected[0])))
	}
	found = bigIntAnd([][]byte{EncodeBigInt(big.NewInt(15))}, [][]byte{EncodeBigInt(big.NewInt(3))}, 2)
	expected = [][]byte{EncodeBigInt(big.NewInt(6))}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", fmt.Sprint(DecodeBigInt(found[0])), fmt.Sprint(DecodeBigInt(expected[0])))
	}
}

func TestFloatMul(t *testing.T) {
	found := floatMul([][]byte{EncodeFloat64(1)}, [][]byte{EncodeFloat64(2), EncodeFloat64(3), EncodeFloat64(4)}, 1)
	expected := [][]byte{EncodeFloat64(24)}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", fmt.Sprint(DecodeFloat64(found[0])), fmt.Sprint(DecodeFloat64(expected[0])))
	}
	found = floatMul(nil, [][]byte{EncodeFloat64(1), EncodeFloat64(2), EncodeFloat64(3), EncodeFloat64(4)}, 1)
	expected = [][]byte{EncodeFloat64(24)}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", fmt.Sprint(DecodeFloat64(found[0])), fmt.Sprint(DecodeFloat64(expected[0])))
	}
	found = floatMul([][]byte{EncodeFloat64(2)}, [][]byte{EncodeFloat64(2)}, 2)
	expected = [][]byte{EncodeFloat64(8)}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", fmt.Sprint(DecodeFloat64(found[0])), fmt.Sprint(DecodeFloat64(expected[0])))
	}
}

func TestFloatDiv(t *testing.T) {
	found := floatDiv([][]byte{EncodeFloat64(48)}, [][]byte{EncodeFloat64(2), EncodeFloat64(3), EncodeFloat64(4)}, 1)
	expected := [][]byte{EncodeFloat64(2)}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", fmt.Sprint(DecodeFloat64(found[0])), fmt.Sprint(DecodeFloat64(expected[0])))
	}
	found = floatDiv(nil, [][]byte{EncodeFloat64(48), EncodeFloat64(2), EncodeFloat64(3), EncodeFloat64(4)}, 1)
	expected = [][]byte{EncodeFloat64(2)}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", fmt.Sprint(DecodeFloat64(found[0])), fmt.Sprint(DecodeFloat64(expected[0])))
	}
	found = floatDiv([][]byte{EncodeFloat64(48)}, [][]byte{EncodeFloat64(2)}, 2)
	expected = [][]byte{EncodeFloat64(12)}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", fmt.Sprint(DecodeFloat64(found[0])), fmt.Sprint(DecodeFloat64(expected[0])))
	}
}

func TestFloatSum(t *testing.T) {
	found := floatSum([][]byte{EncodeFloat64(1)}, [][]byte{EncodeFloat64(2), EncodeFloat64(3), EncodeFloat64(4)}, 1)
	expected := [][]byte{EncodeFloat64(10)}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", found, expected)
	}
	found = floatSum(nil, [][]byte{EncodeFloat64(1), EncodeFloat64(2), EncodeFloat64(3), EncodeFloat64(4)}, 1)
	expected = [][]byte{EncodeFloat64(10)}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", found, expected)
	}
	found = floatSum([][]byte{EncodeFloat64(1)}, [][]byte{EncodeFloat64(2)}, 2)
	expected = [][]byte{EncodeFloat64(5)}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", found, expected)
	}
}

func TestIntegerMul(t *testing.T) {
	found := integerMul([][]byte{EncodeInt64(1)}, [][]byte{EncodeInt64(2), EncodeInt64(3), EncodeInt64(4)}, 1)
	expected := [][]byte{EncodeInt64(24)}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", found, expected)
	}
	found = integerMul(nil, [][]byte{EncodeInt64(1), EncodeInt64(2), EncodeInt64(3), EncodeInt64(4)}, 1)
	expected = [][]byte{EncodeInt64(24)}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", found, expected)
	}
	found = integerMul([][]byte{EncodeInt64(2)}, [][]byte{EncodeInt64(2)}, 2)
	expected = [][]byte{EncodeInt64(8)}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", found, expected)
	}
}

func TestIntegerDiv(t *testing.T) {
	found := integerDiv([][]byte{EncodeInt64(48)}, [][]byte{EncodeInt64(2), EncodeInt64(3), EncodeInt64(4)}, 1)
	expected := [][]byte{EncodeInt64(2)}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", found, expected)
	}
	found = integerDiv(nil, [][]byte{EncodeInt64(48), EncodeInt64(2), EncodeInt64(3), EncodeInt64(4)}, 1)
	expected = [][]byte{EncodeInt64(2)}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", found, expected)
	}
	found = integerDiv([][]byte{EncodeInt64(48)}, [][]byte{EncodeInt64(2)}, 2)
	expected = [][]byte{EncodeInt64(12)}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", found, expected)
	}
}

func TestIntegerSum(t *testing.T) {
	found := integerSum([][]byte{EncodeInt64(1)}, [][]byte{EncodeInt64(2), EncodeInt64(3), EncodeInt64(4)}, 1)
	expected := [][]byte{EncodeInt64(10)}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", found, expected)
	}
	found = integerSum(nil, [][]byte{EncodeInt64(1), EncodeInt64(2), EncodeInt64(3), EncodeInt64(4)}, 1)
	expected = [][]byte{EncodeInt64(10)}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", found, expected)
	}
	found = integerSum([][]byte{EncodeInt64(1)}, [][]byte{EncodeInt64(2)}, 2)
	expected = [][]byte{EncodeInt64(5)}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", found, expected)
	}
}

func TestConCat(t *testing.T) {
	found := conCat([][]byte{[]byte{1}}, [][]byte{[]byte{2}, []byte{3}, []byte{4}}, 1)
	expected := [][]byte{[]byte{1, 2, 3, 4}}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", found, expected)
	}
	found = conCat(nil, [][]byte{[]byte{1}, []byte{2}, []byte{3}, []byte{4}}, 1)
	expected = [][]byte{[]byte{1, 2, 3, 4}}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", found, expected)
	}
	found = conCat([][]byte{[]byte{1}}, [][]byte{[]byte{2}}, 2)
	expected = [][]byte{[]byte{1, 2, 2}}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", found, expected)
	}
}

func TestAppend(t *testing.T) {
	found := _append([][]byte{[]byte{1}}, [][]byte{[]byte{2}, []byte{3}, []byte{4}}, 1)
	expected := [][]byte{[]byte{1}, []byte{2}, []byte{3}, []byte{4}}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", found, expected)
	}
	found = _append(nil, [][]byte{[]byte{1}, []byte{2}, []byte{3}, []byte{4}}, 1)
	expected = [][]byte{[]byte{1}, []byte{2}, []byte{3}, []byte{4}}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", found, expected)
	}
	found = _append([][]byte{[]byte{1}}, [][]byte{[]byte{2}}, 2)
	expected = [][]byte{[]byte{1}, []byte{2}, []byte{2}}
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", found, expected)
	}
}

func TestUnion(t *testing.T) {
	resetSets()
	found := collect(t, "(U a b)")
	expected := union(_append, [][]tP{testSets["a"].pairs, testSets["b"].pairs}, []float64{1, 1})
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", found, expected)
	}
}

func TestInter1(t *testing.T) {
	resetSets()
	found := collect(t, "(I a b)")
	expected := inter(_append, [][]tP{testSets["a"].pairs, testSets["b"].pairs}, []float64{1, 1})
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", found, expected)
	}
}

func TestInter2(t *testing.T) {
	resetSets()
	found := collect(t, "(I c d e)")
	expected := inter(_append, [][]tP{testSets["c"].pairs, testSets["d"].pairs, testSets["e"].pairs}, []float64{1, 1, 1})
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", found, expected)
	}
}

func TestDiff(t *testing.T) {
	resetSets()
	found := collect(t, "(D a b)")
	expected := diff(_append, [][]tP{testSets["a"].pairs, testSets["b"].pairs}, []float64{1, 1})
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", found, expected)
	}
}

func TestXor(t *testing.T) {
	resetSets()
	found := collect(t, "(X a b)")
	expected := xor(_append, [][]tP{testSets["a"].pairs, testSets["b"].pairs}, []float64{1, 1})
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", found, expected)
	}
}
