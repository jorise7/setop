package setop

import (
	"bytes"
	"encoding/binary"
	"math/big"
)

func EncodeBigInt(i *big.Int) []byte {
	return i.Bytes()
}

func DecodeBigInt(b []byte) (result *big.Int) {
	result = new(big.Int).SetBytes(b)
	return
}

func EncodeInt64(i int64) []byte {
	result := new(bytes.Buffer)
	if err := binary.Write(result, binary.BigEndian, i); err != nil {
		panic(err)
	}
	return result.Bytes()
}

func DecodeInt64(b []byte) (result int64, err error) {
	err = binary.Read(bytes.NewBuffer(b), binary.BigEndian, &result)
	return
}

func EncodeFloat64(f float64) []byte {
	result := new(bytes.Buffer)
	if err := binary.Write(result, binary.BigEndian, f); err != nil {
		panic(err)
	}
	return result.Bytes()
}

func DecodeFloat64(b []byte) (result float64, err error) {
	err = binary.Read(bytes.NewBuffer(b), binary.BigEndian, &result)
	return
}
