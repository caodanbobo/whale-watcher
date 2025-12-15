package utils

import (
	"fmt"
	"math/big"
	"strconv"
	"strings"
)

func HexToInt64(hexStr string) int64 {
	cleanStr := strings.TrimPrefix(hexStr, "0x")

	val, err := strconv.ParseInt(cleanStr, 16, 64)
	if err != nil {
		fmt.Println("Hex转换失败", err)
		return 0
	}
	return val
}

func HexToBigInt(hexStr string) *big.Int {
	n := new(big.Int)

	cleanStr := strings.TrimPrefix(hexStr, "0x")

	n, ok := n.SetString(cleanStr, 16)
	if !ok {
		return new(big.Int)
	}
	return n
}
