package utils

import (
	"fmt"
	"math/big"
	"strconv"
	"strings"
)

const hexPrefix = "0x"

// HexToInt64 converts a hexadecimal string to a 64-bit integer
// Returns 0 if conversion fails
func HexToInt64(hexStr string) int64 {
	cleanStr := strings.TrimPrefix(hexStr, hexPrefix)

	val, err := strconv.ParseInt(cleanStr, 16, 64)
	if err != nil {
		fmt.Println("Hex conversion failed:", err)
		return 0
	}

	return val
}

// HexToBigInt converts a hexadecimal string to a big.Int
// Returns 0 if conversion fails
func HexToBigInt(hexStr string) *big.Int {
	cleanStr := strings.TrimPrefix(hexStr, hexPrefix)

	result := new(big.Int)
	result, ok := result.SetString(cleanStr, 16)
	if !ok {
		return new(big.Int)
	}

	return result
}
