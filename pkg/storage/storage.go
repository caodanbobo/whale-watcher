package storage

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"
	"whale-watcher/pkg/model"
	"whale-watcher/pkg/utils"
)

const MagicNumber = 0xBEEF

func AppendBlockToDB(filename string, block model.Block) error {
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	height := uint64(utils.HexToInt64(block.Number))

	txCount := uint32(len(block.Transactions))

	cleanHash := strings.TrimPrefix(block.Hash, "0x")

	hashBytes, err := hex.DecodeString(cleanHash)
	if err != nil {
		return fmt.Errorf("hash decode error: %v", err)
	}
	if len(hashBytes) != 32 {
		return fmt.Errorf("invalid hash length: %d", len(hashBytes))
	}

	if err := binary.Write(f, binary.LittleEndian, uint16(MagicNumber)); err != nil {
		return err
	}
	if err := binary.Write(f, binary.LittleEndian, height); err != nil {
		return err
	}
	// 写入 TxCount (4 bytes)
	if err := binary.Write(f, binary.LittleEndian, txCount); err != nil {
		return err
	}
	if _, err := f.Write(hashBytes); err != nil {
		return err
	}
	return nil

}

func ReadBlocksFromDB(filename string) {

	f, err := os.Open(filename)
	if err != nil {
		fmt.Println("打开文件失败:", err)
		return
	}
	defer f.Close()

	fmt.Println("📂 开始读取本地数据库...")

	for {
		var magic uint16
		err := binary.Read(f, binary.LittleEndian, &magic)
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println("读取出错:", err)
			return
		}
		if MagicNumber != magic {
			fmt.Println("❌ 数据损坏！MagicNumber 不匹配")
			return
		}

		// 2. 读取 Height
		var height uint64
		binary.Read(f, binary.LittleEndian, &height)

		// 3. 读取 TxCount
		var txCount uint32
		binary.Read(f, binary.LittleEndian, &txCount)

		// 4. 读取 Hash
		// 创建一个 32 字节的缓冲区
		hashBuf := make([]byte, 32)
		f.Read(hashBuf)

		// 打印还原出来的数据
		fmt.Printf("📜 [DB] Block #%d | Txs: %d | Hash: 0x%x\n",
			height, txCount, hashBuf)
	}
	fmt.Println("✅ 读取完成")
}
