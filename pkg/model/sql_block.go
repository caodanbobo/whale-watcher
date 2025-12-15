package model

import (
	"time"
)

type SQLBlock struct {
	ID        uint   `gorm:"primaryKey"`
	Height    uint64 `gorm:"uniqueIndex"`
	Hash      string `gorm:"type:char(66);uniqueIndex"`
	TxCount   int
	Timestamp time.Time
	BaseFee   string
}

func (SQLBlock) TableName() string {
	return "blocks"
}
