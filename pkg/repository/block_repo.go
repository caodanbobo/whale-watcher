package repository

import (
	"errors"
	"whale-watcher/pkg/model"

	"gorm.io/gorm"
)

type BlockRepository interface {
	Save(block *model.SQLBlock) error
	GetLastBlock() (*model.SQLBlock, error)
	GetByHeight(height uint64) (*model.SQLBlock, error)
	DeleteByHeightGreaterThan(height uint64) error
	GetMaxHeight() (uint64, error)
}

type blockRepo struct {
	db *gorm.DB
}

func NewBlockRepo(db *gorm.DB) BlockRepository {
	return &blockRepo{
		db: db,
	}
}

func (r *blockRepo) Save(block *model.SQLBlock) error {
	return r.db.Save(block).Error
}

func (r *blockRepo) GetLastBlock() (*model.SQLBlock, error) {
	var block model.SQLBlock
	result := r.db.Order("height desc").First(&block)

	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, result.Error
	}
	return &block, nil
}

func (r *blockRepo) GetByHeight(height uint64) (*model.SQLBlock, error) {
	var block model.SQLBlock
	err := r.db.Where("height=?", height).First(&block).Error
	return &block, err
}

func (r *blockRepo) DeleteByHeightGreaterThan(height uint64) error {
	return r.db.Where("height >?", height).Delete(&model.SQLBlock{}).Error
}

func (r *blockRepo) GetMaxHeight() (uint64, error) {

	var block model.SQLBlock

	err := r.db.Select("height").Order("height desc").First(&block).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return 0, nil
		}
		return 0, errors.ErrUnsupported
	}
	return block.Height, nil

}
