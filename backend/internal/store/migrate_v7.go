// Package store 数据库迁移 v7：创建 stream_events 表。
package store

import (
	"github.com/helloxz/zacp/internal/model"
	"gorm.io/gorm"
)

// migrateV7 创建 stream_events 表，用于 ZACP 进程重启后恢复当前进行中的 turn 事件。
// 该表为临时缓存：turn 正常结束后由 bridge 清理；崩溃/重启后由 resync 逻辑补齐为历史消息。
func migrateV7(db *gorm.DB) error {
	return db.AutoMigrate(&model.StreamEvent{})
}
