package ws

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	acp "github.com/coder/acp-go-sdk"

	"github.com/helloxz/zacp/internal/acp/client"
	"github.com/helloxz/zacp/internal/acp/manager"
	"github.com/helloxz/zacp/internal/acp/providers"
	"github.com/helloxz/zacp/internal/model"
	"github.com/helloxz/zacp/internal/store"
	"github.com/helloxz/zacp/pkg/eventstore"
)

// EventBridge 将 ACP 事件桥接到 WebSocket，并负责 WS prompt 流程的消息落库与权限交互。
type EventBridge struct {
	handler     *Handler
	manager     *manager.Manager
	sessionRepo *store.SessionRepository
	msgRepo     *store.MessageRepository
	streamRepo  *store.StreamEventRepository
	log         *slog.Logger

	// promptOrderTail 让 WS 按收到帧的顺序进入 Manager 全局 FIFO；前置落库耗时
	// 不会导致后来的请求抢先取得执行槽位。
	promptOrderMu   sync.Mutex
	promptOrderTail chan struct{}

	// pendingPermissions 等待前端回传的权限请求（permissionID → 响应通道）。
	// ACP 请求自身带 sessionId，因此多个 session 可独立挂起，不依赖 Agent 的单一当前会话。
	pendingPermissions sync.Map

	// acpSessionMap 缓存 ACP session id → ZACP DB session id。
	// 事件持久化/清理需要按 ZACP session 落库；ACP session id 会在 recoverSession 时改变，
	// 因此 recoverSession 后必须同步更新本映射。
	acpSessionMu sync.RWMutex
	acpSessionMap map[string]uint
}

// permissionTimeout 前端未响应权限请求的等待上限；超时自动取消，避免阻塞 agent turn。
// 5 分钟：给用户足够时间阅读工具调用入参并做安全判断（原 60s 偏短，容易误超时取消）。
const permissionTimeout = 5 * time.Minute

// NewEventBridge 创建事件桥接器
func NewEventBridge(handler *Handler, mgr *manager.Manager, sessionRepo *store.SessionRepository, msgRepo *store.MessageRepository, streamRepo *store.StreamEventRepository, log *slog.Logger) *EventBridge {
	ready := make(chan struct{})
	close(ready)
	return &EventBridge{
		handler:         handler,
		manager:         mgr,
		sessionRepo:     sessionRepo,
		msgRepo:         msgRepo,
		streamRepo:      streamRepo,
		log:             log,
		promptOrderTail: ready,
		acpSessionMap:   make(map[string]uint),
	}
}

// newPromptOrderTicket 返回一个按 WS 帧到达顺序串联的等待票据。
// release 可安全重复调用：前置准备失败、排队取消和成功入队都走同一收尾路径。
func (b *EventBridge) newPromptOrderTicket() (<-chan struct{}, func()) {
	b.promptOrderMu.Lock()
	wait := b.promptOrderTail
	next := make(chan struct{})
	b.promptOrderTail = next
	b.promptOrderMu.Unlock()

	var once sync.Once
	release := func() {
		once.Do(func() { close(next) })
	}
	return wait, release
}

// HasPromptInProgress 报告指定 ACP 会话是否处于 prompt 执行/排队中。
// 供 WS resync 查询：running=true 表示前端可恢复 streaming 续流。
func (b *EventBridge) HasPromptInProgress(agentID, sessionID string) bool {
	return b.manager.HasPromptInProgress(agentID, sessionID)
}

// NewEventBridge 组装完成后，由调用方（cmd/server）注入「prompt 开始执行」钩子：
// 全局三槽位获取成功（真正执行）时注册该会话的事件处理并广播 turn.started，
// 排队期间不注册，避免未开始的 session 覆盖正在执行的回调。

// OnPromptStarted 在全局执行槽位获取成功、本会话 prompt 即将发送时调用：
// 1. 注册按 ACP session id 路由的事件处理器；
// 2. 广播 turn.started，让前端把「排队中」切换为「流式」。
func (b *EventBridge) OnPromptStarted(agentID, sessionID string) {
	if err := b.SetupEventCallback(agentID, sessionID); err != nil {
		b.log.Warn("setup event callback on prompt started", "agentID", agentID, "sessionID", sessionID, "err", err)
		return
	}
	b.handler.BroadcastTurnStarted(sessionID)
}

// Log 返回 EventBridge 的日志器，供外部（如 handler）记录桥接相关事件。
func (b *EventBridge) Log() *slog.Logger {
	return b.log
}

// cacheSessionID 按 ACP session id 缓存对应的 ZACP DB session id。
// 缓存 miss 时回查数据库；查不到返回 0 与 false。
func (b *EventBridge) cacheSessionID(acpSessionID string) (uint, bool) {
	b.acpSessionMu.RLock()
	id, ok := b.acpSessionMap[acpSessionID]
	b.acpSessionMu.RUnlock()
	if ok {
		return id, true
	}

	dbSession, err := b.sessionRepo.GetByACPSessionID(acpSessionID)
	if err != nil {
		return 0, false
	}

	b.acpSessionMu.Lock()
	b.acpSessionMap[acpSessionID] = dbSession.ID
	b.acpSessionMu.Unlock()
	return dbSession.ID, true
}

// dbSessionIDFor 获取 ACP session id 对应的 ZACP DB session id（优先缓存）。
func (b *EventBridge) dbSessionIDFor(acpSessionID string) (uint, bool) {
	b.acpSessionMu.RLock()
	id, ok := b.acpSessionMap[acpSessionID]
	b.acpSessionMu.RUnlock()
	if ok {
		return id, true
	}
	return b.cacheSessionID(acpSessionID)
}

// bindPersistHandlers 为 Bridge 注入事件持久化/清理回调。
// 每个 Bridge 只需注入一次；幂等，重复调用以最新回调为准。
func (b *EventBridge) bindPersistHandlers(bridge *client.Bridge) {
	bridge.SetPersistHandler(func(event client.Event) error {
		dbSessionID, ok := b.dbSessionIDFor(event.SessionID)
		if !ok {
			// 按 ACP session id 查不到会话（如隐式探测阶段），无法落库；丢弃并记录。
			b.log.Debug("drop stream event for unknown session", "sessionID", event.SessionID)
			return nil
		}
		data, err := json.Marshal(event)
		if err != nil {
			return err
		}
		return b.streamRepo.Create(dbSessionID, data)
	})

	bridge.SetPersistResetHandler(func(acpSessionID string) error {
		dbSessionID, ok := b.dbSessionIDFor(acpSessionID)
		if !ok {
			return nil
		}
		return b.streamRepo.DeleteBySession(dbSessionID)
	})
}

// EnsureSessionUpdateHandlers 提前注册「按通知 sessionId 分发」的 session/update 处理器
// （configOptions、availableCommands），用于在会话创建（session/new）之前调用。
// 原因：部分 agent（如 reasonix）在 session/new 响应返回的同一时刻同步推送
// available_commands_update；若等 CreateSession 返回后才注册，SDK 的 read loop
// 可能已先读到该通知而丢弃（omp 有 ~50ms 延迟，此前未暴露此竞态）。
// 这两个处理器不依赖调用方闭包（按 SDK 通知自带 sessionId 分发），可安全提前注册；
// 而 onEvent / 权限等依赖具体会话的处理器仍由 SetupEventCallback 按需注册，
// 避免用空 sessionID 覆盖正在 prompt 的会话回调。
func (b *EventBridge) EnsureSessionUpdateHandlers(agentID string) error {
	bridge, err := b.manager.GetBridge(agentID)
	if err != nil {
		return err
	}

	bridge.SetConfigOptionsHandler(func(sid string, opts []acp.SessionConfigOption) {
		b.handleConfigOptions(sid, opts)
	})
	bridge.SetAvailableCommandsHandler(func(sid string, cmds []acp.AvailableCommand) {
		b.handleAvailableCommands(sid, cmds)
	})
	bridge.SetSessionInfoHandler(func(sid string, info acp.SessionSessionInfoUpdate) {
		b.handleSessionInfo(sid, info)
	})

	b.log.Info("session update handlers ensured for agent", "agentID", agentID)
	return nil
}

// SetupEventCallback 为 Agent 连接设置按 session 路由的事件/权限处理器。
// 回调可以在多个 prompt 并发执行时重复设置；事件和权限请求均携带 ACP session id，
// 不再依赖「每个 Agent 只有一个当前执行 session」的隐含前提。
func (b *EventBridge) SetupEventCallback(agentID, sessionID string) error {
	bridge, err := b.manager.GetBridge(agentID)
	if err != nil {
		return err
	}

	// 缓存 ACP session id → DB session id，供事件持久化/清理使用；
	// 同时注入持久化回调（幂等，重复设置无影响）。
	b.cacheSessionID(sessionID)
	b.bindPersistHandlers(bridge)

	// 事件按事件自身携带的 ACP session id 分发（client.Event.SessionID 来自
	// SDK session/update 通知的 SessionId），并发 session 不会互相覆盖。
	bridge.SetOnEvent(func(event client.Event) {
		b.handleEvent(event.SessionID, event)
	})

	// 非自动批准模式下，把 agent 的权限请求转发给前端交互式选择（见 RequestPermission）。
	// 闭包捕获 agentID：跨 agent 并行时权限路由到各自正在执行的会话。
	bridge.SetPermissionHandler(func(req acp.RequestPermissionRequest) (acp.RequestPermissionResponse, error) {
		return b.HandlePermissionRequest(agentID, req)
	})

	// 接收 agent 经 session/update 通知下发的配置项（模型/思考强度/mode 等），
	// 覆盖 session/new 响应未带 configOptions 的情况，实时落库供前端读取。
	// 回调自带 SDK 通知的 ACP session id，按会话分发（见 client.Bridge.SessionUpdate）。
	bridge.SetConfigOptionsHandler(func(sid string, opts []acp.SessionConfigOption) {
		b.handleConfigOptions(sid, opts)
	})

	// 接收 agent 经 session/update 的 available_commands_update 通知下发的可用 / 命令，
	// 落库供重进会话恢复，并实时广播给前端刷新候选面板。
	// 回调自带 SDK 通知的 ACP session id（同上）。
	bridge.SetAvailableCommandsHandler(func(sid string, cmds []acp.AvailableCommand) {
		b.handleAvailableCommands(sid, cmds)
	})

	// 接收 agent 经 session/update 的 session_info_update 通知下发的会话信息
	// （AI 总结标题等），落库供侧栏/信息面板展示，并实时广播给前端。
	// 回调自带 SDK 通知的 ACP session id（同上）。
	bridge.SetSessionInfoHandler(func(sid string, info acp.SessionSessionInfoUpdate) {
		b.handleSessionInfo(sid, info)
	})

	b.log.Info("event callback setup for agent", "agentID", agentID, "sessionID", sessionID)
	return nil
}

// handleConfigOptions 收到 agent 下发的配置项后落库 + 实时广播给前端。
// 场景：切换模型后 agent 推送新的 configOptions（如 deepseek 官方模型带思维强度选项），
// 前端收到广播立即刷新下拉，无需重新进入会话。
func (b *EventBridge) handleConfigOptions(sessionID string, opts []acp.SessionConfigOption) {
	dbSession, err := b.sessionRepo.GetByACPSessionID(sessionID)
	if err != nil {
		// reasonix 等 agent 在 session/new 响应同一毫秒同步推送通告，
		// 此时 DB 会话记录可能尚未落库（svc.CreateSession 仍在进行中）；
		// 延迟重试一次，避免「通告先到、落库后到」的竞态丢数据。
		go func() {
			if s := b.findSessionWithRetry(sessionID); s != nil {
				b.applyConfigOptions(s, opts)
			}
		}()
		return
	}
	b.applyConfigOptions(dbSession, opts)
}

func (b *EventBridge) applyConfigOptions(dbSession *model.Session, opts []acp.SessionConfigOption) {
	sessionID := dbSession.ACPSessionID
	dtos := client.ToConfigOptionDTOs(opts)
	data, err := json.Marshal(dtos)
	if err != nil {
		b.log.Warn("marshal config options failed", "sessionID", sessionID, "err", err)
		return
	}
	if err := b.sessionRepo.UpdateConfigOptions(dbSession.ID, string(data)); err != nil {
		b.log.Warn("save config options failed", "sessionID", sessionID, "err", err)
		return
	}
	b.handler.BroadcastConfigOptions(sessionID, dtos)
	b.log.Info("config options updated", "sessionID", sessionID, "count", len(opts))
}

// findSessionWithRetry 按 ACP session id 查 DB 会话；首次查不到时延迟 300ms 重试一次。
// 用于 reasonix 等 agent 在 session/new 响应同一毫秒同步推送 session/update 通告、
// 而 DB 记录尚未落库的竞态窗口。重试后仍无则返回 nil（调用方丢弃）。
func (b *EventBridge) findSessionWithRetry(acpSessionID string) *model.Session {
	dbSession, err := b.sessionRepo.GetByACPSessionID(acpSessionID)
	if err == nil {
		return dbSession
	}
	time.Sleep(300 * time.Millisecond)
	dbSession, err = b.sessionRepo.GetByACPSessionID(acpSessionID)
	if err == nil {
		return dbSession
	}
	b.log.Warn("session update for unknown session (after retry)",
		"sessionID", acpSessionID, "err", err)
	return nil
}

// handleAvailableCommands 收到 agent 通告的可用 / 命令后落库 + 实时广播给前端。
// 与 handleConfigOptions 同理：命令列表可能随会话状态动态变化（agent 随时可重新通告），
// 前端收到广播立即刷新候选面板；落库保证重进会话时无需等待 agent 重新通告即可恢复。
func (b *EventBridge) handleAvailableCommands(sessionID string, cmds []acp.AvailableCommand) {
	dbSession, err := b.sessionRepo.GetByACPSessionID(sessionID)
	if err != nil {
		// 同 handleConfigOptions：通告可能在 DB 落库前到达，延迟重试一次。
		go func() {
			if s := b.findSessionWithRetry(sessionID); s != nil {
				b.applyAvailableCommands(s, cmds)
			}
		}()
		return
	}
	b.applyAvailableCommands(dbSession, cmds)
}

func (b *EventBridge) applyAvailableCommands(dbSession *model.Session, cmds []acp.AvailableCommand) {
	sessionID := dbSession.ACPSessionID
	dtos := client.ToAvailableCommandDTOs(cmds)
	// 落库保持 agent 通告原样（静态命令不入库，避免 DB 语义被污染；
	// 重进会话时由 REST GetSlashCommands 动态合并兜底）。
	data, err := json.Marshal(dtos)
	if err != nil {
		b.log.Warn("marshal slash commands failed", "sessionID", sessionID, "err", err)
		return
	}
	if err := b.sessionRepo.UpdateAvailableCommands(dbSession.ID, string(data)); err != nil {
		b.log.Warn("save slash commands failed", "sessionID", sessionID, "err", err)
		return
	}
	// 广播合并后的列表：agent 不通告命令（如 grok）时静态命令仍能展示；
	// 同名以 agent 通告为准，见 providers.MergeSlashCommands。
	broadcast := providers.MergeSlashCommands(dtos, providers.DefaultSlashCommands(dbSession.AgentID))
	b.handler.BroadcastSlashCommands(sessionID, broadcast)
	b.log.Info("slash commands updated", "sessionID", sessionID, "count", len(broadcast))
}

// handleSessionInfo 收到 agent 通告的会话信息（AI 总结标题等）后落库 + 实时广播给前端。
// 标题优先级约定：agent 经 session_info_update 推送的标题优先于 zacp 本地的
// deriveTitle 截取逻辑（见 HandlePrompt）——agent 没推过标题时维持截取结果，
// 推过则以 agent 标题为准（覆盖）。与 handleConfigOptions 同理：通告可能早于
// DB 落库到达，查不到时延迟重试一次。
func (b *EventBridge) handleSessionInfo(sessionID string, info acp.SessionSessionInfoUpdate) {
	if info.Title == nil || strings.TrimSpace(*info.Title) == "" {
		// agent 未提供标题（或显式清空）：保持现有标题不变，避免误覆盖
		return
	}
	title := strings.TrimSpace(*info.Title)

	dbSession, err := b.sessionRepo.GetByACPSessionID(sessionID)
	if err != nil {
		// 同 handleConfigOptions：通告可能在 DB 落库前到达，延迟重试一次。
		go func() {
			if s := b.findSessionWithRetry(sessionID); s != nil {
				b.applySessionInfo(s, title)
			}
		}()
		return
	}
	b.applySessionInfo(dbSession, title)
}

func (b *EventBridge) applySessionInfo(dbSession *model.Session, title string) {
	sessionID := dbSession.ACPSessionID
	if err := b.sessionRepo.UpdateTitle(dbSession.ID, title); err != nil {
		b.log.Warn("save session title failed", "sessionID", sessionID, "err", err)
		return
	}
	b.handler.BroadcastSessionInfo(sessionID, map[string]any{"title": title})
	b.log.Info("session title updated by agent", "sessionID", sessionID, "title", title)
}

// clientEventToWsEvent 将 ACP 事件转换为前端可消费的 WebSocket 事件。
func clientEventToWsEvent(event client.Event) map[string]interface{} {
	wsEvent := map[string]interface{}{
		"type":   event.Type,
		"text":   event.Text,
		"title":  event.Title,
		"status": event.Status,
		"toolId": event.ToolID,
	}
	// 工具调用入参/出参：严格判空后省略字段，避免广播冗余的 null（判空逻辑在 pkg/eventstore，落库拆分共用）
	if !eventstore.IsEmpty(event.Input) {
		wsEvent["input"] = event.Input
	}
	if !eventstore.IsEmpty(event.Output) {
		wsEvent["output"] = event.Output
	}
	// 执行计划（TODO 列表）：整体替换语义，随事件原样透传；nil 时省略
	if event.Plan != nil {
		wsEvent["plan"] = event.Plan
	}
	return wsEvent
}

// handleEvent 处理 ACP 事件并广播到 WebSocket
func (b *EventBridge) handleEvent(sessionID string, event client.Event) {
	b.handler.BroadcastEvent(sessionID, clientEventToWsEvent(event))
}

// ResyncClient 在 WebSocket 重连/刷新后向该客户端推送：
//  1. 尝试从 stream_events 恢复本轮输出（ZACP 重启场景）
//  2. session.resynced（告知是否仍在执行）
//  3. history（提示前端刷新该会话历史消息）
//  4. 若仍在执行，重放本轮已累积的 ACP 事件，使前端能够续流
func (b *EventBridge) ResyncClient(client *Client, acpSessionID, agentID string) {
	running := b.manager.HasPromptInProgress(agentID, acpSessionID)

	// 优先恢复持久化的流式事件：ZACP 进程重启后内存缓存丢失，但 DB 中可能仍有当前 turn 输出。
	if recovered, err := b.recoverStreamEvents(client, acpSessionID, agentID, running); err != nil {
		b.log.Warn("recover stream events failed", "sessionID", acpSessionID, "agentID", agentID, "err", err)
	} else if recovered {
		// 已把暂存事件补齐为历史消息：running 视为结束，前端通过 history 刷新即可。
		running = false
	}

	client.Send(ServerMessage{
		Type:      MsgTypeSessionResynced,
		SessionID: acpSessionID,
		Running:   running,
	})
	// 提示前端刷新历史消息（浏览器只做显示，数据库是权威来源）
	client.Send(ServerMessage{
		Type:      MsgTypeHistory,
		SessionID: acpSessionID,
	})

	if running {
		if err := b.replayEvents(client, agentID, acpSessionID); err != nil {
			b.log.Warn("failed to replay session events", "sessionID", acpSessionID, "agentID", agentID, "err", err)
		}
	}
}

// recoverStreamEvents 从 stream_events 恢复当前 turn 事件。
// - running=true：说明 ACP 仍在执行，把事件恢复到内存缓存并清理 DB，供后续 replayEvents 重放。
// - running=false：说明 ZACP 可能已重启，把暂存事件补成一条 assistant 历史消息后清理 DB。
// 返回 (recovered, error)：recovered=true 表示已补齐为历史消息，running 应置 false。
func (b *EventBridge) recoverStreamEvents(c *Client, acpSessionID, agentID string, running bool) (bool, error) {
	dbSessionID, ok := b.dbSessionIDFor(acpSessionID)
	if !ok {
		return false, nil
	}

	streamEvents, err := b.streamRepo.ListBySession(dbSessionID)
	if err != nil {
		return false, err
	}
	if len(streamEvents) == 0 {
		return false, nil
	}

	events := make([]client.Event, 0, len(streamEvents))
	for _, se := range streamEvents {
		var e client.Event
		if err := json.Unmarshal([]byte(se.Data), &e); err != nil {
			b.log.Warn("unmarshal stream event failed", "sessionID", acpSessionID, "err", err)
			continue
		}
		events = append(events, e)
	}
	if len(events) == 0 {
		_ = b.streamRepo.DeleteBySession(dbSessionID)
		return false, nil
	}

	if running {
		// 仍在执行：恢复到 Bridge 内存，然后清理 DB 防止重复恢复。
		bridge, err := b.manager.GetBridge(agentID)
		if err != nil {
			return false, err
		}
		bridge.RestoreSession(acpSessionID, events)
		if err := b.streamRepo.DeleteBySession(dbSessionID); err != nil {
			b.log.Warn("delete stream events after restore failed", "sessionID", acpSessionID, "err", err)
		}
		return false, nil
	}

	// 未在执行：补齐为一条 assistant 消息，避免 turn 因 ZACP 重启而完全丢失。
	lastAssistant, err := b.msgRepo.GetLastAssistantBySession(dbSessionID)
	if err == nil && !lastAssistant.CreatedAt.Before(streamEvents[len(streamEvents)-1].CreatedAt) {
		// 数据库里已有更新的 assistant 消息，说明 turn 已正常收尾；只清理临时事件。
		_ = b.streamRepo.DeleteBySession(dbSessionID)
		return false, nil
	}

	content := client.EventsText(events)
	slimEvents, toolDetails := eventstore.SplitToolDetails(events)
	msg := &model.Message{
		SessionID:   dbSessionID,
		Role:        "assistant",
		Content:     content,
		Events:      eventstore.Marshal(slimEvents),
		ToolDetails: eventstore.MarshalDetails(toolDetails),
		CreatedAt:   time.Now(),
	}
	if err := b.msgRepo.Create(msg); err != nil {
		return false, fmt.Errorf("create recovered assistant message: %w", err)
	}
	if err := b.streamRepo.DeleteBySession(dbSessionID); err != nil {
		b.log.Warn("delete stream events after recovery failed", "sessionID", acpSessionID, "err", err)
	}

	// 若 Bridge 内存中有同 session 残留事件（理论上不应有），也清掉。
	if bridge, err := b.manager.GetBridge(agentID); err == nil {
		bridge.ResetSession(acpSessionID)
	}
	return true, nil
}

// replayEvents 将指定 ACP 会话已缓存的事件按序重放给指定客户端。
// 用于浏览器断连期间事件仍在缓存中的场景，保证重连后能看到当前轮的完整输出。
func (b *EventBridge) replayEvents(client *Client, agentID, acpSessionID string) error {
	bridge, err := b.manager.GetBridge(agentID)
	if err != nil {
		return err
	}
	events := bridge.Events(acpSessionID)
	if len(events) == 0 {
		return nil
	}
	for _, event := range events {
		client.Send(ServerMessage{
			Type:      MsgTypeEvent,
			SessionID: acpSessionID,
			Event:     clientEventToWsEvent(event),
		})
	}
	return nil
}

// HandlePermissionRequest 处理 agent 的权限请求（在 RequestPermission 回调中同步调用）：
// 权限请求自身带 ACP sessionId，按请求归属广播给前端；每个 permission 独立等待用户选择。
// 超时（permissionTimeout）自动取消，避免阻塞 agent turn。
func (b *EventBridge) HandlePermissionRequest(agentID string, req acp.RequestPermissionRequest) (acp.RequestPermissionResponse, error) {
	permissionID := fmt.Sprintf("perm-%d", time.Now().UnixNano())
	ch := make(chan acp.RequestPermissionResponse, 1)
	b.pendingPermissions.Store(permissionID, ch)

	sessionID := string(req.SessionId)
	// 转成前端友好结构（SDK 类型直接序列化字段不稳定，显式挑字段）
	toolCall := map[string]interface{}{
		"toolCallId": string(req.ToolCall.ToolCallId),
	}
	if req.ToolCall.Title != nil {
		toolCall["title"] = *req.ToolCall.Title
	}
	if req.ToolCall.Status != nil {
		toolCall["status"] = string(*req.ToolCall.Status)
	}
	if req.ToolCall.RawInput != nil {
		toolCall["rawInput"] = req.ToolCall.RawInput
	}

	options := make([]map[string]interface{}, 0, len(req.Options))
	for _, o := range req.Options {
		options = append(options, map[string]interface{}{
			"optionId": string(o.OptionId),
			"name":     o.Name,
			"kind":     string(o.Kind),
		})
	}

	b.handler.BroadcastPermissionRequest(sessionID, permissionID, toolCall, options)
	b.log.Info("permission requested", "permissionID", permissionID, "sessionID", sessionID)

	// 使用 NewTimer 替代 time.After，避免超时前已响应仍常驻 5 分钟 Timer 直至触发
	timer := time.NewTimer(permissionTimeout)
	defer timer.Stop()
	select {
	case resp := <-ch:
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		return resp, nil
	case <-timer.C:
		b.pendingPermissions.Delete(permissionID)
		b.log.Warn("permission request timed out", "permissionID", permissionID)
		return acp.RequestPermissionResponse{
			Outcome: acp.RequestPermissionOutcome{Cancelled: &acp.RequestPermissionOutcomeCancelled{}},
		}, nil
	}
}

// ResolvePermission 处理前端回传的权限选择结果（WS permission 消息），
// 唤醒等待中的 HandlePermissionRequest。
func (b *EventBridge) ResolvePermission(permissionID, optionID string) {
	v, ok := b.pendingPermissions.LoadAndDelete(permissionID)
	if !ok {
		b.log.Warn("permission not pending", "permissionID", permissionID)
		return
	}
	ch, ok := v.(chan acp.RequestPermissionResponse)
	if !ok {
		return
	}
	select {
	case ch <- acp.RequestPermissionResponse{
		Outcome: acp.RequestPermissionOutcome{
			Selected: &acp.RequestPermissionOutcomeSelected{OptionId: acp.PermissionOptionId(optionID)},
		},
	}:
	default:
		// 通道已满（理论上不会），丢弃
	}
}

// deriveTitle 从首条用户消息生成会话标题（最多 24 个字符）
func deriveTitle(message string) string {
	r := []rune(strings.TrimSpace(message))
	if len(r) <= 24 {
		return string(r)
	}
	return string(r[:24]) + "…"
}

// HandlePrompt 处理 WebSocket 的 prompt 消息（每帧一个 goroutine，可并发进入）。
// 并发语义：全局最多 3 个 prompt 进入 ACP，更多请求按 FIFO 排队；
// 不同 session 的事件、回复和权限按 ACP session id 隔离。
// 排队中的 prompt 被 Cancel 撤销时返回 ErrPromptCancelled，此处广播
// turn.done(cancelled) 让前端复位「排队中」状态，不报错、不落库。
//
// 流程：
//  1. 按需启动 agent 进程
//  2. 按 ACP session id 反查 DB 会话并落库用户消息（首条消息生成标题）
//  3. 草稿转正
//  4. 经 manager.Prompt 排队执行（事件回调由 onStarted 钩子注册）
//  5. 落库助手回复、touch 会话（驱动侧栏排序），广播 turn.done
//
// HandlePrompt 处理一个不要求 WS 到达顺序的 prompt 调用（兼容 REST/测试调用方）。
func (b *EventBridge) HandlePrompt(ctx context.Context, sessionID, agentID, message string) error {
	return b.handlePrompt(ctx, sessionID, agentID, message, nil, nil)
}

// HandlePromptWithOrder 处理 WS prompt：等待前序帧完成入队，再进入 Manager 全局 FIFO。
func (b *EventBridge) HandlePromptWithOrder(ctx context.Context, sessionID, agentID, message string, wait <-chan struct{}, release func()) error {
	return b.handlePrompt(ctx, sessionID, agentID, message, wait, release)
}

func (b *EventBridge) handlePrompt(ctx context.Context, sessionID, agentID, message string, wait <-chan struct{}, release func()) error {
	if release != nil {
		defer release()
	}
	// 按需启动兜底：服务端重启后仅预启动第一个 agent，用户直接对其它
	// agent 的旧会话发消息时，这里先确保进程已启动——否则事件回调注册
	// 的 GetBridge 会返回 "agent not started"，根本走不到 manager.Prompt。
	if err := b.manager.EnsureStarted(ctx, agentID); err != nil {
		return err
	}

	dbSession, err := b.sessionRepo.GetByACPSessionID(sessionID)
	if err != nil {
		return fmt.Errorf("session not found: %w", err)
	}
	// 缓存 ACP session 与 DB session 的映射，供 ResetSession 清理 stream_events 使用。
	b.cacheSessionID(sessionID)

	// 落库用户消息（即使 agent 调用失败也保留）
	userMsg := &model.Message{
		SessionID: dbSession.ID,
		Role:      "user",
		Content:   message,
		CreatedAt: time.Now(),
	}
	if err := b.msgRepo.Create(userMsg); err != nil {
		return fmt.Errorf("failed to save user message: %w", err)
	}

	// 草稿转正：隐式草稿会话在发出首条 prompt 时转为正常会话（is_draft=false），
	// 此后进入侧栏列表展示。设计约定「转正时机=发出首条 prompt 即转正，不等回复」。
	if dbSession.IsDraft {
		if err := b.sessionRepo.PromoteFromDraft(dbSession.ID); err != nil {
			b.log.Warn("promote draft session failed", "sessionID", sessionID, "err", err)
		} else {
			b.log.Info("draft session promoted", "sessionID", sessionID, "dbID", dbSession.ID)
		}
	}

	// 首条消息生成会话标题（仅当仍是默认标题时）
	if dbSession.Title == "" || dbSession.Title == "新会话" {
		_ = b.sessionRepo.UpdateTitle(dbSession.ID, deriveTitle(message))
	}

	if wait != nil {
		select {
		case <-wait:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	result, err := b.manager.PromptWithAdmission(ctx, agentID, sessionID, message, release)
	if err != nil && manager.IsUnknownSessionErr(err) {
		// ACP session 失效（服务端/agent 重启后 DB 记录仍在、agent 端已丢失）：
		// 自动恢复并重试一次，前端无感知。事件回调由 onStarted 钩子注册，
		// 闭包捕获的 sessionID 变量在重试前已更新，会绑定到新 session。
		b.log.Warn("acp session invalid, recovering", "sessionID", sessionID, "err", err)
		if newID, ok := b.recoverSession(ctx, dbSession, agentID, sessionID); ok {
			sessionID = newID
			result, err = b.manager.PromptWithAdmission(ctx, agentID, sessionID, message, release)
		}
	}
	if err != nil {
		if manager.IsPromptCancelledErr(err) {
			// 排队中的 prompt 被用户取消（撤销排队）：agent 尚未收到任何内容，
			// 用户消息已落库（保留），不落 assistant、不报错；广播 turn.done(cancelled)
			// 让前端复位「排队中」状态并刷新消息（占位消息替换为 DB 版本）。
			b.log.Info("queued prompt cancelled", "sessionID", sessionID)
			b.handler.BroadcastTurnDone(sessionID, "", "cancelled")
			return nil
		}
		return err
	}

	// 落库助手回复：events 拆分为「瘦身事件 + 工具详情」两列落库，
	// 避免 tool_call_update 的重复全量快照撑大历史消息（见 pkg/eventstore）
	slimEvents, toolDetails := eventstore.SplitToolDetails(result.Events)
	assistantMsg := &model.Message{
		SessionID:   dbSession.ID,
		Role:        "assistant",
		Content:     result.Reply,
		Events:      eventstore.Marshal(slimEvents),
		ToolDetails: eventstore.MarshalDetails(toolDetails),
		CreatedAt:   time.Now(),
	}
	if err := b.msgRepo.Create(assistantMsg); err != nil {
		return fmt.Errorf("failed to save assistant message: %w", err)
	}

	// touch 会话驱动侧栏排序
	_ = b.sessionRepo.Touch(dbSession.ID)

	b.handler.BroadcastTurnDone(sessionID, result.Reply, result.StopReason)
	return nil
}

// HandleCancel 处理 WebSocket 的 cancel 消息
func (b *EventBridge) HandleCancel(ctx context.Context, sessionID, agentID string) error {
	// 与 HandlePrompt 一致：先确保 agent 已启动（幂等），避免对未启动
	// agent 的旧会话发 cancel 时报 "agent not started"。
	if err := b.manager.EnsureStarted(ctx, agentID); err != nil {
		return err
	}
	return b.manager.Cancel(ctx, agentID, sessionID)
}

// recoverSession 处理 ACP session 失效（服务端/agent 重启后 DB 记录仍在但 agent 端丢失）：
// 委托 manager.RecoverSession：优先 ACP session/load（agent 支持持久化会话时保留对话上下文），
// 失败则新建 ACP session；重建时更新 DB 记录并迁移 WS 订阅（旧 id → 新 id，
// 否则广播按新 id 匹配不到订阅者、前端一直 loading），返回最终可用的 ACP session id。
func (b *EventBridge) recoverSession(ctx context.Context, dbSession *model.Session, agentID, oldAcpID string) (string, bool) {
	// cwd 为空时传 ""，由 manager.RecoverSession 统一按 provider 默认工作区解析
	//（与创建会话的路径语义一致，均为绝对路径；传 "." 等相对路径会导致
	//  omp 等按 cwd 定位会话文件的 agent load 永远失败、每次都走重建）。
	cwd := ""
	if dbSession.Workspace.Path != "" {
		cwd = dbSession.Workspace.Path
	}
	newID, rebuilt, err := b.manager.RecoverSession(ctx, agentID, oldAcpID, cwd, dbSession.ConfigOptions)
	if err != nil {
		b.log.Error("failed to recover acp session", "err", err)
		return "", false
	}
	if rebuilt {
		if err := b.sessionRepo.UpdateACPSessionID(dbSession.ID, newID); err != nil {
			b.log.Error("failed to update acp session id in db", "err", err)
		}
		// 同步更新 ACP session 映射；若 agent 已重建，新的 Bridge 需要重新注入持久化回调。
		b.acpSessionMu.Lock()
		b.acpSessionMap[newID] = dbSession.ID
		delete(b.acpSessionMap, oldAcpID)
		b.acpSessionMu.Unlock()
		if bridge, err := b.manager.GetBridge(agentID); err == nil {
			b.bindPersistHandlers(bridge)
		}

		// 订阅迁移必须在重试 prompt 之前完成：HandlePrompt 的重试在 recoverSession
		// 返回后执行，随后的 turn.started/event/turn.done 广播都走新 id，
		// 只有迁移后的连接才能收到（另有 session.recovered 消息让前端更新映射）。
		b.handler.RebindSession(oldAcpID, newID)
		b.log.Info("rebound ws subscriptions", "old", oldAcpID, "new", newID)
		// 回放用户配置：重建 = 全新 ACP 会话，agent 侧配置回到默认值。
		//（load 成功时配置随上下文原样恢复，无需回放；此处兜底 load 失败/
		//  不支持 load 的 agent，按 DB 存档逐项 set 回去，尽力而为。）
		b.manager.ReplaySessionConfigOptions(ctx, agentID, newID, dbSession.ConfigOptions)
	}
	return newID, true
}
