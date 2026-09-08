package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
)

// UpstreamModelSyncScheduler 周期性对所有 active 账号跑上游模型目录同步
// （与 admin 的 sync-upstream 按钮同一逻辑：SyncUpstreamModelCatalog）。
//
// 姿态：best-effort。单账号失败只记日志，不影响其他账号；同步本身只刷新
// accounts.extra 里的上游元数据快照，不动 model_mapping / 账号调度状态。
//
// 配置（环境变量）：
//
//	UPSTREAM_MODEL_SYNC_ENABLED                  "false" 关闭（默认开启）
//	UPSTREAM_MODEL_SYNC_INTERVAL_HOURS           周期小时数（默认 24）
//	UPSTREAM_MODEL_SYNC_ACCOUNT_TIMEOUT_SECONDS  单账号超时秒数（默认 120）
type UpstreamModelSyncScheduler struct {
	accountLister      upstreamModelSyncAccountLister
	accountTestService *AccountTestService

	// syncAccount 是可替换的同步单元（默认走 AccountTestService），测试可注入假实现。
	syncAccount func(ctx context.Context, account *Account) error

	parentCtx    context.Context
	parentCancel context.CancelFunc
	wg           sync.WaitGroup
	mu           sync.Mutex
	started      bool
	stopped      bool
	cycleMu      sync.Mutex
}

// 窄接口：与 upstreamBillingProbeDueAccountLister 同一思路，调度器只需要列举
// active 账号，不依赖整个 AccountRepository。
type upstreamModelSyncAccountLister interface {
	ListActive(ctx context.Context) ([]Account, error)
}

const (
	defaultUpstreamModelSyncIntervalHours  = 24
	defaultUpstreamModelSyncAccountTimeout = 120 * time.Second
)

func upstreamModelSyncEnabled() bool {
	return !strings.EqualFold(strings.TrimSpace(os.Getenv("UPSTREAM_MODEL_SYNC_ENABLED")), "false")
}

func upstreamModelSyncInterval() time.Duration {
	if v, err := strconv.Atoi(strings.TrimSpace(os.Getenv("UPSTREAM_MODEL_SYNC_INTERVAL_HOURS"))); err == nil && v > 0 {
		return time.Duration(v) * time.Hour
	}
	return defaultUpstreamModelSyncIntervalHours * time.Hour
}

func upstreamModelSyncAccountTimeout() time.Duration {
	if v, err := strconv.Atoi(strings.TrimSpace(os.Getenv("UPSTREAM_MODEL_SYNC_ACCOUNT_TIMEOUT_SECONDS"))); err == nil && v > 0 {
		return time.Duration(v) * time.Second
	}
	return defaultUpstreamModelSyncAccountTimeout
}

func NewUpstreamModelSyncScheduler(
	accountLister upstreamModelSyncAccountLister,
	accountTestService *AccountTestService,
) *UpstreamModelSyncScheduler {
	ctx, cancel := context.WithCancel(context.Background())
	s := &UpstreamModelSyncScheduler{
		accountLister:      accountLister,
		accountTestService: accountTestService,
		parentCtx:          ctx,
		parentCancel:       cancel,
	}
	if accountTestService != nil {
		s.syncAccount = func(ctx context.Context, account *Account) error {
			_, err := accountTestService.SyncUpstreamModelCatalog(ctx, account)
			return err
		}
	}
	return s
}

// ProvideUpstreamModelSyncScheduler starts the process-wide periodic runner.
func ProvideUpstreamModelSyncScheduler(
	accountRepo AccountRepository,
	accountTestService *AccountTestService,
) *UpstreamModelSyncScheduler {
	svc := NewUpstreamModelSyncScheduler(accountRepo, accountTestService)
	svc.Start()
	return svc
}

func (s *UpstreamModelSyncScheduler) Start() {
	if s == nil || !upstreamModelSyncEnabled() {
		return
	}
	s.mu.Lock()
	if s.started || s.stopped {
		s.mu.Unlock()
		return
	}
	s.started = true
	s.wg.Add(1)
	s.mu.Unlock()
	go s.runLoop()
}

func (s *UpstreamModelSyncScheduler) Stop() {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return
	}
	s.stopped = true
	s.parentCancel()
	s.mu.Unlock()
	s.wg.Wait()
}

func (s *UpstreamModelSyncScheduler) runLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(upstreamModelSyncInterval())
	defer ticker.Stop()
	// 启动后不立即跑：账号刚建/刚编辑时探测链路已经同步过一次，首轮等一个周期，
	// 避免每次进程重启都对所有上游打一轮 /models。
	for {
		select {
		case <-s.parentCtx.Done():
			return
		case <-ticker.C:
			if err := s.RunOnce(s.parentCtx); err != nil {
				logger.LegacyPrintf("service.upstream_model_sync", "run_once_failed: err=%v", err)
			}
		}
	}
}

// RunOnce 对所有 active 账号各跑一次目录同步。单账号失败只记日志并继续；
// 返回聚合错误（供手动触发/测试观察），调用方按 best-effort 处理。
func (s *UpstreamModelSyncScheduler) RunOnce(ctx context.Context) error {
	if s == nil || s.accountLister == nil || s.syncAccount == nil {
		return nil
	}
	s.cycleMu.Lock()
	defer s.cycleMu.Unlock()

	accounts, err := s.accountLister.ListActive(ctx)
	if err != nil {
		return fmt.Errorf("list active accounts: %w", err)
	}
	failures := 0
	for i := range accounts {
		account := &accounts[i]
		if ctx.Err() != nil {
			return ctx.Err()
		}
		accountCtx, cancel := context.WithTimeout(ctx, upstreamModelSyncAccountTimeout())
		syncErr := s.syncAccount(accountCtx, account)
		cancel()
		if syncErr == nil {
			continue
		}
		failures++
		// 不支持该平台（如未来新增类型）是结构性的，降级 Info 避免每周期刷 Warn。
		var upstreamErr *UpstreamModelSyncError
		if errors.As(syncErr, &upstreamErr) && upstreamErr.Kind == UpstreamModelSyncErrorUnsupported {
			slog.Info("upstream model sync skipped: platform unsupported",
				"account_id", account.ID, "platform", account.Platform)
			continue
		}
		slog.Warn("upstream model sync failed",
			"account_id", account.ID,
			"platform", account.Platform,
			"error", syncErr,
		)
	}
	if failures > 0 {
		return fmt.Errorf("upstream model sync: %d/%d accounts failed", failures, len(accounts))
	}
	return nil
}
