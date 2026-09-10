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
//	UPSTREAM_MODEL_SYNC_ENABLED                  "false" 关闭（默认关闭）
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
	config       upstreamModelSyncConfig
	settings     *SettingService
	wake         chan struct{}
	unsubscribe  func()
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

type upstreamModelSyncConfig struct {
	enabled        bool
	interval       time.Duration
	accountTimeout time.Duration
}

func defaultUpstreamModelSyncConfig() upstreamModelSyncConfig {
	return upstreamModelSyncConfig{false, defaultUpstreamModelSyncIntervalHours * time.Hour, defaultUpstreamModelSyncAccountTimeout}
}

func parseUpstreamModelSyncDuration(raw string) (time.Duration, error) {
	value, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("expected a positive duration (for example 24h or 120s), got %q", raw)
	}
	return value, nil
}

func resolveUpstreamModelSyncConfig(values map[string]string, current upstreamModelSyncConfig) (upstreamModelSyncConfig, error) {
	result := current
	var diagnostics []error
	if raw, ok := values[SettingKeyUpstreamModelSyncEnabled]; ok {
		value, err := strconv.ParseBool(strings.TrimSpace(raw))
		if err != nil {
			diagnostics = append(diagnostics, fmt.Errorf("%s: %w", SettingKeyUpstreamModelSyncEnabled, err))
		} else {
			result.enabled = value
		}
	} else if raw, ok := os.LookupEnv("UPSTREAM_MODEL_SYNC_ENABLED"); ok && strings.TrimSpace(raw) != "" {
		value, err := strconv.ParseBool(strings.TrimSpace(raw))
		if err != nil {
			diagnostics = append(diagnostics, fmt.Errorf("UPSTREAM_MODEL_SYNC_ENABLED: %w", err))
		} else {
			result.enabled = value
		}
	}
	for _, field := range []struct {
		key, env string
		unit     time.Duration
		target   *time.Duration
	}{
		{SettingKeyUpstreamModelSyncInterval, "UPSTREAM_MODEL_SYNC_INTERVAL_HOURS", time.Hour, &result.interval},
		{SettingKeyUpstreamModelSyncAccountTimeout, "UPSTREAM_MODEL_SYNC_ACCOUNT_TIMEOUT_SECONDS", time.Second, &result.accountTimeout},
	} {
		raw, present := values[field.key]
		source := field.key
		if !present {
			raw, present = os.LookupEnv(field.env)
			present = present && strings.TrimSpace(raw) != ""
			source = field.env
			if present {
				count, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
				if err != nil || count <= 0 || count > int64((1<<63-1)/field.unit) {
					diagnostics = append(diagnostics, fmt.Errorf("%s: expected a positive integer within duration range, got %q", source, raw))
					continue
				}
				raw = (time.Duration(count) * field.unit).String()
			}
		}
		if present {
			value, err := parseUpstreamModelSyncDuration(raw)
			if err != nil {
				diagnostics = append(diagnostics, fmt.Errorf("%s: %w", source, err))
			} else {
				*field.target = value
			}
		}
	}
	return result, errors.Join(diagnostics...)
}

func NewUpstreamModelSyncScheduler(
	accountLister upstreamModelSyncAccountLister,
	accountTestService *AccountTestService,
) *UpstreamModelSyncScheduler {
	config, err := resolveUpstreamModelSyncConfig(nil, defaultUpstreamModelSyncConfig())
	if err != nil {
		slog.Error("invalid upstream model sync environment; scheduler disabled", "error", err)
		config.enabled = false
	}
	return newUpstreamModelSyncScheduler(accountLister, accountTestService, config)
}

func newUpstreamModelSyncScheduler(accountLister upstreamModelSyncAccountLister, accountTestService *AccountTestService, config upstreamModelSyncConfig) *UpstreamModelSyncScheduler {
	ctx, cancel := context.WithCancel(context.Background())
	s := &UpstreamModelSyncScheduler{
		accountLister:      accountLister,
		accountTestService: accountTestService,
		parentCtx:          ctx,
		parentCancel:       cancel,
		config:             config,
		wake:               make(chan struct{}, 1),
	}
	if accountTestService != nil {
		s.syncAccount = func(ctx context.Context, account *Account) error {
			catalog, err := accountTestService.SyncUpstreamModelCatalog(ctx, account)
			if catalog != nil && len(catalog.Warnings) > 0 {
				slog.Warn("upstream model sync partially succeeded",
					"account_id", account.ID, "status", catalog.Status, "warnings", catalog.Warnings)
			}
			return err
		}
	}
	return s
}

// ProvideUpstreamModelSyncScheduler starts the process-wide periodic runner.
func ProvideUpstreamModelSyncScheduler(
	accountRepo AccountRepository,
	accountTestService *AccountTestService,
	settings *SettingService,
) *UpstreamModelSyncScheduler {
	svc := newUpstreamModelSyncScheduler(accountRepo, accountTestService, defaultUpstreamModelSyncConfig())
	svc.settings = settings
	if settings != nil {
		// These existing listeners are notified after every successful settings write.
		svc.unsubscribe = settings.SubscribeChannelMonitorRuntime(func() {
			select {
			case svc.wake <- struct{}{}:
			default:
			}
		})
	}
	svc.Start()
	return svc
}

func (s *UpstreamModelSyncScheduler) Start() {
	if s == nil {
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
	if !s.stopped {
		s.stopped = true
		s.parentCancel()
		if s.unsubscribe != nil {
			s.unsubscribe()
		}
	}
	s.mu.Unlock()
	s.wg.Wait()
}

func (s *UpstreamModelSyncScheduler) reloadConfig() (bool, error) {
	var values map[string]string
	if s.settings != nil {
		ctx, cancel := context.WithTimeout(s.parentCtx, 5*time.Second)
		defer cancel()
		var err error
		values, err = s.settings.settingRepo.GetAll(ctx)
		if err != nil {
			return false, err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	next, err := resolveUpstreamModelSyncConfig(values, s.config)
	if err != nil {
		return false, err
	}
	changed := next != s.config
	s.config = next
	return changed, nil
}

func (s *UpstreamModelSyncScheduler) runLoop() {
	defer s.wg.Done()
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	var ticks <-chan time.Time
	reset := func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		s.mu.Lock()
		config := s.config
		s.mu.Unlock()
		ticks = nil
		if config.enabled {
			timer.Reset(config.interval)
			ticks = timer.C
		}
	}
	// Retry configuration independently of the possibly disabled cycle timer.
	retry := time.NewTimer(time.Hour)
	defer retry.Stop()
	var retries <-chan time.Time
	backoff := time.Second
	reload := func() (changed bool, ok bool) {
		if !retry.Stop() {
			select {
			case <-retry.C:
			default:
			}
		}
		retries = nil
		changed, err := s.reloadConfig()
		if err != nil {
			slog.Error("reload upstream model sync settings; retrying", "error", err)
			retry.Reset(backoff)
			retries = retry.C
			backoff = min(backoff*2, time.Minute)
			return false, false
		}
		backoff = time.Second
		return changed, true
	}
	reset()
	if _, ok := reload(); !ok {
		ticks = nil
	} else {
		reset()
	}
	for {
		select {
		case <-s.parentCtx.Done():
			return
		case <-s.wake:
			if changed, ok := reload(); ok && (changed || ticks == nil) {
				reset()
			}
		case <-retries:
			if changed, ok := reload(); ok && (changed || ticks == nil) {
				reset()
			}
		case <-ticks:
			if _, ok := reload(); ok {
				s.mu.Lock()
				enabled := s.config.enabled
				s.mu.Unlock()
				if enabled {
					if err := s.RunOnce(s.parentCtx); err != nil {
						logger.LegacyPrintf("service.upstream_model_sync", "run_once_failed: err=%v", err)
					}
				}
				reset()
			} else {
				ticks = nil
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
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return context.Canceled
	}
	s.wg.Add(1)
	accountTimeout := s.config.accountTimeout
	s.mu.Unlock()
	defer s.wg.Done()
	ctx, cancel := context.WithCancel(ctx)
	stopCancel := context.AfterFunc(s.parentCtx, cancel)
	defer stopCancel()
	defer cancel()

	s.cycleMu.Lock()
	defer s.cycleMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}

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
		accountCtx, cancel := context.WithTimeout(ctx, accountTimeout)
		syncErr := s.syncAccount(accountCtx, account)
		cancel()
		if syncErr == nil {
			continue
		}
		// 不支持该平台（如未来新增类型）是结构性的，降级 Info 避免每周期刷 Warn。
		var upstreamErr *UpstreamModelSyncError
		if errors.As(syncErr, &upstreamErr) && upstreamErr.Kind == UpstreamModelSyncErrorUnsupported {
			slog.Info("upstream model sync skipped: platform unsupported",
				"account_id", account.ID, "platform", account.Platform)
			continue
		}
		failures++
		slog.Warn("upstream model sync failed",
			"account_id", account.ID,
			"platform", account.Platform,
			"error", syncErr,
		)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if failures > 0 {
		return fmt.Errorf("upstream model sync: %d/%d accounts failed", failures, len(accounts))
	}
	return nil
}
