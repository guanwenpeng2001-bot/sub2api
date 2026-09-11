//go:build unit

package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	"github.com/DATA-DOG/go-sqlmock"
	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/stretchr/testify/require"
)

type registrationAffiliateRepo struct {
	AffiliateRepository
	profiles map[int64]*AffiliateSummary
	bindErr  error
}

func (r *registrationAffiliateRepo) EnsureUserAffiliate(_ context.Context, id int64) (*AffiliateSummary, error) {
	if r.profiles[id] == nil {
		r.profiles[id] = &AffiliateSummary{UserID: id}
	}
	return r.profiles[id], nil
}
func (r *registrationAffiliateRepo) GetAffiliateByCode(_ context.Context, code string) (*AffiliateSummary, error) {
	for _, p := range r.profiles {
		if p.AffCode == code {
			return p, nil
		}
	}
	return nil, ErrAffiliateProfileNotFound
}
func (r *registrationAffiliateRepo) BindInviter(ctx context.Context, id, inviter int64) (bool, error) {
	if r.bindErr != nil {
		return false, r.bindErr
	}
	p, _ := r.EnsureUserAffiliate(ctx, id)
	if p.InviterID != nil {
		return false, nil
	}
	p.InviterID = &inviter
	r.profiles[inviter].AffCount++
	return true, nil
}
func newPersonalInvitationTestService() (*AuthService, *registrationAffiliateRepo, *raceSafeUserRepo, map[string]string) {
	users := newRaceSafeUserRepo()
	users.byID[100] = &User{ID: 100, Email: "inviter@example.com", Status: StatusActive}
	settings := map[string]string{
		SettingKeyRegistrationEnabled: "true", SettingKeyInvitationCodeEnabled: "true",
		SettingKeyAffiliateInvitationCodeEnabled: "true", SettingKeyAffiliateEnabled: "false",
	}
	s := newOAuthEmailFlowAuthService(users, &raceSafeRedeemRepo{codes: map[string]*RedeemCode{}}, &refreshTokenCacheStub{}, settings, nil, &userPlatformQuotaRepoStub{})
	affiliates := &registrationAffiliateRepo{profiles: map[int64]*AffiliateSummary{100: {UserID: 100, AffCode: "TEAM2026"}}}
	s.affiliateService = NewAffiliateService(affiliates, s.settingService, nil, nil)
	return s, affiliates, users, settings
}

func TestPersonalInvitationRegistration(t *testing.T) {
	for _, tc := range []struct {
		name, code, setting, value string
		want                       error
	}{
		{name: "valid normalized code", code: " team2026 "},
		{name: "unknown", code: "UNKNOWN", want: ErrInvitationCodeInvalid},
		{name: "invalid format", code: "!!!", want: ErrInvitationCodeInvalid},
		{name: "required", want: ErrInvitationCodeRequired},
		{name: "personal switch disabled", code: "TEAM2026", setting: SettingKeyAffiliateInvitationCodeEnabled, value: "false", want: ErrInvitationCodeInvalid},
		{name: "missing setting defaults disabled", code: "TEAM2026", setting: SettingKeyAffiliateInvitationCodeEnabled, value: "", want: ErrInvitationCodeInvalid},
		{name: "registration disabled", code: "TEAM2026", setting: SettingKeyRegistrationEnabled, value: "false", want: ErrRegDisabled},
		{name: "email verification still required", code: "TEAM2026", setting: SettingKeyEmailVerifyEnabled, value: "true", want: ErrEmailVerifyRequired},
		{name: "invitation mode disabled ignores credential", code: "UNKNOWN", setting: SettingKeyInvitationCodeEnabled, value: "false"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, affiliates, users, settings := newPersonalInvitationTestService()
			if tc.setting != "" {
				settings[tc.setting] = tc.value
			}
			token, u, err := s.RegisterWithVerification(context.Background(), "new@example.com", "Password123!", "", "", tc.code, "")
			if tc.want != nil {
				require.ErrorIs(t, err, tc.want)
				require.Empty(t, token)
				require.Empty(t, users.byEmail)
				return
			}
			require.NoError(t, err)
			require.NotEmpty(t, token)
			if tc.setting == SettingKeyInvitationCodeEnabled {
				require.Nil(t, affiliates.profiles[u.ID].InviterID)
			} else {
				require.Equal(t, int64(100), *affiliates.profiles[u.ID].InviterID)
			}
		})
	}
}

func TestPersonalInvitationReusableAndSourceCannotBeOverridden(t *testing.T) {
	s, affiliates, _, _ := newPersonalInvitationTestService()
	ctx := context.Background()
	for _, email := range []string{"one@example.com", "two@example.com"} {
		_, u, err := s.RegisterWithVerification(ctx, email, "Password123!", "", "", "TEAM2026", "OTHER-CODE")
		require.NoError(t, err)
		require.Equal(t, int64(100), *affiliates.profiles[u.ID].InviterID)
	}
	require.Equal(t, 2, affiliates.profiles[100].AffCount)
	_, _, err := s.RegisterWithVerification(ctx, "one@example.com", "Password123!", "", "", "TEAM2026", "")
	require.ErrorIs(t, err, ErrEmailExists)
	require.Equal(t, 2, affiliates.profiles[100].AffCount)
}

func TestPersonalInvitationRejectsInactiveInviterAndBindingFailure(t *testing.T) {
	s, affiliates, users, _ := newPersonalInvitationTestService()
	users.byID[100].Status = StatusDisabled
	require.ErrorIs(t, s.ValidateRegistrationInvitation(context.Background(), "TEAM2026"), ErrInvitationCodeInvalid)
	users.byID[100].Status = StatusActive
	affiliates.bindErr = errors.New("binding unavailable")
	token, _, err := s.RegisterWithVerification(context.Background(), "new@example.com", "Password123!", "", "", "TEAM2026", "")
	require.ErrorIs(t, err, ErrServiceUnavailable)
	require.Empty(t, token)
}

func TestPersonalInvitationAdminCodePrecedenceAndSingleUse(t *testing.T) {
	s, affiliates, _, _ := newPersonalInvitationTestService()
	codes := s.redeemRepo.(*raceSafeRedeemRepo).codes
	codes["TEAM2026"] = &RedeemCode{ID: 1, Code: "TEAM2026", Type: RedeemTypeInvitation, Status: StatusUnused}
	ctx := context.Background()
	_, _, err := s.RegisterWithVerification(ctx, "one@example.com", "Password123!", "", "", "TEAM2026", "")
	require.NoError(t, err)
	_, _, err = s.RegisterWithVerification(ctx, "two@example.com", "Password123!", "", "", "TEAM2026", "")
	require.ErrorIs(t, err, ErrInvitationCodeInvalid)
	require.Zero(t, affiliates.profiles[100].AffCount)
	expired := time.Now().Add(-time.Hour)
	codes["TEAM2026"].Status = StatusUnused
	codes["TEAM2026"].ExpiresAt = &expired
	require.ErrorIs(t, s.ValidateRegistrationInvitation(ctx, "TEAM2026"), ErrInvitationCodeInvalid)
	codes["TEAM2026"].ExpiresAt = nil
	codes["TEAM2026"].Type = RedeemTypeBalance
	require.ErrorIs(t, s.ValidateRegistrationInvitation(ctx, "TEAM2026"), ErrInvitationCodeInvalid)
}

func TestPersonalInvitationOAuthFinalize(t *testing.T) {
	s, affiliates, users, _ := newPersonalInvitationTestService()
	ctx := context.Background()
	require.NoError(t, s.ValidateRegistrationInvitation(ctx, "TEAM2026"))
	redeem, err := s.validateOAuthRegistrationInvitation(ctx, "TEAM2026")
	require.NoError(t, err)
	require.Nil(t, redeem)
	u := &User{Email: "oauth@example.com", Status: StatusActive}
	require.NoError(t, users.CreateWithEmailAliasGuard(ctx, u))
	require.NoError(t, s.FinalizeOAuthEmailAccount(ctx, u, "TEAM2026", "oidc", "OTHER-CODE"))
	require.Equal(t, int64(100), *affiliates.profiles[u.ID].InviterID)
	require.Equal(t, 1, affiliates.profiles[100].AffCount)
}

func TestPersonalInvitationCreationTransaction(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "commit", true: "rollback"}[fail], func(t *testing.T) {
			s, affiliates, _, _ := newPersonalInvitationTestService()
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			s.entClient = dbent.NewClient(dbent.Driver(entsql.OpenDB(dialect.Postgres, db)))
			mock.ExpectBegin()
			if fail {
				affiliates.bindErr = errors.New("binding failed")
				mock.ExpectRollback()
			} else {
				mock.ExpectCommit()
			}
			err = s.createUserAndClaimInvitation(context.Background(), &User{Email: "transaction@example.com"}, nil, 100)
			if fail {
				require.ErrorIs(t, err, affiliates.bindErr)
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// OAuth's legacy entry point uses Create directly.
func (s *raceSafeUserRepo) Create(ctx context.Context, user *User) error {
	return s.CreateWithEmailAliasGuard(ctx, user)
}

func TestPersonalInvitationOAuthRegistration(t *testing.T) {
	s, affiliates, _, settings := newPersonalInvitationTestService()
	ctx := context.Background()
	for _, email := range []string{"oauth-one@example.com", "oauth-two@example.com"} {
		pair, u, err := s.LoginOrRegisterOAuthWithTokenPair(ctx, email, "member", "TEAM2026", "OTHER-CODE", "oidc")
		require.NoError(t, err)
		require.NotEmpty(t, pair.AccessToken)
		require.Equal(t, int64(100), *affiliates.profiles[u.ID].InviterID)
	}
	require.Equal(t, 2, affiliates.profiles[100].AffCount)
	settings[SettingKeyAffiliateInvitationCodeEnabled] = "false"
	_, _, err := s.LoginOrRegisterOAuthWithTokenPair(ctx, "oauth-three@example.com", "member", "TEAM2026", "", "oidc")
	require.ErrorIs(t, err, ErrInvitationCodeInvalid)
	// Existing accounts can still log in without a registration credential.
	_, _, err = s.LoginOrRegisterOAuthWithTokenPair(ctx, "oauth-one@example.com", "member", "", "", "oidc")
	require.NoError(t, err)
	require.Equal(t, 2, affiliates.profiles[100].AffCount)
}
