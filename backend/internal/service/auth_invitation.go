package service

import (
	"context"
	"errors"
	"strings"
)

// resolveRegistrationInvitation gives one-time admin codes precedence. Personal
// codes are reusable and establish the same relationship used by affiliate reports
// and payment rebates, even when rebate payments are disabled.
func (s *AuthService) resolveRegistrationInvitation(ctx context.Context, rawCode string) (*RedeemCode, int64, error) {
	if s == nil || s.settingService == nil || !s.settingService.IsInvitationCodeEnabled(ctx) {
		return nil, 0, nil
	}
	code := strings.TrimSpace(rawCode)
	if code == "" {
		return nil, 0, ErrInvitationCodeRequired
	}
	if s.redeemRepo == nil && s.oauthEmailFlowClient(ctx) == nil {
		return nil, 0, ErrServiceUnavailable
	}
	redeem, err := s.loadOAuthRegistrationInvitation(ctx, code)
	if err == nil {
		if redeem == nil || redeem.Type != RedeemTypeInvitation || !redeem.CanUse() {
			return nil, 0, ErrInvitationCodeInvalid
		}
		return redeem, 0, nil
	}
	// Never turn database failures or exhausted admin codes into a fallback credential.
	if !errors.Is(err, ErrRedeemCodeNotFound) {
		return nil, 0, ErrInvitationCodeInvalid
	}
	if !s.settingService.IsAffiliateInvitationCodeEnabled(ctx) {
		return nil, 0, ErrInvitationCodeInvalid
	}
	if s.affiliateService == nil || s.affiliateService.repo == nil {
		return nil, 0, ErrServiceUnavailable
	}
	code = strings.ToUpper(code)
	if !isValidAffiliateCodeFormat(code) {
		return nil, 0, ErrInvitationCodeInvalid
	}
	inviter, err := s.affiliateService.repo.GetAffiliateByCode(ctx, code)
	if err != nil || inviter == nil || inviter.UserID <= 0 {
		return nil, 0, ErrInvitationCodeInvalid
	}
	owner, err := s.userRepo.GetByID(ctx, inviter.UserID)
	if err != nil || owner == nil || !owner.IsActive() {
		return nil, 0, ErrInvitationCodeInvalid
	}
	return nil, inviter.UserID, nil
}

// ValidateRegistrationInvitation is shared by registration and its public preflight.
func (s *AuthService) ValidateRegistrationInvitation(ctx context.Context, code string) error {
	_, _, err := s.resolveRegistrationInvitation(ctx, code)
	return err
}

func (s *AuthService) bindRegistrationInviter(ctx context.Context, userID, inviterID int64) error {
	if inviterID == 0 {
		return nil
	}
	if userID == inviterID {
		return ErrInvitationCodeInvalid
	}
	bound, err := s.affiliateService.repo.BindInviter(ctx, userID, inviterID)
	if err != nil {
		return err
	}
	if !bound {
		return ErrAffiliateAlreadyBound
	}
	return nil
}
