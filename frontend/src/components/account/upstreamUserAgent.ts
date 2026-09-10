export const MAX_UPSTREAM_USER_AGENT_BYTES = 200

export function validateUpstreamUserAgent(value: string): string | null {
  if (Array.from(value).some((character) => {
    const code = character.charCodeAt(0)
    return code < 32 || (code >= 127 && code <= 159)
  })) {
    return 'admin.accounts.upstreamUserAgentControlCharacters'
  }
  if (new TextEncoder().encode(value.trim()).length > MAX_UPSTREAM_USER_AGENT_BYTES) {
    return 'admin.accounts.upstreamUserAgentTooLong'
  }
  return null
}

export function withUpstreamUserAgent(
  extra: Record<string, unknown> | undefined,
  value: string
): Record<string, unknown> {
  const result = { ...extra }
  const normalized = value.trim()
  if (normalized) {
    result.upstream_user_agent = normalized
  } else {
    delete result.upstream_user_agent
  }
  return result
}
