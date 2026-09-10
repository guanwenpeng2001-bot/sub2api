import { describe, expect, it } from 'vitest'
import { validateUpstreamUserAgent, withUpstreamUserAgent } from '../upstreamUserAgent'

describe('management upstream User-Agent', () => {
  it.each(Array.from({ length: 160 }, (_, code) => code).filter(code => code < 32 || code >= 127))(
    'rejects control character %i even at value boundaries', (code) => {
      expect(validateUpstreamUserAgent(String.fromCharCode(code) + 'agent')).toBe('admin.accounts.upstreamUserAgentControlCharacters')
    }
  )
  it('enforces the backend UTF-8 byte boundary after trimming', () => {
    for (const value of ['', '   ', ' a'.trim().repeat(200), '界'.repeat(66) + 'ab', '😀'.repeat(50)]) {
      expect(validateUpstreamUserAgent('  ' + value + '  ')).toBeNull()
    }
    for (const value of ['a'.repeat(201), '界'.repeat(67), '😀'.repeat(50) + 'a']) {
      expect(validateUpstreamUserAgent(value)).toBe('admin.accounts.upstreamUserAgentTooLong')
    }
  })
  it('sets and deletes only the UA key without mutating the original extra', () => {
    const original = { upstream_user_agent: 'old', sync: { models: ['a'] }, other: true }
    expect(withUpstreamUserAgent(original, ' new ')).toEqual({ ...original, upstream_user_agent: 'new' })
    expect(withUpstreamUserAgent(original, '')).toEqual({ sync: original.sync, other: true })
    expect(original.upstream_user_agent).toBe('old')
    expect(withUpstreamUserAgent(undefined, '')).toEqual({})
  })
})
