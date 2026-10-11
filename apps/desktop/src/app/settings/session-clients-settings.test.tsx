import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { atom } from 'nanostores'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { SessionClientsSettings } from './session-clients-settings'

const server = vi.hoisted(() => ({
  values: {} as Record<string, unknown>,
  failSave: false
}))

vi.mock('@/store/connections', () => ({ $activeConnectionId: atom('local') }))
vi.mock('@/store/settings-scope', () => ({ $settingsScopeProfile: atom('research') }))
vi.mock('@/store/notifications', () => ({ notifyError: vi.fn() }))
vi.mock('@/store/gateway', () => ({
  requestGatewayForAgent: async (
    _connection: string,
    _profile: string,
    method: string,
    params: Record<string, unknown>
  ) => {
    const key = `${params.profile}:${params.key}`

    if (method === 'config.set') {
      if (server.failSave) {
        throw new Error('Save failed')
      }

      server.values[key] = params.value
    }

    return { value: server.values[key] }
  }
}))

beforeEach(() => {
  server.values = {}
  server.failSave = false
})
afterEach(cleanup)

describe('session sharing opt-in', () => {
  it('defaults off when unset and saves only the selected profile when enabled', async () => {
    render(<SessionClientsSettings />)
    const toggle = screen.getByRole('switch', { name: 'Enable session sharing' })
    expect(toggle.getAttribute('aria-checked')).toBe('false')
    await waitFor(() => expect(toggle.hasAttribute('disabled')).toBe(false))

    fireEvent.click(toggle)
    await waitFor(() => expect(server.values).toEqual({ 'research:session_clients.enabled': 'true' }))
    expect(toggle.getAttribute('aria-checked')).toBe('true')
  })

  it('restores the confirmed off value when saving fails', async () => {
    server.failSave = true
    render(<SessionClientsSettings />)
    const toggle = screen.getByRole('switch', { name: 'Enable session sharing' })
    await waitFor(() => expect(toggle.hasAttribute('disabled')).toBe(false))
    fireEvent.click(toggle)

    await waitFor(() => expect(toggle.getAttribute('aria-checked')).toBe('false'))
    expect(server.values).toEqual({})
  })
})
