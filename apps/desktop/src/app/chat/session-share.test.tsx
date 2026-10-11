import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { SessionShare } from './session-share'

const server = vi.hoisted(() => ({
  error: '',
  tokens: [] as Array<{ connection: string; profile: string; session: unknown }>,
  clipboard: ''
}))

vi.mock('@/store/session-states', () => ({
  knownOwnerForSession: () => ({ connectionId: 'local', profile: 'research' })
}))
vi.mock('@/store/gateway', () => ({
  requestGatewayForAgent: async (
    connection: string,
    profile: string,
    method: string,
    params: Record<string, unknown>
  ) => {
    if (method !== 'session.clients.share') {
      throw new Error(`Unexpected RPC: ${method}`)
    }

    if (server.error) {
      throw new Error(server.error)
    }

    server.tokens.push({ connection, profile, session: params.session_id })

    return { token: 'single-use-token', expires_at: Date.now() / 1000 + 600 }
  }
}))

beforeEach(() => {
  server.error = ''
  server.tokens = []
  server.clipboard = ''
  Object.defineProperty(navigator, 'clipboard', {
    configurable: true,
    value: {
      writeText: async (text: string) => {
        server.clipboard = text
      }
    }
  })
})
afterEach(cleanup)

describe('session sharing', () => {
  it('shares the live runtime on its owner and gives the user a copyable attach command', async () => {
    render(<SessionShare runtimeId="runtime-live" />)
    fireEvent.click(screen.getByRole('button', { name: 'Share to Discord' }))

    expect(await screen.findByText('/attach single-use-token')).toBeTruthy()
    expect(server.tokens).toEqual([{ connection: 'local', profile: 'research', session: 'runtime-live' }])
    expect(screen.getByText(/single use and expires in 10 minutes/)).toBeTruthy()
    fireEvent.click(screen.getByRole('button', { name: 'Copy attach command' }))
    await waitFor(() => expect(server.clipboard).toBe('/attach single-use-token'))
  })

  it('shows a backend refusal without exposing a token and allows another attempt', async () => {
    server.error = 'Session sharing is disabled'
    render(<SessionShare runtimeId="runtime-live" />)
    fireEvent.click(screen.getByRole('button', { name: 'Share to Discord' }))

    expect((await screen.findByRole('alert')).textContent).toBe('Session sharing is disabled')
    expect(screen.queryByRole('button', { name: 'Copy attach command' })).toBeNull()
    expect(server.tokens).toEqual([])
    fireEvent.click(screen.getByRole('button', { name: 'Close' }))
    server.error = ''
    fireEvent.click(screen.getByRole('button', { name: 'Share to Discord' }))
    expect(await screen.findByText('/attach single-use-token')).toBeTruthy()
  })
})
