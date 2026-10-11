import type { GatewayEvent } from '@hermes/shared'
import { act, cleanup } from '@testing-library/react'
import { afterEach, describe, expect, it } from 'vitest'

import { textPart } from '@/lib/chat-messages'
import { createClientSessionState, toRuntimeMessage } from '@/lib/chat-runtime'
import { $composerAttachments } from '@/store/composer'

import { renderMessageStream } from './test-harness'

const SID = 'shared-session'

const input = (text: string, row_id: number | null, origin: string | null = 'matrix'): GatewayEvent => ({
  type: 'session.clients.input',
  session_id: SID,
  payload: { text, row_id, origin }
})

afterEach(cleanup)

describe('shared session events through the live dispatcher', () => {
  it('shows one remote prompt before its streamed answer and ignores replayed row IDs', () => {
    const stream = renderMessageStream(SID)
    act(() => {
      stream.handleEvent(input('remote prompt', 42))
      stream.handleEvent({ type: 'message.start', session_id: SID })
      stream.handleEvent({ type: 'message.delta', session_id: SID, payload: { text: 'answer' } })
      stream.handleEvent({ type: 'message.complete', session_id: SID, payload: { text: 'answer' } })
      stream.handleEvent(input('remote prompt', 42))
    })
    expect(stream.state().messages.map(m => m.role)).toEqual(['user', 'assistant'])
    expect(stream.state().messages[0]).toMatchObject({ rowId: 42, parts: [textPart('remote prompt')] })
    expect(stream.text()).toBe('answer')
    act(() => stream.handleEvent(input('remote prompt', 43, 'desktop')))
    expect(stream.state().messages.filter(m => m.role === 'user')).toHaveLength(2)
  })

  it('adopts its own pending optimistic send before the submit RPC replies', () => {
    const optimistic = {
      id: 'local-send',
      role: 'user' as const,
      parts: [textPart('/skill task')],
      submittedInputText: 'expanded skill task',
      attachmentRefs: ['@image:/tmp/local.png']
    }

    const states = new Map([[SID, { ...createClientSessionState(), messages: [optimistic], awaitingResponse: true }]])
    const stream = renderMessageStream(SID, { states })
    act(() => {
      stream.handleEvent(input('expanded skill task', 51, null))
      stream.handleEvent({ type: 'message.start', session_id: SID })
      stream.handleEvent(input('expanded skill task', 51, null))
    })
    expect(stream.state().messages).toEqual([{ ...optimistic, rowId: 51, submittedInputText: undefined }])
    expect(stream.state().turnLive).toBe(true)
  })

  it('routes background inputs and received files without staging them in the composer', () => {
    const stream = renderMessageStream('foreground')
    const before = $composerAttachments.get()
    act(() => {
      stream.handleEvent(input('background prompt', null, 'slack'))

      for (const [path, name] of [
        ['/host/photo space.png', 'photo space.png'],
        ['/host/report.pdf', 'report.pdf']
      ]) {
        stream.handleEvent({
          type: 'session.clients.attachment',
          session_id: SID,
          payload: { path, name, origin: 'slack' }
        })
      }
    })
    expect(stream.state().messages).toEqual([])
    expect(stream.state(SID).messages).toHaveLength(3)
    const attachments = stream.state(SID).messages.slice(1).map(toRuntimeMessage)
    expect(attachments.map(m => m.metadata.custom?.receivedAttachment)).toEqual([
      { path: '/host/photo space.png', name: 'photo space.png' },
      { path: '/host/report.pdf', name: 'report.pdf' }
    ])
    expect($composerAttachments.get()).toBe(before)
  })

  it('does not attribute an unscoped shared prompt to whichever chat is focused', () => {
    const stream = renderMessageStream(SID)
    act(() => stream.handleEvent({ ...input('unattributed', 80), session_id: undefined }))
    expect(stream.state().messages).toEqual([])
  })
})
