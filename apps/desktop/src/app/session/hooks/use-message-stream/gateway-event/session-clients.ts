import type { GatewayEvent } from '@hermes/shared'

import { finalizeInterruptedMessages, textPart, toChatMessages } from '@/lib/chat-messages'

import type { GatewayEventContext } from './types'

/** Shared-client frames precede message.start and update only the addressed transcript. */
export function handleSessionClientsEvent(ctx: GatewayEventContext): boolean {
  const { event, explicitSid: sessionId, deps, occurredAt } = ctx

  if (event.type === 'session.clients.input') {
    const input = (event as GatewayEvent<'session.clients.input'>).payload

    if (!sessionId || !input) {
      return true
    }

    deps.updateSessionState(sessionId, state => {
      const rowId = input.row_id ?? undefined

      if (rowId !== undefined && state.messages.some(message => message.rowId === rowId)) {
        return state
      }

      const pending = state.messages.findIndex(
        message => message.role === 'user' && message.submittedInputText === input.text && message.rowId === undefined
      )

      if (pending >= 0) {
        return {
          ...state,
          messages: state.messages.map((message, index) =>
            index === pending ? { ...message, rowId, submittedInputText: undefined } : message
          )
        }
      }

      const message = toChatMessages([{ role: 'user', content: input.text }])[0]

      if (!message) {
        return state
      }

      return {
        ...state,
        messages: [
          ...finalizeInterruptedMessages(state.messages, state.streamId),
          {
            ...message,
            id: rowId === undefined ? `client-input-${crypto.randomUUID()}` : `client-input-${rowId}`,
            rowId,
            timestamp: occurredAt
          }
        ],
        streamId: null,
        interrupted: false
      }
    })

    return true
  }

  if (event.type === 'session.clients.attachment') {
    const attachment = (event as GatewayEvent<'session.clients.attachment'>).payload

    if (sessionId && attachment?.path) {
      deps.updateSessionState(sessionId, state => ({
        ...state,
        messages: [
          ...state.messages,
          {
            id: `client-attachment-${crypto.randomUUID()}`,
            role: 'system',
            parts: [textPart(attachment.name)],
            receivedAttachment: { path: attachment.path, name: attachment.name },
            timestamp: occurredAt
          }
        ]
      }))
    }

    return true
  }

  return false
}
