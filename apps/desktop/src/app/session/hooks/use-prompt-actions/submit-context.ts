import type { ClientSessionState } from '@/app/types'
import type { ChatMessage } from '@/lib/chat-messages'
import type { ComposerAttachment } from '@/store/composer'

/** Freeze the context sent to the backend so its shared-input echo identifies this send. */
export function buildContextText(attachments: ComposerAttachment[], visibleText: string): string {
  const present = attachments.filter((a): a is ComposerAttachment => Boolean(a))

  const refs = present
    .map(a => a.refText)
    .filter(Boolean)
    .join('\n')

  return (
    [refs, visibleText].filter(Boolean).join('\n\n') ||
    (present.some(a => a.kind === 'image') ? 'What do you see in this image?' : '')
  )
}

/** Synced file references replace the local preview and its pending echo identity together. */
export function replaceOptimisticUserMessage(state: ClientSessionState, message: ChatMessage): ClientSessionState {
  return { ...state, messages: state.messages.map(current => (current.id === message.id ? message : current)) }
}
