import { requestForSessionProfile } from '@/store/session-request-router'
import { knownOwnerForSession } from '@/store/session-states'

export interface SessionShareToken {
  token: string
  expires_at: number
}

export async function shareSession(runtimeId: string): Promise<SessionShareToken> {
  const owner = knownOwnerForSession(runtimeId)

  if (!runtimeId || !owner) {
    throw new Error('Reopen this session before sharing it.')
  }

  return requestForSessionProfile<SessionShareToken>(
    owner,
    () => Promise.reject(new Error('Session owner is unavailable.')),
    'session.clients.share',
    { session_id: runtimeId }
  )
}
