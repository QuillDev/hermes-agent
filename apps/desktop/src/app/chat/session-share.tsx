import { useState } from 'react'

import { Button } from '@/components/ui/button'
import { CopyButton } from '@/components/ui/copy-button'
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { useI18n } from '@/i18n'

import { type SessionShareToken, shareSession } from './session-share-action'

export function SessionShare({ runtimeId }: { runtimeId: string }) {
  const { t } = useI18n()
  const copy = t.sessionClients
  const [open, setOpen] = useState(false)
  const [busy, setBusy] = useState(false)
  const [token, setToken] = useState<SessionShareToken | null>(null)
  const [error, setError] = useState<string | null>(null)

  const share = async () => {
    setOpen(true)
    setBusy(true)
    setToken(null)
    setError(null)

    try {
      setToken(await shareSession(runtimeId))
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setBusy(false)
    }
  }

  return (
    <>
      <Button
        className="pointer-events-auto shrink-0 [-webkit-app-region:no-drag]"
        disabled={busy}
        onClick={() => void share()}
        size="sm"
        variant="ghost"
      >
        {copy.share}
      </Button>
      <Dialog onOpenChange={setOpen} open={open}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>{copy.share}</DialogTitle>
            <DialogDescription>{copy.attachHelp}</DialogDescription>
          </DialogHeader>
          {busy && <p role="status">{copy.creating}</p>}
          {error && <p role="alert">{error}</p>}
          {token && (
            <div className="grid gap-3">
              <code className="break-all select-text">/attach {token.token}</code>
              <CopyButton label={copy.copy} showLabel text={`/attach ${token.token}`} />
              <p className="text-sm text-(--ui-text-secondary)">{copy.expires}</p>
              <p className="text-sm text-(--ui-text-secondary)">{copy.keepAlive}</p>
            </div>
          )}
        </DialogContent>
      </Dialog>
    </>
  )
}
