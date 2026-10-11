import { useStore } from '@nanostores/react'
import { useEffect, useState } from 'react'

import { useI18n } from '@/i18n'
import { $activeConnectionId } from '@/store/connections'
import { requestGatewayForAgent } from '@/store/gateway'
import { notifyError } from '@/store/notifications'
import { $settingsScopeProfile } from '@/store/settings-scope'

import { ToggleRow } from './primitives'

export function SessionClientsSettings() {
  const connectionId = useStore($activeConnectionId)
  const profile = useStore($settingsScopeProfile)

  return <SessionClientsToggle connectionId={connectionId} key={`${connectionId}:${profile}`} profile={profile} />
}

function SessionClientsToggle({ connectionId, profile }: { connectionId: string | null; profile: string }) {
  const { t } = useI18n()
  const copy = t.sessionClients
  const [enabled, setEnabled] = useState(false)
  const [loaded, setLoaded] = useState(false)
  const [busy, setBusy] = useState(false)
  const [failed, setFailed] = useState(false)
  const scope = profile === 'custom' ? {} : { profile }

  useEffect(() => {
    let cancelled = false

    void requestGatewayForAgent<{ value?: unknown }>(connectionId, profile, 'config.get', {
      key: 'session_clients.enabled',
      ...(profile === 'custom' ? {} : { profile })
    })
      .then(result => {
        if (!cancelled) {
          setEnabled(result.value === true || result.value === 'true')
          setLoaded(true)
        }
      })
      .catch(() => {
        if (!cancelled) {
          setFailed(true)
        }
      })

    return () => {
      cancelled = true
    }
  }, [connectionId, profile])

  const save = async (next: boolean) => {
    setEnabled(next)
    setBusy(true)

    try {
      await requestGatewayForAgent(connectionId, profile, 'config.set', {
        key: 'session_clients.enabled',
        value: String(next),
        ...scope
      })
    } catch (error) {
      setEnabled(!next)
      notifyError(error, copy.saveFailed)
    } finally {
      setBusy(false)
    }
  }

  return (
    <ToggleRow
      checked={enabled}
      description={copy.enableHelp}
      disabled={!loaded || busy}
      hint={failed ? copy.unavailable : undefined}
      id="setting-session-clients"
      label={copy.enable}
      onChange={next => void save(next)}
    />
  )
}
