import type * as AssistantUI from '@assistant-ui/react'
import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react'
import type { ReactNode } from 'react'
import { afterEach, beforeEach, expect, it, vi } from 'vitest'

import type * as DirectiveText from '@/components/assistant-ui/directive-text'

import { SystemMessage } from './system-message'

const fake = vi.hoisted(() => ({
  attachment: { path: '', name: '' },
  imageRef: vi.fn(),
  openFile: vi.fn()
}))

vi.mock('@assistant-ui/react', async importOriginal => ({
  ...(await importOriginal<typeof AssistantUI>()),
  MessagePrimitive: { Root: ({ children }: { children: ReactNode }) => <div>{children}</div> },
  useAuiState: (selector: (state: unknown) => unknown) =>
    selector({
      message: {
        content: [{ type: 'text', text: fake.attachment.name }],
        metadata: { custom: { receivedAttachment: fake.attachment } }
      }
    })
}))

vi.mock('@/components/assistant-ui/directive-text', async importOriginal => ({
  ...(await importOriginal<typeof DirectiveText>()),
  DirectiveContent: ({ text }: { text: string }) => {
    fake.imageRef(text)

    return <div data-testid="resolved-image">{text}</div>
  }
}))

vi.mock('@/components/chat/preview-attachment', () => ({
  PreviewAttachment: ({ target }: { target: string }) => (
    <button onClick={() => fake.openFile(target)}>Open preview</button>
  )
}))

beforeEach(() => vi.clearAllMocks())
afterEach(cleanup)

it('renders a received image through the existing image-reference resolver with its host path intact', async () => {
  fake.attachment = { path: '/host/photos/shared image.png', name: 'shared image.png' }
  render(<SystemMessage />)
  await waitFor(() => expect(fake.imageRef).toHaveBeenCalledWith('@image:`/host/photos/shared image.png`'))
  expect(screen.getByText('/host/photos/shared image.png')).toBeTruthy()
  expect(screen.queryByRole('button', { name: 'Open preview' })).toBeNull()
})

it('offers the existing file preview action for a received document', () => {
  fake.attachment = { path: '/host/reports/shared.pdf', name: 'shared.pdf' }
  render(<SystemMessage />)
  fireEvent.click(screen.getByRole('button', { name: 'Open preview' }))
  expect(fake.openFile).toHaveBeenCalledWith('/host/reports/shared.pdf')
  expect(fake.imageRef).not.toHaveBeenCalled()
  expect(screen.getByText('/host/reports/shared.pdf')).toBeTruthy()
})
