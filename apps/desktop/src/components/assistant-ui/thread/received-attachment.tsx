import { MessagePrimitive } from '@assistant-ui/react'

import { DirectiveContent, formatRefValue } from '@/components/assistant-ui/directive-text'
import { PreviewAttachment } from '@/components/chat/preview-attachment'
import { mediaKind } from '@/lib/media'

interface ReceivedAttachmentProps {
  attachment: { path: string; name: string }
}

/** Received files are transcript content, never pending composer uploads. */
export function ReceivedAttachment({ attachment }: ReceivedAttachmentProps) {
  return (
    <MessagePrimitive.Root className="w-full min-w-0 self-start" data-role="system">
      <div className="text-xs text-muted-foreground">{attachment.name}</div>
      {mediaKind(attachment.path) === 'image' ? (
        <DirectiveContent text={`@image:${formatRefValue(attachment.path)}`} />
      ) : (
        <PreviewAttachment target={attachment.path} />
      )}
      <div className="break-all text-xs text-muted-foreground">{attachment.path}</div>
    </MessagePrimitive.Root>
  )
}
