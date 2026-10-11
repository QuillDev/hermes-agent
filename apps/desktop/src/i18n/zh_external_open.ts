import type { TranslationOverrides } from './define-locale'

export const zhExternalOpenFailed: NonNullable<TranslationOverrides['externalOpenFailed']> = {
  title: '无法打开此链接',
  message: '没有注册用于打开此地址的浏览器。请复制链接并手动打开。',
  copyUrl: '复制链接',
  close: '关闭'
}
