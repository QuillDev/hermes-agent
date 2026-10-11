export interface SessionClientsTranslations {
  share: string
  attachHelp: string
  creating: string
  copy: string
  expires: string
  keepAlive: string
  enable: string
  enableHelp: string
  saveFailed: string
  unavailable: string
}

export const sessionClientsCopy: Record<string, SessionClientsTranslations> = {
  en: {
    share: 'Share to Discord',
    attachHelp: 'Paste this command in an authorized Discord channel to create a new thread linked to this session.',
    creating: 'Creating token…',
    copy: 'Copy attach command',
    expires: 'This token is single use and expires in 10 minutes.',
    keepAlive: 'Keep the process running that owns this session. Sharing does not move it to Discord.',
    enable: 'Enable session sharing',
    enableHelp:
      'Off by default. Share sessions with Discord on the same host and profile. Private chats are never mirrored automatically.',
    saveFailed: 'Could not save session sharing',
    unavailable: 'Session sharing settings are unavailable.'
  },
  zh: {
    share: '分享到 Discord',
    attachHelp: '将此命令粘贴到已授权的 Discord 频道，创建关联此会话的新帖子。',
    creating: '正在创建令牌…',
    copy: '复制关联命令',
    expires: '令牌仅可使用一次，10 分钟后过期。',
    keepAlive: '请保持拥有此会话的进程运行。分享不会将会话迁移到 Discord。',
    enable: '启用会话分享',
    enableHelp: '默认关闭。仅可在同一主机和配置中与 Discord 分享会话。私人聊天不会自动同步。',
    saveFailed: '无法保存会话分享设置',
    unavailable: '会话分享设置不可用。'
  },
  'zh-hant': {
    share: '分享到 Discord',
    attachHelp: '將此命令貼到已授權的 Discord 頻道，以建立連結此工作階段的新討論串。',
    creating: '正在建立權杖…',
    copy: '複製連結命令',
    expires: '權杖僅可使用一次，10 分鐘後到期。',
    keepAlive: '請保持擁有此工作階段的程序執行。分享不會將工作階段遷移至 Discord。',
    enable: '啟用工作階段分享',
    enableHelp: '預設關閉。僅能在相同主機和設定檔中與 Discord 分享。私人聊天不會自動同步。',
    saveFailed: '無法儲存分享設定',
    unavailable: '工作階段分享設定無法使用。'
  },
  ja: {
    share: 'Discord に共有',
    attachHelp:
      '許可された Discord チャンネルにこのコマンドを貼り付け、このセッションにリンクする新しいスレッドを作成します。',
    creating: 'トークンを作成中…',
    copy: '接続コマンドをコピー',
    expires: 'トークンは一度だけ使用でき、10 分後に失効します。',
    keepAlive: 'このセッションを所有するプロセスを実行したままにしてください。共有しても Discord に移行しません。',
    enable: 'セッション共有を有効化',
    enableHelp:
      '既定ではオフです。同じホストとプロファイルの Discord と共有します。プライベートチャットは自動転送されません。',
    saveFailed: '共有設定を保存できませんでした',
    unavailable: 'セッション共有設定を利用できません。'
  },
  de: {
    share: 'Mit Discord teilen',
    attachHelp:
      'Diesen Befehl in einen autorisierten Discord-Kanal einfügen, um einen neuen, mit dieser Sitzung verknüpften Thread zu erstellen.',
    creating: 'Token wird erstellt…',
    copy: 'Verknüpfungsbefehl kopieren',
    expires: 'Dieses Token ist einmal verwendbar und läuft nach 10 Minuten ab.',
    keepAlive: 'Der Prozess, dem diese Sitzung gehört, muss weiterlaufen. Teilen verschiebt sie nicht zu Discord.',
    enable: 'Sitzungen teilen aktivieren',
    enableHelp:
      'Standardmäßig aus. Sitzungen mit Discord auf demselben Host und im selben Profil teilen. Private Chats werden nie automatisch gespiegelt.',
    saveFailed: 'Teilen-Einstellung konnte nicht gespeichert werden',
    unavailable: 'Einstellungen zum Teilen von Sitzungen sind nicht verfügbar.'
  },
  es: {
    share: 'Compartir en Discord',
    attachHelp: 'Pega este comando en un canal autorizado de Discord para crear un hilo nuevo vinculado a esta sesión.',
    creating: 'Creando token…',
    copy: 'Copiar comando de enlace',
    expires: 'Este token es de un solo uso y caduca en 10 minutos.',
    keepAlive: 'Mantén en ejecución el proceso propietario de esta sesión. Compartir no la traslada a Discord.',
    enable: 'Activar sesiones compartidas',
    enableHelp:
      'Desactivado por defecto. Comparte sesiones con Discord en el mismo equipo y perfil. Los chats privados nunca se reflejan automáticamente.',
    saveFailed: 'No se pudo guardar el ajuste',
    unavailable: 'Los ajustes de sesiones compartidas no están disponibles.'
  },
  fr: {
    share: 'Partager sur Discord',
    attachHelp: 'Collez cette commande dans un canal Discord autorisé pour créer un nouveau fil lié à cette session.',
    creating: 'Création du jeton…',
    copy: 'Copier la commande de liaison',
    expires: 'Ce jeton est à usage unique et expire dans 10 minutes.',
    keepAlive: 'Laissez le processus propriétaire de cette session en cours. Le partage ne la transfère pas à Discord.',
    enable: 'Activer le partage de sessions',
    enableHelp:
      'Désactivé par défaut. Partagez avec Discord sur le même hôte et profil. Les conversations privées ne sont jamais relayées automatiquement.',
    saveFailed: 'Impossible d’enregistrer le réglage',
    unavailable: 'Les réglages du partage de sessions sont indisponibles.'
  },
  ru: {
    share: 'Поделиться в Discord',
    attachHelp:
      'Вставьте эту команду в разрешённом канале Discord, чтобы создать новую ветку, связанную с этой сессией.',
    creating: 'Создание токена…',
    copy: 'Копировать команду подключения',
    expires: 'Токен одноразовый и истекает через 10 минут.',
    keepAlive: 'Процесс, которому принадлежит сессия, должен работать. Публикация не переносит её в Discord.',
    enable: 'Разрешить общий доступ к сессиям',
    enableHelp:
      'По умолчанию выключено. Общий доступ через Discord на том же хосте и в том же профиле. Личные чаты никогда не транслируются автоматически.',
    saveFailed: 'Не удалось сохранить настройку',
    unavailable: 'Настройки общего доступа недоступны.'
  },
  ar: {
    share: 'المشاركة مع Discord',
    attachHelp: 'الصق هذا الأمر في قناة Discord مصرح بها لإنشاء سلسلة محادثة جديدة مرتبطة بهذه الجلسة.',
    creating: 'جارٍ إنشاء الرمز…',
    copy: 'نسخ أمر الربط',
    expires: 'هذا الرمز للاستخدام مرة واحدة وتنتهي صلاحيته خلال 10 دقائق.',
    keepAlive: 'أبقِ العملية المالكة لهذه الجلسة قيد التشغيل. المشاركة لا تنقلها إلى Discord.',
    enable: 'تفعيل مشاركة الجلسات',
    enableHelp:
      'معطّل افتراضيًا. شارك الجلسات مع Discord على المضيف والملف الشخصي نفسيهما. لا تُنسخ المحادثات الخاصة تلقائيًا.',
    saveFailed: 'تعذّر حفظ إعداد المشاركة',
    unavailable: 'إعدادات مشاركة الجلسات غير متاحة.'
  }
}
