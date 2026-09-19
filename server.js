// جسر واتساب كامل - Baileys + واجهة شبيهة بـ WhatsApp Web
import http from 'node:http'
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { WebSocketServer } from 'ws'
import QRCode from 'qrcode'
import P from 'pino'
import makeWASocket, {
  useMultiFileAuthState,
  fetchLatestBaileysVersion,
  DisconnectReason,
  makeCacheableSignalKeyStore,
  isJidGroup,
  isJidBroadcast,
  isJidStatusBroadcast,
  jidNormalizedUser,
  toNumber,
  getContentType,
} from '@whiskeysockets/baileys'

const isJidUser = (jid) => typeof jid === 'string' && jid.endsWith('@s.whatsapp.net')

process.setMaxListeners(30)
process.on('uncaughtException', (e) => console.error('[uncaughtException]', e?.message || e))
process.on('unhandledRejection', (e) => console.error('[unhandledRejection]', e?.message || e))

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const HOST = '0.0.0.0'
const PORT = 3000
const AUTH_DIR = path.join(__dirname, 'auth_info')
const MEDIA_DIR = path.join(__dirname, 'media')
try { fs.mkdirSync(MEDIA_DIR, { recursive: true }) } catch {}
try { fs.mkdirSync(AUTH_DIR, { recursive: true }) } catch {}

const server = http.createServer((req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`)
  let filePath = url.pathname
  if (filePath === '/') filePath = '/index.html'
  const full = path.join(__dirname, filePath)
  if (!full.startsWith(__dirname)) return res.writeHead(403).end()
  const ext = path.extname(full)
  const types = {
    '.html': 'text/html; charset=utf-8', '.js': 'application/javascript',
    '.css': 'text/css', '.json': 'application/json', '.png': 'image/png',
    '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.webp': 'image/webp',
    '.svg': 'image/svg+xml', '.mp3': 'audio/mpeg', '.mp4': 'video/mp4',
    '.wav': 'audio/wav', '.ogg': 'audio/ogg',
  }
  fs.readFile(full, (err, data) => {
    if (err) return res.writeHead(404).end('Not found')
    res.writeHead(200, { 'Content-Type': types[ext] || 'application/octet-stream', 'Cache-Control': 'no-cache' })
    res.end(data)
  })
})

const wss = new WebSocketServer({ server })

// ---------- تنظيف نص HTML قبل الإرسال لواتساب ----------
function formatWhatsAppText(text) {
  if (!text) return ''
  let t = String(text)
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<\/p>/gi, '\n')
    .replace(/<\/div>/gi, '\n')
    .replace(/<\s*b[^>]*>(.*?)<\s*\/\s*b\s*>/gis, '*$1*')
    .replace(/<\s*strong[^>]*>(.*?)<\s*\/\s*strong\s*>/gis, '*$1*')
    .replace(/<\s*i[^>]*>(.*?)<\s*\/\s*i\s*>/gis, '_$1_')
    .replace(/<\s*em[^>]*>(.*?)<\s*\/\s*em\s*>/gis, '_$1_')
    .replace(/<\s*s[^>]*>(.*?)<\s*\/\s*s\s*>/gis, '~$1~')
    .replace(/<\s*strike[^>]*>(.*?)<\s*\/\s*strike\s*>/gis, '~$1~')
    .replace(/<\s*del[^>]*>(.*?)<\s*\/\s*del\s*>/gis, '~$1~')
    .replace(/<\s*code[^>]*>(.*?)<\s*\/\s*code\s*>/gis, '```$1```')
    .replace(/<\s*pre[^>]*>(.*?)<\s*\/\s*pre\s*>/gis, '```$1```')
    .replace(/<[^>]+>/g, '')
    .replace(/&nbsp;/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/\u00A0/g, ' ')
  t = t.replace(/\n{3,}/g, '\n\n').trim()
  return t
}

const clients = new Set()
const broadcast = (payload) => {
  const data = JSON.stringify(payload)
  for (const c of clients) if (c.readyState === 1) c.send(data)
}

let bridgeState = { status: 'starting', qr: null, me: null, meName: '', mePic: null, meStatus: '', initialLoadDone: false }
const setState = (patch) => {
  bridgeState = { ...bridgeState, ...patch }
  console.log('[state]', bridgeState.status, patch.qr ? '(QR)' : '', patch.meName || '')
  broadcast({ type: 'state', ...bridgeState })
}

const chatStore = new Map()
let sock = null
let restartTimer = null
const picCache = new Map()

function getChat(jid) {
  if (!chatStore.has(jid)) chatStore.set(jid, {
    jid, messages: [], unreadCount: 0, lastMsg: null, name: null, pic: null,
    pinned: false, muted: false, isGroup: isJidGroup(jid), presence: 'unavailable',
    ephemeral: 0, isContact: false, lastSeen: null, archived: false,
    about: '', composition: null,
  })
  return chatStore.get(jid)
}

function extractText(m) {
  if (!m) return ''
  const msg = m.message || {}
  if (msg.conversation) return msg.conversation
  if (msg.extendedTextMessage?.text) return msg.extendedTextMessage.text
  if (msg.editedMessage?.message?.protocolMessage?.editedMessage?.conversation) return msg.editedMessage.message.protocolMessage.editedMessage.conversation
  if (msg.editedMessage?.message?.protocolMessage?.editedMessage?.extendedTextMessage?.text) return msg.editedMessage.message.protocolMessage.editedMessage.extendedTextMessage.text
  if (msg.imageMessage?.caption) return msg.imageMessage.caption
  if (msg.videoMessage?.caption) return msg.videoMessage.caption
  if (msg.documentMessage?.caption) return msg.documentMessage.caption
  if (msg.stickerMessage) return '[ملصق]'
  if (msg.imageMessage) return '[صورة]'
  if (msg.videoMessage) return '[فيديو]'
  if (msg.audioMessage) return msg.audioMessage.ptt ? '[رسالة صوتية]' : '[مقطع صوتي]'
  if (msg.documentMessage) return `[مستند: ${msg.documentMessage.fileName || ''}]`
  if (msg.contactMessage) return '[جهة اتصال]'
  if (msg.locationMessage) return '[موقع]'
  if (msg.pollCreationMessage) return '[استطلاع]'
  if (msg.reactionMessage) return '[تفاعل]'
  if (msg.groupInviteMessage) return '[دعوة لمجموعة]'
  if (msg.protocolMessage) return msg.protocolMessage.type === 0 ? '[تم حذف هذه الرسالة]' : ''
  const keys = Object.keys(msg)
  if (keys.length === 0) return ''
  return '[رسالة]'
}

function getMediaType(msg) {
  if (!msg) return null
  if (msg.imageMessage) return 'image'
  if (msg.videoMessage) return 'video'
  if (msg.audioMessage) return msg.audioMessage.ptt ? 'ptt' : 'audio'
  if (msg.stickerMessage) return 'sticker'
  if (msg.documentMessage) return 'document'
  if (msg.contactMessage) return 'contact'
  if (msg.locationMessage) return 'location'
  if (msg.pollCreationMessage) return 'poll'
  return null
}

function formatMsg(m) {
  if (!m) return null
  const msg = m.message || {}
  const key = m.key || {}
  const id = key.id
  const fromMe = !!key.fromMe
  const remoteJid = key.remoteJid
  const participant = key.participant
  const text = extractText(m)
  let ts = m.messageTimestamp ? toNumber(m.messageTimestamp) : Date.now()
  if (ts < 1e12) ts = ts * 1000
  const status = fromMe ? (m.status ?? (m.ack !== undefined ? m.ack : 1)) : null
  const mediaType = getMediaType(m)
  const isRevoked = !!msg.protocolMessage?.type && msg.protocolMessage.type === 0
  const isEdited = !!msg.editedMessage
  const senderName = m.pushName || (participant ? participant.split('@')[0] : '')
  const url = m.url || null
  const mimetype = msg.imageMessage?.mimetype || msg.videoMessage?.mimetype || msg.audioMessage?.mimetype || msg.documentMessage?.mimetype || null
  const caption = msg.imageMessage?.caption || msg.videoMessage?.caption || msg.documentMessage?.caption || ''
  const fileName = msg.documentMessage?.fileName || ''
  const fileSize = msg.documentMessage?.fileLength || msg.imageMessage?.fileLength || msg.videoMessage?.fileLength || 0
  const duration = msg.audioMessage?.seconds || msg.videoMessage?.seconds || 0
  const isViewOnce = !!(msg.imageMessage?.viewOnce || msg.videoMessage?.viewOnce)
  return {
    id, fromMe, remoteJid, participant, text, ts, status,
    mediaType, isRevoked, isEdited, senderName, url,
    mimetype, caption, fileName, fileSize, duration, isViewOnce,
    quoted: msg.extendedTextMessage?.contextInfo?.quotedMessage ? { text: extractText({ message: msg.extendedTextMessage.contextInfo.quotedMessage }) } : null,
  }
}

async function fetchProfilePic(jid, type='preview') {
  if (!sock || !jid) return null
  if (picCache.has(jid + type)) return picCache.get(jid + type)
  try {
    const p = await sock.profilePictureUrl(jid, type === 'image' ? 'image' : 'preview')
    picCache.set(jid + type, p)
    return p
  } catch { return null }
}

async function start() {
  try {
    const { state, saveCreds } = await useMultiFileAuthState(AUTH_DIR)
    let version
    try { ;({ version } = await fetchLatestBaileysVersion()) } catch (e) { console.log('تعذر جلب آخر إصدار') }

    sock = makeWASocket({
      auth: { creds: state.creds, keys: makeCacheableSignalKeyStore(state.keys, P({ level: 'silent' })) },
      logger: P({ level: 'silent' }),
      syncFullHistory: true,
      markOnlineOnConnect: true,
      printQRInTerminal: false,
      generateHighQualityLinkPreview: true,
      ...(version ? { version } : {}),
    })

    sock.ev.on('creds.update', saveCreds)

    sock.ev.on('connection.update', async (update) => {
      const { connection, lastDisconnect, qr, isNewLogin } = update
      console.log('[conn.update]', connection || 'qr?', qr ? 'QR' : '', lastDisconnect?.error?.output?.statusCode || '')
      try {
        if (qr) {
          const qrDataUrl = await QRCode.toDataURL(qr, { margin: 1, width: 264 })
          setState({ status: 'qr', qr: qrDataUrl, initialLoadDone: false })
        }
        if (connection === 'connecting') setState({ status: 'connecting' })
        if (connection === 'open') {
          const me = sock.user
          const myJid = me?.id
          const myName = me?.name || me?.verifiedName || me?.notify || (myJid ? myJid.split('@')[0] : '')
          setState({ status: 'open', qr: null, me: myJid || null, meName: myName, mePic: null })
          // صورة ملفي
          if (myJid) {
            sock.profilePictureUrl(myJid, 'preview').then(p => { if (p) setState({ mePic: p }) }).catch(()=>{})
            sock.fetchStatus(myJid).then(s => { if (s?.status) setState({ meStatus: s.status }) }).catch(()=>{})
          }
          setTimeout(loadAllInitialData, 1500)
        }
        if (connection === 'close') {
          const err = lastDisconnect?.error
          const code = err?.output?.statusCode
          const reason = err?.output?.payload?.error || err?.message || ''
          console.log('[close]', code, reason)
          const shouldReconnect = code !== DisconnectReason.loggedOut && code !== 401
          if (code === DisconnectReason.loggedOut || code === 401) {
            try { fs.rmSync(AUTH_DIR, { recursive: true, force: true }) } catch {}
            chatStore.clear()
            setState({ status: 'logged_out', qr: null, me: null, meName: '', mePic: null, initialLoadDone: false })
          } else {
            setState({ status: 'reconnecting', qr: null })
          }
          if (shouldReconnect) {
            clearTimeout(restartTimer)
            restartTimer = setTimeout(() => start().catch(console.error), 3000)
          } else {
            clearTimeout(restartTimer)
            restartTimer = setTimeout(() => start().catch(console.error), 2000)
          }
        }
      } catch (err) { console.error('[connection.update handler error]', err) }
    })

    sock.ev.on('messaging-history.set', ({ chats, messages, contacts, isLatest }) => {
      try {
        console.log('[history.set] chats=', chats?.length, 'msgs=', messages?.length, 'contacts=', contacts?.length, 'latest=', isLatest)
        // contacts first
        if (contacts) for (const c of contacts) {
          const ch = getChat(c.id)
          if (c.name || c.notify) ch.name = c.name || c.notify
          ch.isContact = !!(c.notify || c.name)
        }
        // chats
        if (chats) for (const c of chats) {
          const ch = getChat(c.id)
          if (c.name) ch.name = c.name
          ch.pinned = !!c.pinned
          ch.muted = !!c.muteEndTime
          ch.unreadCount = c.unreadCount || 0
          ch.ephemeral = c.ephemeralExpiration || 0
          ch.archived = !!c.archived
          if (!isJidGroup(c.id) && !isJidBroadcast(c.id)) ch.isContact = true
        }
        // messages
        if (messages) for (const m of messages) {
          const jid = m.key?.remoteJid; if (!jid) continue
          if (isJidStatusBroadcast(jid) || isJidBroadcast(jid)) continue
          const ch = getChat(jid)
          const fm = formatMsg(m)
          if (fm && fm.id && !ch.messages.find(x => x.id === fm.id)) {
            ch.messages.push(fm)
          }
        }
        for (const ch of chatStore.values()) {
          ch.messages.sort((a, b) => a.ts - b.ts)
          ch.lastMsg = ch.messages[ch.messages.length - 1] || null
        }
        broadcast({ type: 'chats_updated', chats: serializeChats(), loading: true })
      } catch (e) { console.error('history.set error', e) }
    })

    sock.ev.on('chats.set', ({ chats, isLatest }) => {
      try {
        console.log('[chats.set]', chats?.length, 'latest=', isLatest)
        for (const c of chats) {
          const ch = getChat(c.id)
          if (c.name) ch.name = c.name
          ch.pinned = !!c.pinned
          ch.muted = !!c.muteEndTime
          ch.unreadCount = c.unreadCount || 0
          ch.ephemeral = c.ephemeralExpiration || 0
          ch.archived = !!c.archived
        }
        broadcast({ type: 'chats_updated', chats: serializeChats() })
      } catch (e) { console.error('chats.set error', e) }
    })

    sock.ev.on('chats.upsert', (chats) => {
      try {
        for (const c of chats) {
          const ch = getChat(c.id)
          if (c.name) ch.name = c.name
          ch.pinned = !!c.pinned
          ch.muted = !!c.muteEndTime
          ch.archived = !!c.archived
        }
        broadcast({ type: 'chats_updated', chats: serializeChats() })
      } catch (e) { console.error('chats.upsert error', e) }
    })

    sock.ev.on('chats.update', (updates) => {
      try {
        for (const u of updates) {
          const ch = getChat(u.id)
          if (u.unreadCount !== undefined) ch.unreadCount = u.unreadCount
          if (u.muteEndTime !== undefined) ch.muted = !!u.muteEndTime
          if (u.pinned !== undefined) ch.pinned = u.pinned
          if (u.archived !== undefined) ch.archived = u.archived
          if (u.ephemeralExpiration !== undefined) ch.ephemeral = u.ephemeralExpiration
        }
        broadcast({ type: 'chats_updated', chats: serializeChats() })
      } catch (e) { console.error('chats.update error', e) }
    })

    sock.ev.on('messages.set', ({ messages }) => {
      try {
        for (const m of messages) {
          const jid = m.key?.remoteJid; if (!jid) continue
          if (isJidBroadcast(jid)) continue
          const ch = getChat(jid)
          const fm = formatMsg(m)
          if (fm && fm.id && !ch.messages.find(x => x.id === fm.id)) ch.messages.push(fm)
        }
        for (const ch of chatStore.values()) { ch.messages.sort((a, b) => a.ts - b.ts); ch.lastMsg = ch.messages[ch.messages.length-1] || null }
        broadcast({ type: 'chats_updated', chats: serializeChats() })
      } catch (e) { console.error('messages.set error', e) }
    })

    sock.ev.on('messages.upsert', ({ messages, type }) => {
      try {
        for (const m of messages) {
          const jid = m.key?.remoteJid; if (!jid) continue
          if (isJidStatusBroadcast(jid) || isJidBroadcast(jid)) {
            // status broadcast
            const fm = formatMsg(m)
            if (fm) broadcast({ type: 'status_update', message: fm, participant: m.key?.participant })
            continue
          }
          const ch = getChat(jid)
          const fm = formatMsg(m)
          if (!fm || !fm.id) continue
          if (fm.isRevoked && m.message?.protocolMessage?.key?.id) {
            const idx = ch.messages.findIndex(x => x.id === m.message.protocolMessage.key.id)
            if (idx >= 0) ch.messages[idx].isRevoked = true
          } else {
            const ex = ch.messages.findIndex(x => x.id === fm.id)
            if (ex >= 0) ch.messages[ex] = fm; else ch.messages.push(fm)
            ch.messages.sort((a, b) => a.ts - b.ts)
            ch.lastMsg = fm
            if (!fm.fromMe && type === 'notify') ch.unreadCount = (ch.unreadCount || 0) + 1
            if (isJidUser(jid) && !ch.name) ch.name = fm.senderName || jid.split('@')[0]
          }
          if (!ch.pic) fetchProfilePic(jid).then(p => { if (p) { ch.pic = p; broadcast({ type: 'chat_pic', jid, pic: p }) } })
          broadcast({ type: 'message', jid, message: fm })
        }
        broadcast({ type: 'chats_updated', chats: serializeChats() })
      } catch (e) { console.error('messages.upsert error', e) }
    })

    sock.ev.on('messages.update', (updates) => {
      try {
        for (const u of updates) {
          const jid = u.key?.remoteJid; if (!jid) continue
          if (isJidBroadcast(jid)) continue
          const ch = getChat(jid)
          const m = ch.messages.find(x => x.id === u.key.id)
          if (m) {
            if (u.update.status !== undefined) m.status = u.update.status
          }
          broadcast({ type: 'message_update', jid, id: u.key.id, update: u.update })
        }
        broadcast({ type: 'chats_updated', chats: serializeChats() })
      } catch (e) { console.error('messages.update error', e) }
    })

    sock.ev.on('contacts.set', ({ contacts }) => {
      try {
        for (const c of contacts) {
          const ch = getChat(c.id)
          if (c.name || c.notify) ch.name = c.name || c.notify
          ch.isContact = !!(c.notify || c.name)
        }
        broadcast({ type: 'chats_updated', chats: serializeChats() })
      } catch (e) { console.error('contacts.set error', e) }
    })

    sock.ev.on('contacts.upsert', (contacts) => {
      try {
        for (const c of contacts) {
          const ch = getChat(c.id)
          if (c.name || c.notify) ch.name = c.name || c.notify
          ch.isContact = !!(c.notify || c.name)
        }
        broadcast({ type: 'chats_updated', chats: serializeChats() })
      } catch (e) { console.error('contacts.upsert error', e) }
    })

    sock.ev.on('presence.update', ({ id, presences }) => {
      try {
        const ch = getChat(id)
        for (const [who, p] of Object.entries(presences || {})) {
          if (who === sock.user?.id) continue
          ch.presence = p.lastKnownPresence || 'unavailable'
          ch.lastSeen = p.lastSeen ? p.lastSeen * 1000 : null
        }
        broadcast({ type: 'presence', jid: id, presence: ch.presence, lastSeen: ch.lastSeen })
      } catch (e) { console.error('presence error', e) }
    })

    sock.ev.on('message-receipt.update', (updates) => {
      try {
        const list = Array.isArray(updates) ? updates : [updates]
        for (const r of list) {
          const key = r.key || r
          const jid = key?.remoteJid; if (!jid) continue
          if (isJidBroadcast(jid)) continue
          const ch = getChat(jid)
          const m = ch.messages.find(x => x.id === key.id)
          if (m) {
            const t = r.receipt?.type || r.type || 'delivery'
            m.status = t === 'read' ? 3 : (m.status >= 2 ? m.status : 2)
          }
          broadcast({ type: 'message_update', jid, id: key.id, update: { status: m?.status } })
        }
      } catch (e) { console.error('receipt error', e) }
    })

  } catch (err) {
    console.error('[fatal start error]', err)
    setState({ status: 'bridge_down', qr: null })
    clearTimeout(restartTimer)
    restartTimer = setTimeout(() => start().catch(console.error), 5000)
  }
}

async function loadAllInitialData() {
  if (!sock) return
  try {
    setState({ initialLoadDone: false })
    // Fetch all contacts & chats via store
    let total = 0
    for (const ch of chatStore.values()) {
      if (!ch.pic) {
        fetchProfilePic(ch.jid).then(p => { if (p) { ch.pic = p; broadcast({ type: 'chat_pic', jid: ch.jid, pic: p }) } })
      }
      total++
    }
    console.log('[init] loaded', total, 'chats, fetching missing profiles...')
    // fetch status for my profile
    try {
      const s = await sock.fetchStatus(sock.user.id)
      if (s?.status) setState({ meStatus: s.status })
    } catch {}
    setState({ initialLoadDone: true })
    broadcast({ type: 'chats_updated', chats: serializeChats(), initialLoadDone: true })
    broadcast({ type: 'init_done' })
  } catch (e) { console.error('initial load error', e) }
}

function serializeChats() {
  const arr = []
  for (const ch of chatStore.values()) {
    if (isJidBroadcast(ch.jid) || isJidStatusBroadcast(ch.jid)) continue
    arr.push({
      jid: ch.jid,
      name: ch.name || ch.jid.split('@')[0],
      pic: ch.pic,
      lastMsg: ch.lastMsg,
      unreadCount: ch.unreadCount,
      pinned: ch.pinned, muted: ch.muted, archived: ch.archived,
      isGroup: ch.isGroup, presence: ch.presence, lastSeen: ch.lastSeen,
      ephemeral: ch.ephemeral, isContact: ch.isContact,
      about: ch.about, msgCount: ch.messages.length,
    })
  }
  arr.sort((a, b) => {
    if (a.pinned && !b.pinned) return -1
    if (!a.pinned && b.pinned) return 1
    return (b.lastMsg?.ts || 0) - (a.lastMsg?.ts || 0)
  })
  return arr
}

wss.on('connection', (ws) => {
  clients.add(ws)
  console.log('[ws] client connected, total:', clients.size)
  try {
    ws.send(JSON.stringify({ type: 'state', ...bridgeState }))
    ws.send(JSON.stringify({ type: 'chats_updated', chats: serializeChats(), initialLoadDone: bridgeState.initialLoadDone }))
  } catch (e) { console.error('ws send initial error', e) }

  ws.on('message', async (raw) => {
    let msg
    try { msg = JSON.parse(raw.toString()) } catch { return }
    const reply = (payload) => { try { ws.readyState === 1 && ws.send(JSON.stringify(payload)) } catch {} }

    try {
      switch (msg.type) {
        case 'send': {
          if (bridgeState.status !== 'open') return reply({ type: 'send_result', ok: false, error: 'غير متصل بعد' })
          const jid = msg.jid
          if (!jid) return reply({ type: 'send_result', ok: false, error: 'لا يوجد jid' })
          const to = jid.includes('@') ? jid : `${String(jid).replace(/\D/g, '')}@s.whatsapp.net`
          const clean = formatWhatsAppText(msg.text || '')
          const sent = await sock.sendMessage(to, { text: clean })
          reply({ type: 'send_result', ok: true, id: sent?.key?.id })
          break
        }
        case 'get_messages': {
          const ch = getChat(msg.jid)
          // Load additional via fetch if needed
          reply({
            type: 'messages', jid: msg.jid,
            messages: ch.messages.slice(-200),
            name: ch.name, pic: ch.pic, isGroup: ch.isGroup,
            total: ch.messages.length,
          })
          break
        }
        case 'load_older': {
          const ch = getChat(msg.jid)
          const before = msg.before || Date.now()
          const limit = msg.limit || 50
          const older = []
          for (const m of ch.messages) {
            if (m.ts < before) { older.push(m); if (older.length >= limit) break }
          }
          // try to fetch from Baileys history if we don't have enough
          if (older.length < limit && sock?.store?.messages) {
            try {
              const extra = await sock.fetchMessagesFromWA(msg.jid, limit, { before: { timestamp: Math.floor(before/1000) } }).catch(() => [])
              if (extra && extra.length) {
                for (const m of extra) {
                  const fm = formatMsg(m)
                  if (fm && fm.id && !ch.messages.find(x => x.id === fm.id)) {
                    ch.messages.push(fm); older.push(fm)
                  }
                }
                ch.messages.sort((a,b)=>a.ts-b.ts)
                ch.lastMsg = ch.messages[ch.messages.length-1] || ch.lastMsg
              }
            } catch(e){}
          }
          reply({ type: 'older_messages', jid: msg.jid, messages: older })
          break
        }
        case 'mark_read': {
          if (bridgeState.status === 'open') {
            const ch = getChat(msg.jid); ch.unreadCount = 0
            if (msg.id) {
              await sock.readMessages([{ remoteJid: msg.jid, id: msg.id, fromMe: false }]).catch(()=>{})
            }
            broadcast({ type: 'chats_updated', chats: serializeChats() })
          }
          break
        }
        case 'typing': {
          if (bridgeState.status === 'open') await sock.sendPresenceUpdate(msg.typing ? 'composing' : 'paused', msg.jid).catch(()=>{})
          break
        }
        case 'set_status': {
          if (bridgeState.status === 'open') {
            try { await sock.updateProfileStatus(String(msg.status || '')); setState({ meStatus: msg.status || '' }); reply({ type: 'cmd_result', ok: true }) }
            catch (e) { reply({ type: 'cmd_result', ok: false, error: String(e.message || e) }) }
          }
          break
        }
        case 'set_name': {
          if (bridgeState.status === 'open') {
            try { await sock.updateProfileName(String(msg.name || '')); setState({ meName: String(msg.name || '') }); reply({ type: 'cmd_result', ok: true }) }
            catch (e) { reply({ type: 'cmd_result', ok: false, error: String(e.message || e) }) }
          }
          break
        }
        case 'logout': { try { if (sock) await sock.logout() } catch {}; break }
        case 'pin': {
          if (bridgeState.status === 'open') {
            await sock.chatModify({ pin: msg.pin ? true : false }, msg.jid).catch(()=>{})
            const ch = getChat(msg.jid); ch.pinned = !!msg.pin
            broadcast({ type: 'chats_updated', chats: serializeChats() })
          }
          break
        }
        case 'mute': {
          if (bridgeState.status === 'open') {
            const dur = msg.mute ? (msg.duration || 8*60*60*1000) : 0
            await sock.chatModify({ mute: dur }, msg.jid).catch(()=>{})
            const ch = getChat(msg.jid); ch.muted = !!msg.mute
            broadcast({ type: 'chats_updated', chats: serializeChats() })
          }
          break
        }
        case 'archive': {
          if (bridgeState.status === 'open') {
            await sock.chatModify({ archive: !!msg.archive }, msg.jid).catch(()=>{})
            const ch = getChat(msg.jid); ch.archived = !!msg.archive
            broadcast({ type: 'chats_updated', chats: serializeChats() })
          }
          break
        }
        case 'delete_chat': {
          if (bridgeState.status === 'open') {
            await sock.chatModify({ delete: true, lastMessages: [] }, msg.jid).catch(()=>{})
            chatStore.delete(msg.jid)
            broadcast({ type: 'chats_updated', chats: serializeChats() })
            reply({ type: 'chat_deleted', jid: msg.jid })
          }
          break
        }
        case 'clear_messages': {
          if (bridgeState.status === 'open') {
            const ch = getChat(msg.jid); ch.messages = []; ch.lastMsg = null
            await sock.chatModify({ clear: { messages: [] } }, msg.jid).catch(()=>{})
            broadcast({ type: 'messages', jid: msg.jid, messages: [], name: ch.name, pic: ch.pic, isGroup: ch.isGroup })
            broadcast({ type: 'chats_updated', chats: serializeChats() })
          }
          break
        }
        case 'delete_msg': {
          if (bridgeState.status === 'open') {
            await sock.sendMessage(msg.jid, { delete: { remoteJid: msg.jid, fromMe: true, id: msg.id } }).catch(()=>{})
          }
          break
        }
        case 'react': {
          if (bridgeState.status === 'open') {
            await sock.sendMessage(msg.jid, { react: { text: msg.emoji || '', key: { id: msg.id, remoteJid: msg.jid, fromMe: !!msg.fromMe } } }).catch(()=>{})
          }
          break
        }
        case 'send_image': {
          if (bridgeState.status === 'open') {
            const mm = String(msg.data || '').match(/^data:(image\/\w+);base64,(.+)$/)
            if (mm) {
              const buf = Buffer.from(mm[2], 'base64')
              await sock.sendMessage(msg.jid, { image: buf, caption: msg.caption || '' })
              reply({ type: 'send_result', ok: true })
            } else reply({ type: 'send_result', ok: false, error: 'صورة غير صالحة' })
          }
          break
        }
        case 'send_audio': {
          if (bridgeState.status === 'open') {
            const mm = String(msg.data || '').match(/^data:(audio\/\w+);base64,(.+)$/)
            if (mm) {
              const buf = Buffer.from(mm[2], 'base64')
              await sock.sendMessage(msg.jid, { audio: buf, mimetype: mm[1] || 'audio/ogg; codecs=opus', ptt: true })
              reply({ type: 'send_result', ok: true })
            } else reply({ type: 'send_result', ok: false, error: 'مقطع صوتي غير صالح' })
          }
          break
        }
        case 'call_action': {
          // المكالمات الصوتية والمرئية غير مدعومة في Baileys حالياً
          reply({ type: 'cmd_result', ok: true, simulated: true, action: msg.act, jid: msg.jid })
          break
        }
        case 'new_group': {
          if (bridgeState.status === 'open') {
            try {
              const parts = msg.participants.map(p => p.includes('@') ? p : `${String(p).replace(/\D/g,'')}@s.whatsapp.net`).filter(x => x.length > 5)
              const res = await sock.groupCreate(String(msg.name||'مجموعة'), parts)
              reply({ type: 'cmd_result', ok: true, jid: res.id })
              broadcast({ type: 'chats_updated', chats: serializeChats() })
            } catch (e) { reply({ type: 'cmd_result', ok: false, error: String(e.message || e) }) }
          }
          break
        }
        case 'group_participants': {
          if (bridgeState.status === 'open' && isJidGroup(msg.jid)) {
            try {
              const meta = await sock.groupMetadata(msg.jid)
              reply({ type: 'group_info', jid: msg.jid, meta: {
                id: meta.id, subject: meta.subject,
                owner: meta.owner, creation: meta.creation,
                desc: meta.desc?.toString() || '', size: meta.size,
                participants: (meta.participants||[]).map(p => ({
                  id: p.id, admin: p.admin || null, name: p.name || p.id.split('@')[0],
                })),
              }})
            } catch (e) { reply({ type: 'group_info', jid: msg.jid, error: String(e.message||e) }) }
          }
          break
        }
        case 'get_profile': {
          const jid = msg.jid || sock.user?.id
          const ch = getChat(jid)
          let about = ch.about || ''
          try { const s = await sock.fetchStatus(jid); about = s?.status || about; ch.about = about } catch {}
          let pic = ch.pic
          if (!pic) { try { pic = await sock.profilePictureUrl(jid, 'image'); ch.pic = pic } catch {} }
          if (isJidUser(jid) && !ch.name) ch.name = jid.split('@')[0]
          reply({
            type: 'profile', jid,
            name: ch.name || jid.split('@')[0],
            pic, about,
            isGroup: ch.isGroup,
            isContact: ch.isContact,
            number: jid.split('@')[0],
            presence: ch.presence, lastSeen: ch.lastSeen,
            ...(jid === sock.user?.id ? { me: true } : {}),
          })
          fetchProfilePic(jid, 'image').then(p => { if (p && p !== pic) broadcast({ type: 'chat_pic', jid, pic: p }); ch.pic = ch.pic || p })
          break
        }
        case 'get_media': {
          // Stub — we'll return existing URL if any
          if (bridgeState.status === 'open') {
            const ch = getChat(msg.jid)
            const m = ch.messages.find(x => x.id === msg.id)
            if (m?.url) return reply({ type: 'media_url', id: msg.id, url: m.url })
            reply({ type: 'media_url', id: msg.id, url: null })
          }
          break
        }
        case 'block': {
          if (bridgeState.status === 'open') {
            try { await sock.updateBlockStatus(msg.jid, msg.block ? 'block' : 'unblock'); reply({ type: 'cmd_result', ok: true }) }
            catch (e) { reply({ type: 'cmd_result', ok: false, error: String(e.message||e) }) }
          }
          break
        }
        case 'search': {
          if (bridgeState.status === 'open') {
            const q = String(msg.query || '').trim().toLowerCase()
            const results = []
            for (const ch of chatStore.values()) {
              if (isJidBroadcast(ch.jid)) continue
              const matches = ch.messages.filter(m => (m.text || '').toLowerCase().includes(q)).slice(-20)
              if (matches.length) results.push({ jid: ch.jid, name: ch.name, pic: ch.pic, messages: matches })
            }
            reply({ type: 'search_results', query: msg.query, results })
          }
          break
        }
        case 'presence_subscribe': {
          if (bridgeState.status === 'open') sock.presenceSubscribe(msg.jid).catch(()=>{})
          break
        }
        case 'get_archived': {
          const arr = serializeChats().filter(c => c.archived)
          reply({ type: 'archived', chats: arr })
          break
        }
      }
    } catch (err) {
      console.error('[ws cmd error]', msg?.type, err)
      reply({ type: 'cmd_result', ok: false, error: String(err?.message || err) })
    }
  })

  ws.on('close', () => { clients.delete(ws); console.log('[ws] client left, total:', clients.size) })
  ws.on('error', () => clients.delete(ws))
})

server.listen(PORT, HOST, () => {
  console.log(`========================================`)
  console.log(`WhatsApp Web يعمل على http://127.0.0.1:${PORT}`)
  console.log(`========================================`)
  start().catch(console.error)
})
