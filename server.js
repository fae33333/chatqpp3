// جسر محلي: يشغّل Baileys (المصافحة + التشفير + فك WABinary)
// ويمرّر الإطارات الخام والمفكوكة إلى صفحة index.html عبر WebSocket محلي.

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
} from '@whiskeysockets/baileys'

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const HOST = '127.0.0.1'
const PORT = 3000
const AUTH_DIR = path.join(__dirname, 'auth_info')
const MAX_HEX_BYTES = 1024 // أقصى عدد bytes يُرسل للمعاينة من كل إطار

// ---------- خادم HTTP + WebSocket المحلي ----------
const server = http.createServer((req, res) => {
  if (req.url === '/' || req.url === '/index.html') {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' })
    fs.createReadStream(path.join(__dirname, 'index.html')).pipe(res)
  } else {
    res.writeHead(404).end()
  }
})

// نقبل الاتصال من صفحتنا فقط، حتى لا يستطيع موقع آخر يعمل في متصفحك التحكم بحسابك
const allowedOrigins = new Set([`http://${HOST}:${PORT}`, `http://localhost:${PORT}`])
const wss = new WebSocketServer({
  server,
  verifyClient: ({ origin }) => allowedOrigins.has(origin),
})

const clients = new Set()
const broadcast = (payload) => {
  const data = JSON.stringify(payload)
  for (const c of clients) if (c.readyState === 1) c.send(data)
}

let bridgeState = { status: 'starting', qr: null, me: null }
const setState = (patch) => {
  bridgeState = { ...bridgeState, ...patch }
  broadcast({ type: 'state', ...bridgeState })
}

// ---------- تحويل الإطارات إلى JSON ----------
const bytesInfo = (input) => {
  const buf = Buffer.from(input)
  return {
    bytes: buf.length,
    hex: buf.subarray(0, MAX_HEX_BYTES).toString('hex'),
    truncated: buf.length > MAX_HEX_BYTES,
  }
}

// BinaryNode = { tag, attrs, content } حيث content: نص | bytes | مصفوفة عقد
const nodeToJSON = (node) => {
  const out = { tag: node.tag, attrs: node.attrs || {} }
  const c = node.content
  if (Array.isArray(c)) out.content = c.map(nodeToJSON)
  else if (c instanceof Uint8Array) out.content = bytesInfo(c)
  else if (c !== undefined && c !== null) out.content = c
  return out
}

// ---------- اتصال واتساب ----------
let sock = null

async function start() {
  const { state, saveCreds } = await useMultiFileAuthState(AUTH_DIR)

  let version
  try {
    ;({ version } = await fetchLatestBaileysVersion())
  } catch {
    /* نستخدم الإصدار الافتراضي في المكتبة */
  }

  sock = makeWASocket({
    auth: state,
    logger: P({ level: 'silent' }),
    ...(version ? { version } : {}),
  })

  sock.ev.on('creds.update', saveCreds)

  // 1) الإطارات الخام كما تصل عبر WebSocket (مشفّرة بـ Noise)
  //    وهي نفس ما رأيته في DevTools (306 B, 55 B, 584 B ...)
  sock.ws.on('message', (data) => {
    try {
      if (typeof data === 'string') return
      const info = bytesInfo(data)
      broadcast({ type: 'raw', ts: Date.now(), len: info.bytes, hex: info.hex, truncated: info.truncated })
    } catch {
      /* تجاهل */
    }
  })

  // 2) الإطارات بعد فك التشفير وفك WABinary
  sock.ws.on('frame', (frame) => {
    try {
      if (frame instanceof Uint8Array) return // مرحلة المصافحة، ليست stanza
      broadcast({ type: 'frame', ts: Date.now(), node: nodeToJSON(frame) })
    } catch {
      /* تجاهل */
    }
  })

  sock.ev.on('connection.update', async ({ connection, lastDisconnect, qr }) => {
    if (qr) setState({ status: 'qr', qr: await QRCode.toDataURL(qr, { margin: 1, width: 280 }) })
    if (connection === 'connecting') setState({ status: 'connecting' })
    if (connection === 'open') setState({ status: 'open', qr: null, me: sock.user?.id ?? null })

    if (connection === 'close') {
      const code = lastDisconnect?.error?.output?.statusCode
      if (code === DisconnectReason.loggedOut) {
        fs.rmSync(AUTH_DIR, { recursive: true, force: true })
        setState({ status: 'logged_out', qr: null, me: null })
      } else {
        setState({ status: 'reconnecting', qr: null })
      }
      setTimeout(() => start().catch(console.error), 1500)
    }
  })

  sock.ev.on('messages.upsert', ({ messages }) => {
    for (const m of messages) {
      const msg = m.message || {}
      const text =
        msg.conversation ||
        msg.extendedTextMessage?.text ||
        msg.imageMessage?.caption ||
        `[${Object.keys(msg)[0] || 'غير معروف'}]`
      broadcast({
        type: 'chat',
        ts: Date.now(),
        from: m.key.remoteJid,
        fromMe: !!m.key.fromMe,
        text,
      })
    }
  })
}

// ---------- أوامر الصفحة ----------
wss.on('connection', (ws) => {
  clients.add(ws)
  ws.send(JSON.stringify({ type: 'state', ...bridgeState }))

  ws.on('message', async (raw) => {
    let msg
    try {
      msg = JSON.parse(raw.toString())
    } catch {
      return
    }
    if (msg.type !== 'send') return

    const reply = (payload) => ws.readyState === 1 && ws.send(JSON.stringify(payload))
    if (bridgeState.status !== 'open') return reply({ type: 'send_result', ok: false, error: 'غير متصل بواتساب بعد' })

    const to = String(msg.to || '').trim()
    const jid = to.includes('@') ? to : `${to.replace(/\D/g, '')}@s.whatsapp.net`
    if (jid.startsWith('@')) return reply({ type: 'send_result', ok: false, error: 'أدخل رقماً صحيحاً' })

    try {
      await sock.sendMessage(jid, { text: String(msg.text || '') })
      reply({ type: 'send_result', ok: true })
    } catch (err) {
      reply({ type: 'send_result', ok: false, error: String(err?.message || err) })
    }
  })

  ws.on('close', () => clients.delete(ws))
})

server.listen(PORT, HOST, () => {
  console.log(`افتح الصفحة: http://${HOST}:${PORT}`)
  start().catch(console.error)
})
