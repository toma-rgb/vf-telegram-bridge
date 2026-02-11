// path: index.local.js
import dotenv from 'dotenv';
dotenv.config({ path: process.env.DOTENV_PATH || '.env', override: true });

import axios from 'axios';
import http from 'http';
import https from 'https';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { Telegraf } from 'telegraf';
import { randomUUID } from 'crypto';
import OpenAI from 'openai';
import FormData from 'form-data';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// =====================
// ENV
// =====================
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const VF_API_KEY = process.env.VF_API_KEY;
const VF_PROJECT_ID = process.env.VF_PROJECT_ID;
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;

const VF_VERSION_ID = process.env.VF_VERSION_ID || '';
const VF_USE_VERSION_HEADER = /^true$/i.test(process.env.VF_USE_VERSION_HEADER || '');

const VF_COMPLETION_EVENTS = /^true$/i.test(process.env.VF_COMPLETION_EVENTS || 'false');
const VF_COMPLETION_TO_TELEGRAM = /^true$/i.test(process.env.VF_COMPLETION_TO_TELEGRAM || 'false');

const MEDIA_FORCE_UPLOAD = process.env.MEDIA_FORCE_UPLOAD ? /^true$/i.test(process.env.MEDIA_FORCE_UPLOAD) : true;
const DEBUG_MEDIA = /^true$/i.test(process.env.DEBUG_MEDIA || '');
const DEBUG_BUTTONS = /^true$/i.test(process.env.DEBUG_BUTTONS || '');
const DEBUG_STREAM = /^true$/i.test(process.env.DEBUG_STREAM || '');

// Session/auto-reset config
const SESSION_RESET_HOURS = parseFloat(process.env.SESSION_RESET_HOURS || '24');
const SESSION_RESET_ON_DAY_CHANGE = /^true$/i.test(process.env.SESSION_RESET_ON_DAY_CHANGE || 'true');
const LOCAL_UTC_OFFSET_HOURS = parseFloat(process.env.LOCAL_UTC_OFFSET_HOURS || '0');

// Media cache config
const MEDIA_CACHE_PATH = process.env.MEDIA_CACHE_PATH || path.join(__dirname, 'media-cache.json');
const MEDIA_CACHE_MAX_ENTRIES = parseInt(process.env.MEDIA_CACHE_MAX_ENTRIES || '1000', 10);
const CALENDLY_MINI_APP_URL = process.env.CALENDLY_MINI_APP_URL || ''; // Link to your hosted calendly.html
const MARKETPLACE_MINI_APP_URL = process.env.MARKETPLACE_MINI_APP_URL || ''; // Link to your hosted marketplace.html
const RESERVATIONS_MINI_APP_URL = process.env.RESERVATIONS_MINI_APP_URL || ''; // Link to your hosted reservations.html
const DEBUG_STT = /^true$/i.test(process.env.DEBUG_STT || '');


if (!TELEGRAM_BOT_TOKEN || !VF_API_KEY || !VF_PROJECT_ID) {
  console.error('❌ Missing env. Need TELEGRAM_BOT_TOKEN, VF_API_KEY, VF_PROJECT_ID');
  process.exit(1);
}

// Increased handler timeout (helps on slow VF turns and media uploads)
const bot = new Telegraf(TELEGRAM_BOT_TOKEN, { handlerTimeout: 120_000 });
const openai = OPENAI_API_KEY ? new OpenAI({ apiKey: OPENAI_API_KEY }) : null;
console.log(`[stt] ${openai ? '✅ OpenAI STT initialized' : '⚠️ OpenAI STT NOT initialized (check OPENAI_API_KEY env)'}`);
console.log(`[system] Bot starting (PID: ${process.pid}) at ${new Date().toISOString()}`);
console.log(
  (VF_USE_VERSION_HEADER
    ? `🔒 VF pinned versionID=${VF_VERSION_ID || '(empty)'}`
    : '🚀 VF Published (no version header)') +
  ` | Streaming: ON | completion_events=${VF_COMPLETION_EVENTS} | completion_to_telegram=${VF_COMPLETION_TO_TELEGRAM} | Media force upload: ${MEDIA_FORCE_UPLOAD}`
);
console.log(`[system] CALENDLY_MINI_APP_URL: ${CALENDLY_MINI_APP_URL ? '✅ SET' : '⚠️ MISSING'}`);
console.log(`[system] MARKETPLACE_MINI_APP_URL: ${MARKETPLACE_MINI_APP_URL ? '✅ SET' : '⚠️ MISSING'}`);
console.log(`[system] RESERVATIONS_MINI_APP_URL: ${RESERVATIONS_MINI_APP_URL ? '✅ SET' : '⚠️ MISSING'}`);
console.log(`[system] CALENDLY_MINI_APP_URL: ${CALENDLY_MINI_APP_URL ? '✅ SET' : '⚠️ MISSING'}`);
console.log(`[system] MARKETPLACE_MINI_APP_URL: ${MARKETPLACE_MINI_APP_URL ? '✅ SET' : '⚠️ MISSING'}`);
console.log(`[system] RESERVATIONS_MINI_APP_URL: ${RESERVATIONS_MINI_APP_URL ? '✅ SET' : '⚠️ MISSING'}`);
console.log('🚀 BRIDGE VERSION: CALENDLY FIX + STREAMING SUPPORT (Commit 12b)');

// =====================
// HTTP (keep-alive)
// =====================
const httpAgent = new http.Agent({ keepAlive: true, keepAliveMsecs: 10_000, maxSockets: 50 });
const httpsAgent = new https.Agent({ keepAlive: true, keepAliveMsecs: 10_000, maxSockets: 50 });
const api = axios.create({
  timeout: 110_000, // Slightly less than bot timeout
  httpAgent,
  httpsAgent,
  validateStatus: (s) => s >= 200 && s < 300,
});

// =====================
// callback_data stash (≤64B)
// =====================
const CALLBACK_PREFIX = 'CB:';
const REQUEST_PREFIX = 'RQ:';
const CALLBACK_TTL_MS = 15 * 60 * 1000;
const stash = new Map();

function stashPut(userId, payload) {
  const key = `${CALLBACK_PREFIX}${randomUUID().slice(0, 12)}`;
  stash.set(key, { userId, payload, ts: Date.now() });
  return key;
}

function stashTake(key, userId) {
  const rec = stash.get(key);
  if (!rec || rec.userId !== userId) return null;
  stash.delete(key);
  return rec.payload;
}

setInterval(() => {
  const now = Date.now();
  for (const [k, v] of stash.entries()) if (now - v.ts > CALLBACK_TTL_MS) stash.delete(k);
}, 5 * 60 * 1000).unref();

// =====================
// Voiceflow helpers (STREAMING)
// =====================
const userStateBase = (userId) => `https://general-runtime.voiceflow.com/state/user/telegram_${userId}`;

function streamUrl(userId) {
  const qp = new URLSearchParams();
  if (VF_COMPLETION_EVENTS) qp.set('completion_events', 'true');
  const qs = qp.toString();
  const base = `https://general-runtime.voiceflow.com/v2/project/${VF_PROJECT_ID}/user/telegram_${userId}/interact/stream`;
  return qs ? `${base}?${qs}` : base;
}

function vfHeaders({ stream = false } = {}) {
  const headers = { Authorization: VF_API_KEY, 'Content-Type': 'application/json' };
  if (stream) headers.Accept = 'text/event-stream';
  if (VF_USE_VERSION_HEADER && VF_VERSION_ID) headers.versionID = VF_VERSION_ID;
  return headers;
}

async function resetVoiceflow(userId) {
  try {
    await api.delete(userStateBase(userId), { headers: { Authorization: VF_API_KEY } });
  } catch { }
}

function parseSseStream(readable, onEvent) {
  let buf = '';
  let curEvent = '';
  let curId = '';
  let dataLines = [];

  const dispatch = () => {
    if (!curEvent && !curId && dataLines.length === 0) return;
    const raw = dataLines.join('\n');
    const ev = curEvent || 'message';
    const id = curId || undefined;

    curEvent = '';
    curId = '';
    dataLines = [];

    if (!raw) return;
    let data = raw;
    try {
      data = JSON.parse(raw);
    } catch { }
    onEvent({ event: ev, id, data });
  };

  readable.on('data', (chunk) => {
    buf += chunk.toString('utf8');

    const parts = buf.split(/\r?\n/);
    buf = parts.pop() || '';

    for (const line of parts) {
      if (!line) {
        dispatch();
        continue;
      }
      if (line.startsWith(':')) continue; // comment/keepalive
      if (line.startsWith('event:')) {
        curEvent = line.slice('event:'.length).trim();
        continue;
      }
      if (line.startsWith('id:')) {
        curId = line.slice('id:'.length).trim();
        continue;
      }
      if (line.startsWith('data:')) {
        dataLines.push(line.slice('data:'.length).trimStart());
        continue;
      }
    }
  });

  readable.on('end', () => {
    dispatch();
    onEvent({ event: 'end-of-stream', data: null });
  });
}

// =====================
// Formatting
// =====================
function esc(s) {
  return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

function normalizeSpacing(text) {
  if (!text) return '';
  return text.replace(/[ \t]+$/gm, '').replace(/\n{3,}/g, '\n\n').trim();
}

function hasCompleteSentence(text) {
  return /[.!?]\s*$/.test(text.trim());
}

function isHttpUrl(s) {
  if (typeof s !== 'string') return false;
  const v = s.trim();
  return /^https?:\/\/\S+$/i.test(v);
}

function stripTags(s) {
  return String(s || '').replace(/<[^>]*>/g, '');
}

/**
 * Splits text into segments of { type: 'text'|'image', value: string }.
 */
function segmentContent(text) {
  if (!text) return [];

  const segments = [];
  const mdImageRe = /!\[([\s\S]*?)\]\s*\(\s*(https?:\/\/[^\s)]+)\s*\)/gi;
  const photoLabelRe = /Photo:\s*(https?:\/\/[^\s]+)/gi;

  const matches = [];
  let match;

  // Find all Markdown images
  while ((match = mdImageRe.exec(text)) !== null) {
    matches.push({ index: match.index, length: match[0].length, url: match[2].trim(), type: 'image' });
  }

  // Find all "Photo: URL" labels
  while ((match = photoLabelRe.exec(text)) !== null) {
    matches.push({ index: match.index, length: match[0].length, url: match[1].trim(), type: 'image' });
  }

  // Sort by appearance
  matches.sort((a, b) => a.index - b.index);

  let lastIndex = 0;
  for (const m of matches) {
    if (m.index > lastIndex) {
      const txt = text.substring(lastIndex, m.index).trim();
      if (txt) segments.push({ type: 'text', value: txt });
    }
    segments.push({ type: 'image', value: m.url });
    lastIndex = m.index + m.length;
  }

  if (lastIndex < text.length) {
    const txt = text.substring(lastIndex).trim();
    if (txt) segments.push({ type: 'text', value: txt });
  }

  return segments;
}

function unescapeVfHtmlArtifacts(raw) {
  return String(raw || '').replace(/\\"/g, '"').replace(/\\n/g, '\n');
}

function htmlAnchorsToMarkdown(raw) {
  const s = String(raw || '');
  return s.replace(/<a\s+[^>]*href\s*=\s*["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi, (_, url, text) => {
    const label = stripTags(text).trim() || url;
    return `[${label}](${url})`;
  });
}

function compactLabelUrlLines(raw) {
  const lines = String(raw || '').split(/\r?\n/);
  const out = [];
  for (let i = 0; i < lines.length; i += 1) {
    const cur = lines[i] ?? '';
    const next = lines[i + 1] ?? '';
    const curTrim = cur.trim();
    const nextTrim = next.trim();

    const curLooksLikeMarkdownLink = /\[[^\]]+?\]\((https?:\/\/[^\s)]+)\)/i.test(curTrim);
    if (curTrim && !curLooksLikeMarkdownLink && curTrim.length <= 120 && isHttpUrl(nextTrim)) {
      out.push(`[${curTrim}](${nextTrim})`);
      i += 1;
      continue;
    }
    out.push(cur);
  }
  return out.join('\n');
}

function linkifyBareUrlsToMarkdown(raw) {
  const s = String(raw || '');
  return s.replace(/(^|[\s>])((https?:\/\/)[^\s)<]+)(?=$|[\s<])/gi, (m, pre, url) => `${pre}[${url}](${url})`);
}

function extractCalendlyUrl(text) {
  if (!text) return null;
  // Look for calendly.com/... - handle www., and exclude trailing parentheses, quotes, or whitespace
  const re = /https?:\/\/(www\.)?calendly\.com\/[^\s"'>\)]+/i;
  const match = text.match(re);
  return match ? match[0].trim() : null;
}

// Markdown → Telegram HTML (bold/italic + hyperlinks ONLY)
function mdToHtml(input) {
  if (!input) return '';

  let raw = unescapeVfHtmlArtifacts(input);
  raw = normalizeSpacing(raw);
  raw = htmlAnchorsToMarkdown(raw);
  raw = compactLabelUrlLines(raw);
  raw = linkifyBareUrlsToMarkdown(raw);

  // STRIP IMAGES (full or partial) - prevent them from appearing in text bubbles
  // 1. Full/Partial Markdown images: ![...] ( http... )
  raw = raw.replace(/!\[[^\]]*?\]\s*\(\s*https?:\/\/[^\s)]*\)?/gi, '');
  // 2. Full/Partial Photo labels (start with space/newline/word boundary)
  raw = raw.replace(/(^|[\s\n])Photo:\s*https?:\/\/[^\s]*/gi, '$1');

  let s = esc(raw);

  // [text](url) but NOT images ![alt](url)
  s = s.replace(/(?<!!)\[([^\]]+?)\]\((https?:\/\/[^\s)]+)\)/g, (_, text, url) => {
    return `<a href="${esc(url)}">${esc(text)}</a>`;
  });

  s = s.replace(/\*\*(.+?)\*\*/g, '<b>$1</b>');
  s = s.replace(/(^|[^*])\*(?!\s)(.+?)\*(?!\*)/g, (_, pre, body) => `${pre}<i>${body}</i>`);
  return s;
}

function slateToText(slate) {
  try {
    return (
      slate?.content
        ?.map((b) => (b.children || []).map((c) => c.text).filter(Boolean).join(''))
        .filter(Boolean)
        .join('\n') || ''
    );
  } catch {
    return '';
  }
}

function textOfTrace(t) {
  // Prefer payload.message for AI + non-AI text
  return t?.payload?.message ?? slateToText(t?.payload?.slate) ?? '';
}

// =====================
// Telegram safe send/edit helpers (NO duplicate messages)
// =====================
async function safeReplyHtml(ctx, html, extra = {}) {
  try {
    return await ctx.reply(html, { parse_mode: 'HTML', disable_web_page_preview: true, ...extra });
  } catch (e) {
    try {
      return await ctx.reply(stripTags(html), { disable_web_page_preview: true, ...extra });
    } catch {
      return null;
    }
  }
}

async function safeEditHtml(ctx, chatId, messageId, html, extra = {}) {
  try {
    await ctx.telegram.editMessageText(chatId, messageId, undefined, html, {
      parse_mode: 'HTML',
      disable_web_page_preview: true,
      ...extra,
    });
    return true;
  } catch (e) {
    const m = String(e?.message || '');

    if (/message is not modified|MESSAGE_NOT_MODIFIED/i.test(m)) return true;

    // If HTML parsing fails, fallback to plain text EDIT (still same message)
    if (/can't parse entities|Bad Request: can't parse entities/i.test(m)) {
      try {
        await ctx.telegram.editMessageText(chatId, messageId, undefined, stripTags(html), {
          disable_web_page_preview: true,
          ...extra,
        });
        return true;
      } catch { }
    }

    if (DEBUG_STREAM) console.log('[tg-edit] failed:', m);
    return false;
  }
}

// =====================
// Track last bot message (keyboard type)
// keyboard: 'none' | 'choice' | 'card'
// =====================
const lastBotMsgByUser = new Map(); // userId -> { chatId, message_id, keyboard }

// =====================
// Completion streaming (single edited message)
// =====================
const completionStateByUser = new Map(); // userId -> state

function defaultCompletionState() {
  return {
    msg: null,
    lastHtml: '',
    lastEditAt: 0,
    timer: null,
    pendingHtml: '',
    accumulated: '',
    active: false,
    hasContent: false,
    endedAt: 0,
    sentImages: new Set(), // Track images sent during this streaming session
    finalizedIdx: -1,     // Track segments (text or image) already permanently handled
  };
}

// Works whether VF sends "delta chunks" OR "full cumulative text"
function mergeCompletion(prev, incoming) {
  const p = String(prev || '');
  const n = String(incoming || '');
  if (!n) return p;
  if (!p) return n;

  // If new is cumulative, just take it
  if (n.startsWith(p)) return n;

  // If new is shorter duplicate, keep prev
  if (p.startsWith(n)) return p;

  // Otherwise treat as delta and append
  return p + n;
}

async function completionSendOrUpdate(ctx, userId, fullText, { force = false } = {}) {
  const s = completionStateByUser.get(userId) || defaultCompletionState();
  s.accumulated = String(fullText || '');

  const segments = segmentContent(s.accumulated);

  for (let i = 0; i < segments.length; i++) {
    if (i <= s.finalizedIdx) continue;

    const seg = segments[i];
    const isLast = i === segments.length - 1;

    if (seg.type === 'image') {
      if (!s.sentImages.has(seg.value)) {
        // If we have an active text bubble, finalize it BEFORE sending the photo
        if (s.msg) {
          await safeEditHtml(ctx, s.msg.chat.id, s.msg.message_id, s.lastHtml);
          s.msg = null;
          s.lastHtml = '';
        }

        s.sentImages.add(seg.value);
        s.finalizedIdx = i; // Mark image as finalized too
        completionStateByUser.set(userId, s);
        try {
          await ctx.replyWithPhoto(seg.value);
        } catch (err) { }
      } else {
        // Even if we already sent the image, mark the segment as finalized if we are past it
        s.finalizedIdx = i;
        completionStateByUser.set(userId, s);
      }
      continue;
    }

    if (seg.type === 'text') {
      const html = mdToHtml(seg.value.trim());
      if (!html) continue;

      const now = Date.now();
      const MIN_EDIT_MS = 150;

      // If NOT the last segment, it's followed by an image. Send it immediately.
      if (!isLast) {
        if (!s.msg) {
          const msg = await safeReplyHtml(ctx, html);
          if (msg) {
            lastBotMsgByUser.set(userId, { chatId: msg.chat.id, message_id: msg.message_id, keyboard: 'none' });
          }
        } else {
          await safeEditHtml(ctx, s.msg.chat.id, s.msg.message_id, html);
        }

        s.msg = null;
        s.lastHtml = '';
        s.finalizedIdx = i; // MARK AS PERMANENT
        completionStateByUser.set(userId, s);
        continue;
      }

      // HANDLE LAST SEGMENT (streaming)
      if (!s.msg) {
        if (html.length < 5) {
          completionStateByUser.set(userId, s);
          return;
        }
        const plainText = seg.value.trim();
        if (!hasCompleteSentence(plainText) && plainText.length < 150) {
          completionStateByUser.set(userId, s);
          return;
        }

        const msg = await safeReplyHtml(ctx, html);
        if (msg) {
          s.msg = msg;
          lastBotMsgByUser.set(userId, { chatId: msg.chat.id, message_id: msg.message_id, keyboard: 'none' });
        }
        s.lastHtml = html;
        s.lastEditAt = now;
        completionStateByUser.set(userId, s);
        return;
      }

      if (!force && html === s.lastHtml) return;

      const doEdit = async (nextHtml) => {
        const ok = await safeEditHtml(ctx, s.msg.chat.id, s.msg.message_id, nextHtml);
        if (ok) {
          s.lastHtml = nextHtml;
          s.lastEditAt = Date.now();
          completionStateByUser.set(userId, s);
        }
      };

      if (force || now - s.lastEditAt >= MIN_EDIT_MS) {
        if (s.timer) { clearTimeout(s.timer); s.timer = null; s.pendingHtml = ''; }
        await doEdit(html);
        return;
      }

      s.pendingHtml = html;
      completionStateByUser.set(userId, s);

      if (!s.timer) {
        s.timer = setTimeout(async () => {
          const st = completionStateByUser.get(userId);
          if (!st?.pendingHtml) { if (st) st.timer = null; return; }
          const pending = st.pendingHtml;
          st.pendingHtml = '';
          st.timer = null;
          completionStateByUser.set(userId, st);
          await doEdit(pending);
        }, 50).unref();
      }
    }
  }
}

async function completionFlush(ctx, userId) {
  const s = completionStateByUser.get(userId);
  if (!s?.msg) return null;

  if (s.timer) {
    clearTimeout(s.timer);
    s.timer = null;
  }

  if (s.pendingHtml) {
    const pending = s.pendingHtml;
    s.pendingHtml = '';
    completionStateByUser.set(userId, s);
    await safeEditHtml(ctx, s.msg.chat.id, s.msg.message_id, pending);
    s.lastHtml = pending;
    s.lastEditAt = Date.now();
    completionStateByUser.set(userId, s);
  }

  return { chatId: s.msg.chat.id, message_id: s.msg.message_id, keyboard: lastBotMsgByUser.get(userId)?.keyboard || 'none' };
}

// Real-time handler: ONLY completion traces (avoid duplicate text messages)
async function handleTraceRealtime(ctx, trace, { skipRendering = false } = {}) {
  if (!trace || trace.type !== 'completion') return;
  if (!VF_COMPLETION_TO_TELEGRAM) return;

  const userId = ctx.from.id;
  const state = trace.payload?.state;

  const s = completionStateByUser.get(userId) || defaultCompletionState();

  if (state === 'start') {
    // New streaming message for this completion
    if (s.timer) {
      clearTimeout(s.timer);
      s.timer = null;
    }
    s.msg = null;
    s.lastHtml = '';
    s.lastEditAt = 0;
    s.pendingHtml = '';
    s.accumulated = '';
    s.active = true;
    s.hasContent = false;
    s.endedAt = 0;
    s.sentImages = new Set();
    s.finalizedIdx = -1;
    completionStateByUser.set(userId, s);

    if (DEBUG_STREAM) console.log('[completion] start');
    return;
  }

  if (state === 'content') {
    const incoming = String(trace.payload?.content || '');
    s.active = true;

    s.accumulated = mergeCompletion(s.accumulated, incoming);
    if (s.accumulated.trim()) s.hasContent = true;

    completionStateByUser.set(userId, s);

    if (skipRendering) return;
    await completionSendOrUpdate(ctx, userId, s.accumulated);
    return;
  }

  if (state === 'end') {
    s.active = false;
    s.endedAt = Date.now();
    completionStateByUser.set(userId, s);

    if (DEBUG_STREAM) console.log('[completion] end');
    if (s.accumulated.trim()) await completionSendOrUpdate(ctx, userId, s.accumulated, { force: true });
    return;
  }
}

// =====================
// Media helpers (+ file_id cache)
// =====================
const IMG_EXT = /\.(png|jpg|jpeg|webp|bmp|heic|heif)(\?|#|$)/i;
const GIF_EXT = /\.(gif|webm|mp4)(\?|#|$)/i;

function isImageLike(url) {
  return typeof url === 'string' && IMG_EXT.test(url);
}
function isGifLike(url) {
  return typeof url === 'string' && GIF_EXT.test(url);
}
function looksLikeMedia(url) {
  return typeof url === 'string' && (isImageLike(url) || isGifLike(url));
}

function normalizeDirectUrl(url) {
  if (!url) return url;
  const g = url.match(/https?:\/\/drive\.google\.com\/file\/d\/([^/]+)\/view/i);
  if (g) return `https://drive.google.com/uc?export=download&id=${g[1]}`;
  if (/https?:\/\/www\.dropbox\.com\/s\//i.test(url))
    return url.replace('www.dropbox.com', 'dl.dropboxusercontent.com').replace('?dl=0', '');
  if (/https?:\/\/i\.imgur\.com\/.+\.gifv$/i.test(url)) return url.replace(/\.gifv$/i, '.mp4');
  return url;
}

// --- file_id cache ---
let mediaCache = new Map();
/** key: normalized url → { kind: 'photo'|'document'|'animation', fileId: string } */
function loadMediaCache() {
  try {
    if (fs.existsSync(MEDIA_CACHE_PATH)) {
      const raw = JSON.parse(fs.readFileSync(MEDIA_CACHE_PATH, 'utf8'));
      mediaCache = new Map(Object.entries(raw));
      if (DEBUG_MEDIA) console.log(`[media-cache] loaded ${mediaCache.size} entries`);
    }
  } catch (e) {
    console.warn('[media-cache] load failed:', e?.message);
  }
}
let saveTimer = null;
function saveMediaCacheSoon() {
  clearTimeout(saveTimer);
  saveTimer = setTimeout(() => {
    try {
      const obj = Object.fromEntries(mediaCache.entries());
      fs.writeFileSync(MEDIA_CACHE_PATH, JSON.stringify(obj));
      if (DEBUG_MEDIA) console.log(`[media-cache] saved (${mediaCache.size}) → ${MEDIA_CACHE_PATH}`);
    } catch (e) {
      console.warn('[media-cache] save failed:', e?.message);
    }
  }, 400).unref();
}
function cacheKeyFor(url) {
  return (normalizeDirectUrl(url) || '').trim().toLowerCase();
}
function rememberFileId(url, kind, fileId) {
  const key = cacheKeyFor(url);
  if (!key || !fileId) return;
  if (mediaCache.size >= MEDIA_CACHE_MAX_ENTRIES && !mediaCache.has(key)) {
    const oldest = mediaCache.keys().next().value;
    if (oldest) mediaCache.delete(oldest);
  }
  mediaCache.set(key, { kind, fileId });
  saveMediaCacheSoon();
}

loadMediaCache();

async function downloadBuffer(url) {
  const direct = normalizeDirectUrl(url);
  const res = await api.get(direct, {
    responseType: 'arraybuffer',
    maxContentLength: 50 * 1024 * 1024,
    headers: { 'User-Agent': 'Mozilla/5.0 (TelegramBot)' },
  });
  const buf = Buffer.from(res.data);
  const ct = (res.headers['content-type'] || '').toLowerCase();
  let ext = '';
  if (ct.includes('gif')) ext = '.gif';
  else if (ct.includes('webm')) ext = '.webm';
  else if (ct.includes('mp4')) ext = '.mp4';
  else if (ct.includes('png')) ext = '.png';
  else if (ct.includes('jpeg') || ct.includes('jpg')) ext = '.jpg';
  else if (ct.includes('webp')) ext = '.webp';
  else ext = isGifLike(direct) ? '.gif' : isImageLike(direct) ? '.jpg' : '';
  return { buffer: buf, filename: `media${ext}` };
}

function extractAndCacheFileId(url, kind, msg) {
  try {
    if (kind === 'animation' && msg?.animation?.file_id) {
      rememberFileId(url, 'animation', msg.animation.file_id);
    } else if (kind === 'document' && msg?.document?.file_id) {
      rememberFileId(url, 'document', msg.document.file_id);
    } else if (kind === 'photo' && Array.isArray(msg?.photo) && msg.photo.length) {
      const best = msg.photo[msg.photo.length - 1];
      rememberFileId(url, 'photo', best.file_id);
    }
  } catch { }
}

async function sendMediaWithCaption(ctx, url, captionHtml, replyMarkup) {
  const direct = normalizeDirectUrl(url);
  const key = cacheKeyFor(direct);
  if (DEBUG_MEDIA) console.log('[media] send', { direct, caption: !!captionHtml, hasMarkup: !!replyMarkup });

  const opts = {
    caption: captionHtml || undefined,
    parse_mode: captionHtml ? 'HTML' : undefined,
    reply_markup: replyMarkup || undefined,
  };

  // 1) cached file_id
  const cached = mediaCache.get(key);
  if (cached?.fileId) {
    try {
      if (cached.kind === 'animation') return await ctx.replyWithAnimation(cached.fileId, opts);
      if (cached.kind === 'document') return await ctx.replyWithDocument(cached.fileId, opts);
      if (cached.kind === 'photo') return await ctx.replyWithPhoto(cached.fileId, opts);
    } catch (e) {
      if (DEBUG_MEDIA) console.log('[media] cached file_id failed, refreshing:', e?.message);
      mediaCache.delete(key);
      saveMediaCacheSoon();
    }
  }

  // 2) direct URL
  if (!MEDIA_FORCE_UPLOAD) {
    if (isGifLike(direct)) {
      try {
        const m = await ctx.replyWithAnimation(direct, opts);
        extractAndCacheFileId(direct, 'animation', m);
        return m;
      } catch { }
      try {
        const m = await ctx.replyWithDocument(direct, opts);
        extractAndCacheFileId(direct, 'document', m);
        return m;
      } catch { }
      try {
        const m = await ctx.replyWithPhoto(direct, opts);
        extractAndCacheFileId(direct, 'photo', m);
        return m;
      } catch { }
    } else {
      try {
        const m = await ctx.replyWithPhoto(direct, opts);
        extractAndCacheFileId(direct, 'photo', m);
        return m;
      } catch { }
      try {
        const m = await ctx.replyWithDocument(direct, opts);
        extractAndCacheFileId(direct, 'document', m);
        return m;
      } catch { }
    }
  }

  // 3) upload buffer
  try {
    const { buffer, filename } = await downloadBuffer(direct);
    const input = { source: buffer, filename };

    if (isGifLike(direct)) {
      try {
        const m = await ctx.replyWithAnimation(input, opts);
        extractAndCacheFileId(direct, 'animation', m);
        return m;
      } catch { }
      try {
        const m = await ctx.replyWithDocument(input, opts);
        extractAndCacheFileId(direct, 'document', m);
        return m;
      } catch { }
      try {
        const m = await ctx.replyWithPhoto(input, opts);
        extractAndCacheFileId(direct, 'photo', m);
        return m;
      } catch { }
    } else {
      try {
        const m = await ctx.replyWithPhoto(input, opts);
        extractAndCacheFileId(direct, 'photo', m);
        return m;
      } catch { }
      try {
        const m = await ctx.replyWithDocument(input, opts);
        extractAndCacheFileId(direct, 'document', m);
        return m;
      } catch { }
    }
  } catch (e) {
    if (DEBUG_MEDIA) console.log('[media] upload failed, last resort = URL', e?.message);
  }

  return await ctx.reply(direct);
}

// =====================
// Gallery extraction (head + items + tail)
// =====================
function parseGalleryBlocks(raw) {
  const lines = raw.split(/\r?\n/);
  const n = lines.length;
  const consumed = new Array(n).fill(false);
  const items = [];

  const imgRe = /^\s*\!\[[^\]]*?\]\((https?:\/\/[^\s)]+)\)\s*$/i;
  const viewRe = /^\s*\[ *View +Menu *\]\((https?:\/\/[^\s)]+)\)\s*$/i;

  let firstIdx = -1;
  let lastIdx = -1;

  for (let i = 0; i < n; i += 1) {
    const imgMatch = lines[i].match(imgRe);
    if (!imgMatch) continue;

    let j = i - 1;
    while (j >= 0 && (!lines[j].trim() || consumed[j])) j -= 1;
    const title = (j >= 0 ? lines[j].trim() : '').trim();

    let k = i + 1;
    while (k < n && !lines[k].trim()) k += 1;
    const viewMatch = lines[k]?.match(viewRe);
    const menuUrl = viewMatch ? viewMatch[1] : '';

    const imageUrl = imgMatch[1];
    if (imageUrl) {
      if (j >= 0) consumed[j] = true;
      consumed[i] = true;
      if (viewMatch) consumed[k] = true;
      items.push({ title, imageUrl, menuUrl });
      if (firstIdx === -1) firstIdx = j >= 0 ? j : i;
      lastIdx = viewMatch ? k : i;
    }
  }

  let head = '';
  let tail = '';
  if (firstIdx === -1) {
    head = normalizeSpacing(lines.join('\n'));
    tail = '';
  } else {
    head = normalizeSpacing(lines.slice(0, firstIdx).filter((_, idx) => !consumed[idx]).join('\n'));
    tail = normalizeSpacing(
      lines
        .slice(lastIdx + 1)
        .filter((_, idx) => !consumed[lastIdx + 1 + idx])
        .join('\n')
    );
  }

  return { head, items, tail };
}

// =====================
// Buttons
// =====================
function btnLabel(b) {
  return String(
    b?.name ??
    b?.request?.payload?.label ??
    b?.request?.payload?.query ??
    b?.request?.payload?.text ??
    b?.request?.payload ??
    'Option'
  ).slice(0, 64);
}

function pickSemanticPayload(p) {
  if (!p) return '';
  if (typeof p === 'string') return p;
  if (typeof p === 'number' || typeof p === 'boolean') return String(p);
  if (typeof p === 'object') {
    if (typeof p.intent === 'string' && p.intent.trim()) return p.intent;
    if (typeof p.query === 'string' && p.query.trim()) return p.query;
    if (typeof p.text === 'string' && p.text.trim()) return p.text;
    if (typeof p.url === 'string' && p.url.trim()) return p.url;
    if (typeof p.href === 'string' && p.href.trim()) return p.href;
    if (typeof p.link === 'string' && p.link.trim()) return p.link;
  }
  return '';
}

function btnPayload(b) {
  const p = b?.request?.payload;
  const semantic = pickSemanticPayload(p);
  if (semantic) return semantic;
  if (b?.request?.type?.toLowerCase?.() === 'path' && b?.name) return String(b.name);
  return String(b?.name ?? '');
}

function extractUrlFromActions(actions) {
  if (!Array.isArray(actions)) return '';
  for (const a of actions) {
    const type = String(a?.type ?? '').toLowerCase();
    if (!type) continue;

    if (type === 'open_url' || type === 'open-url' || type === 'url' || type === 'web_url' || type === 'web-url') {
      const u = a?.payload?.url ?? a?.payload?.href ?? a?.payload?.link;
      if (typeof u === 'string' && isHttpUrl(u)) return u.trim();
    }

    if (typeof a?.url === 'string' && isHttpUrl(a.url)) return a.url.trim();
  }
  return '';
}

function extractUrlFromRequest(req) {
  if (!req || typeof req !== 'object') return '';

  const direct = req.url ?? req.href ?? req.link ?? req.payload?.url ?? req.payload?.href ?? req.payload?.link;
  if (typeof direct === 'string' && isHttpUrl(direct)) return direct.trim();

  const actionsUrl = extractUrlFromActions(req.payload?.actions);
  if (actionsUrl) return actionsUrl;

  const payloadSemantic = pickSemanticPayload(req.payload);
  if (isHttpUrl(payloadSemantic)) return payloadSemantic.trim();

  return '';
}

function extractUrlFromButton(b) {
  const direct =
    b?.url ??
    b?.href ??
    b?.link ??
    b?.request?.url ??
    b?.request?.href ??
    b?.request?.link ??
    b?.request?.payload?.url ??
    b?.request?.payload?.href ??
    b?.request?.payload?.link;

  if (typeof direct === 'string' && isHttpUrl(direct)) return direct.trim();

  const fromReq = extractUrlFromRequest(b?.request);
  if (fromReq) return fromReq;

  const semantic = btnPayload(b);
  if (isHttpUrl(semantic)) return semantic.trim();

  return '';
}

function makeKeyboard(userId, buttons) {
  const rows = [];
  let currentRow = [];

  for (const b of buttons) {
    const text = btnLabel(b);
    const url = extractUrlFromButton(b);

    if (DEBUG_BUTTONS) {
      console.log('[buttons] choice', {
        label: text,
        url: url || null,
        reqType: b?.request?.type || null,
        hasActions: Array.isArray(b?.request?.payload?.actions),
      });
    }

    let buttonObj;
    if (url) {
      if (CALENDLY_MINI_APP_URL && url.includes('calendly.com')) {
        // Swap for Mini App
        const webAppUrl = `${CALENDLY_MINI_APP_URL}?url=${encodeURIComponent(url)}`;
        buttonObj = { text, web_app: { url: webAppUrl } };
      } else if (MARKETPLACE_MINI_APP_URL && (url.toLowerCase().includes('marketplace') || url.toLowerCase().includes('dutyfree'))) {
        // Swap for Marketplace Mini App
        buttonObj = { text, web_app: { url: MARKETPLACE_MINI_APP_URL } };
      } else if (RESERVATIONS_MINI_APP_URL && url.toLowerCase().includes('reservations')) {
        // Swap for Reservations Mini App
        buttonObj = { text, web_app: { url: RESERVATIONS_MINI_APP_URL } };
      } else {
        buttonObj = { text, url };
      }
    } else {
      let data = btnPayload(b) || text;
      if (Buffer.byteLength(data, 'utf8') > 64) data = stashPut(userId, data);
      buttonObj = { text, callback_data: data };
    }

    currentRow.push(buttonObj);

    // Push row when we have 2 buttons
    if (currentRow.length === 2) {
      rows.push(currentRow);
      currentRow = [];
    }
  }

  // Push any remaining button (odd number case)
  if (currentRow.length > 0) {
    rows.push(currentRow);
  }

  return rows;
}

function makeCardV2Keyboard(userId, buttons = []) {
  const rows = [];
  let currentRow = [];

  for (const b of buttons) {
    const label = String(b?.name ?? b?.request?.payload?.label ?? 'Option').slice(0, 64);
    const url = extractUrlFromButton(b);

    if (DEBUG_BUTTONS) {
      console.log('[buttons] cardV2', {
        label,
        url: url || null,
        reqType: b?.request?.type || null,
        hasActions: Array.isArray(b?.request?.payload?.actions),
      });
    }

    let buttonObj;
    if (url) {
      if (CALENDLY_MINI_APP_URL && url.includes('calendly.com')) {
        // Swap for Mini App
        const webAppUrl = `${CALENDLY_MINI_APP_URL}?url=${encodeURIComponent(url)}`;
        buttonObj = { text: label, web_app: { url: webAppUrl } };
      } else if (MARKETPLACE_MINI_APP_URL && (url.toLowerCase().includes('marketplace') || url.toLowerCase().includes('dutyfree'))) {
        // Swap for Marketplace Mini App
        buttonObj = { text: label, web_app: { url: MARKETPLACE_MINI_APP_URL } };
      } else if (RESERVATIONS_MINI_APP_URL && url.toLowerCase().includes('reservations')) {
        // Swap for Reservations Mini App
        buttonObj = { text: label, web_app: { url: RESERVATIONS_MINI_APP_URL } };
      } else {
        buttonObj = { text: label, url };
      }
    } else {
      let data = `${REQUEST_PREFIX}${JSON.stringify(b?.request || {})}`;
      if (Buffer.byteLength(data, 'utf8') > 64) data = stashPut(userId, data);
      buttonObj = { text: label, callback_data: data };
    }

    currentRow.push(buttonObj);

    // Push row when we have 2 buttons
    if (currentRow.length === 2) {
      rows.push(currentRow);
      currentRow = [];
    }
  }

  // Push any remaining button (odd number case)
  if (currentRow.length > 0) {
    rows.push(currentRow);
  }

  return rows;
}

// =====================
// Typing…
// =====================
function keepTyping(ctx) {
  let stop = false;
  (async function loop() {
    if (stop) return;
    try {
      await ctx.sendChatAction('typing');
    } catch { }
    setTimeout(loop, 4500).unref();
  })();
  return () => {
    stop = true;
  };
}

// =====================
// Rendering
// =====================
function tracesOf(vf) {
  return Array.isArray(vf) ? vf : Array.isArray(vf?.traces) ? vf.traces : [];
}

async function sendChoiceAsNewMessage(ctx, inlineKeyboard) {
  if (!inlineKeyboard?.length) return null;
  const msg = await ctx.reply('Select an option:', { reply_markup: { inline_keyboard: inlineKeyboard } });
  if (msg) lastBotMsgByUser.set(ctx.from.id, { chatId: msg.chat.id, message_id: msg.message_id, keyboard: 'choice' });
  return msg;
}

async function attachChoiceKeyboard(ctx, target, inlineKeyboard) {
  if (!inlineKeyboard?.length) return null;

  // If target is a CARD keyboard, don't overwrite; send separate choice message
  if (target?.keyboard === 'card') {
    const newMsg = await sendChoiceAsNewMessage(ctx, inlineKeyboard);
    if (newMsg) return { chatId: newMsg.chat.id, message_id: newMsg.message_id, keyboard: 'choice' };
    return null;
  }

  // Otherwise, overwrite in place (even if previous was 'choice')
  if (target?.chatId && target?.message_id) {
    try {
      await ctx.telegram.editMessageReplyMarkup(target.chatId, target.message_id, undefined, {
        inline_keyboard: inlineKeyboard,
      });
      const ref = { chatId: target.chatId, message_id: target.message_id, keyboard: 'choice' };
      lastBotMsgByUser.set(ctx.from.id, ref);
      return ref;
    } catch (e) {
      if (DEBUG_BUTTONS) console.log('[choice-edit] failed:', e?.message || e);
    }
  }

  const newMsg = await sendChoiceAsNewMessage(ctx, inlineKeyboard);
  if (newMsg) return { chatId: newMsg.chat.id, message_id: newMsg.message_id, keyboard: 'choice' };
  return null;
}

/**
 * Ensure card keyboard without creating duplicate messages:
 */
async function ensureKeyboardOnMessage(ctx, msg, inlineKeyboard) {
  if (!msg || !inlineKeyboard?.length) return false;

  const tryEdit = async () => {
    await ctx.telegram.editMessageReplyMarkup(msg.chat.id, msg.message_id, undefined, {
      inline_keyboard: inlineKeyboard,
    });
  };

  try {
    await tryEdit();
    return true;
  } catch (e1) {
    const m1 = String(e1?.message || '');
    if (/message is not modified|MESSAGE_NOT_MODIFIED/i.test(m1)) return true;
    if (DEBUG_BUTTONS) console.log('[ensure-edit] failed(1):', m1);
  }

  await new Promise((r) => setTimeout(r, 200));

  try {
    await tryEdit();
    return true;
  } catch (e2) {
    const m2 = String(e2?.message || '');
    if (/message is not modified|MESSAGE_NOT_MODIFIED/i.test(m2)) return true;
    if (DEBUG_BUTTONS) console.log('[ensure-edit] failed(2):', m2);
    return false;
  }
}

/**
 * Detects synthetic buttons based on text content (Calendly, Marketplace, etc.)
 */
function getSyntheticButtons(raw) {
  if (!raw) return [];
  const buttons = [];

  // Calendly
  const calendlyUrl = extractCalendlyUrl(raw);
  if (calendlyUrl && CALENDLY_MINI_APP_URL) {
    buttons.push({ name: '📅 Book Now', request: { url: calendlyUrl } });
  }

  // Mini App link detection (Reservations & Marketplace)
  const miniConfigs = [
    { url: RESERVATIONS_MINI_APP_URL, label: '🍴 Book Dining', pattern: /reservations\.html/i },
    { url: MARKETPLACE_MINI_APP_URL, label: '🛍️ Open Marketplace', pattern: /marketplace\.html/i }
  ];

  for (const config of miniConfigs) {
    if (config.url && config.pattern.test(raw)) {
      buttons.push({ name: config.label, request: { url: config.url } });
    }
  }
  return buttons;
}

/**
 * Cleans the text (removes link/iframe) and provides a prompt if the link was the only content.
 */
function getProcessedTextForButtons(raw, calendlyUrl) {
  let text = raw;
  const iframeRe = /<iframe[^>]*src=["']https:\/\/calendly\.com\/[^"']*["'][^>]*>[^]*?<\/iframe>/gi;
  text = text.replace(iframeRe, '').trim();

  if (calendlyUrl && text.includes(calendlyUrl)) {
    text = text.replace(calendlyUrl, '').replace(/\[\]\(\)/g, '').trim();
  }

  const PROMPT = 'Please use the button below to complete your booking:';
  if (!text && calendlyUrl) {
    text = PROMPT;
  }
  return text;
}

async function renderTextChoiceGalleryAndButtonsLast(ctx, raw, maybeChoice) {
  let lastMsg = null;
  let consumed = false;
  const calendlyUrl = extractCalendlyUrl(raw);
  const textToDisplay = getProcessedTextForButtons(raw, calendlyUrl);

  const buttons = maybeChoice?.payload?.buttons ? [...maybeChoice.payload.buttons] : [];
  const syn = getSyntheticButtons(raw);

  if (DEBUG_BUTTONS && syn.length) console.log('[buttons] Found synthetic:', syn.map(b => b.name));

  for (const synBtn of syn) {
    const synUrl = extractUrlFromButton(synBtn);
    if (!synUrl) continue;
    const alreadyPresent = buttons.some((b) => extractUrlFromButton(b) === synUrl);
    if (!alreadyPresent) {
      buttons.unshift(synBtn);
      if (DEBUG_BUTTONS) console.log('[buttons] added synthetic button:', synBtn.name);
    }
  }

  // --- INTERLEAVED CONTENT SEGMENTATION ---
  const segments = segmentContent(textToDisplay);

  // PREPARE THE KEYBOARD
  let kb = null;
  if (buttons.length) {
    const rows = makeKeyboard(ctx.from.id, buttons);
    if (rows.length) kb = { inline_keyboard: rows };
  }

  // Render segments
  if (segments.length > 0) {
    for (let i = 0; i < segments.length; i++) {
      const seg = segments[i];

      if (seg.type === 'text') {
        const html = mdToHtml(seg.value);
        if (!html) continue;
        const isLast = i === segments.length - 1;
        const attachKb = isLast && kb;
        lastMsg = await safeReplyHtml(ctx, html, attachKb ? { reply_markup: kb } : {});
        if (lastMsg) {
          lastBotMsgByUser.set(ctx.from.id, {
            chatId: lastMsg.chat.id,
            message_id: lastMsg.message_id,
            keyboard: attachKb ? 'choice' : 'none'
          });
          if (attachKb) consumed = true;
        }
      } else if (seg.type === 'image') {
        // Group consecutive images
        const batch = [seg.value];
        while (i + 1 < segments.length && segments[i + 1].type === 'image') {
          batch.push(segments[++i].value);
        }

        const isLast = i === segments.length - 1;
        const attachKb = isLast && kb;

        if (batch.length === 1) {
          lastMsg = await sendMediaWithCaption(ctx, batch[0], undefined, attachKb ? kb : undefined);
        } else {
          const mediaGroup = batch.slice(0, 10).map(url => ({ type: 'photo', media: url }));
          const groupMsgs = await ctx.replyWithMediaGroup(mediaGroup);
          lastMsg = groupMsgs[groupMsgs.length - 1];
          // If kb needs to be attached to a group, send a follow-up
          if (attachKb) {
            lastMsg = await ctx.reply('Choose an option:', { reply_markup: kb });
          }
        }

        if (lastMsg) {
          lastBotMsgByUser.set(ctx.from.id, {
            chatId: lastMsg.chat.id,
            message_id: lastMsg.message_id,
            keyboard: attachKb ? 'choice' : 'none'
          });
          if (attachKb) consumed = true;
        }
      }
    }
  } else if (textToDisplay.trim()) {
    // Fallback for non-segmented text
    lastMsg = await safeReplyHtml(ctx, mdToHtml(textToDisplay), kb ? { reply_markup: kb } : {});
    if (lastMsg) {
      lastBotMsgByUser.set(ctx.from.id, {
        chatId: lastMsg.chat.id,
        message_id: lastMsg.message_id,
        keyboard: kb ? 'choice' : 'none'
      });
      if (kb) consumed = !!maybeChoice?.payload?.buttons?.length;
    }
  }

  // If we had buttons but didn't "consume" them yet (meaning we didn't attach them to a message)
  // this would be extremely rare with logic above, but for safety:
  if (kb && !consumed) {
    const target = lastMsg
      ? { chatId: lastMsg.chat.id, message_id: lastMsg.message_id, keyboard: 'none' }
      : lastBotMsgByUser.get(ctx.from.id) || null;

    await attachChoiceKeyboard(ctx, target, kb.inline_keyboard);
  }

  // CRITICAL FIX: Only set consumed = true if we actually "stole" the buttons from a real choice trace
  if (kb && maybeChoice) {
    consumed = true;
  }

  return { consumed };
}

async function sendVFToTelegram(ctx, vfResp) {
  const userId = ctx.from.id;
  const traces = tracesOf(vfResp);
  let lastMsgOverall = lastBotMsgByUser.get(userId) || null;

  // If we streamed an AI completion recently, we'll skip the duplicate AI "text" trace
  const comp = completionStateByUser.get(userId);
  const hasRecentCompletionMsg =
    !!comp?.msg && !!comp?.accumulated?.trim() && (comp.active || (comp.endedAt && Date.now() - comp.endedAt < 90_000));

  for (let i = 0; i < traces.length; i += 1) {
    const t = traces[i];
    if (!t) continue;

    if (t.type === 'text') {
      const isAi = t?.payload?.ai === true;

      const raw = textOfTrace(t).trim();
      if (!raw) continue;

      // If we streamed this AI response via completion events, don't send it again
      const isHandledByStreaming = isAi && VF_COMPLETION_TO_TELEGRAM && hasRecentCompletionMsg;
      if (isHandledByStreaming) {
        const lb = lastBotMsgByUser.get(userId);
        if (lb) {
          lastMsgOverall = lb;
          // Even if streamed, detect synthetic buttons to prepend to next choice or attach now
          const syn = getSyntheticButtons(raw);
          if (syn.length) {
            ctx.state = ctx.state || {};
            ctx.state.pendingSyntheticButtons = syn;
            if (DEBUG_BUTTONS) console.log('[streaming] found synthetic buttons in streamed text:', syn.map(b => b.name));

            // If NO choice follows, we should attach them to the streamed bubble now
            const next = traces[i + 1];
            if (next?.type !== 'choice') {
              const kb = makeKeyboard(userId, syn);
              await attachChoiceKeyboard(ctx, lb, kb);
              ctx.state.pendingSyntheticButtons = [];
            }
          }
        }
        continue;
      }

      const next = traces[i + 1];
      const { consumed } = await renderTextChoiceGalleryAndButtonsLast(ctx, raw, next?.type === 'choice' ? next : null);

      // TRACK HISTORY for smarter rendering
      ctx.state = ctx.state || {};
      ctx.state.lastTraceType = 'text';
      ctx.state.lastTraceText = raw;

      if (consumed && next?.type === 'choice') {
        i += 1;
      }
      lastMsgOverall = lastBotMsgByUser.get(userId) || lastMsgOverall;
      continue;
    }

    if (t.type === 'choice') {
      const buttons = t.payload?.buttons || [];
      if (!buttons.length) continue;

      // Ensure streaming edits are flushed before attaching buttons
      await completionFlush(ctx, userId);

      const syn = ctx.state?.pendingSyntheticButtons || [];
      ctx.state.pendingSyntheticButtons = []; // clear

      const mergedButtons = [...buttons];
      for (const s of syn) {
        if (!mergedButtons.some(b => extractUrlFromButton(b) === extractUrlFromButton(s))) {
          mergedButtons.unshift(s);
          if (DEBUG_BUTTONS) console.log('[choice] Merged synthetic button from streaming:', s.name);
        }
      }

      const kb = makeKeyboard(userId, mergedButtons);

      // Prefer the most recent bot message (streaming message sets this)
      const target = lastBotMsgByUser.get(userId) || lastMsgOverall || null;

      const attached = await attachChoiceKeyboard(ctx, target, kb);
      if (attached) lastMsgOverall = attached;
      else lastMsgOverall = lastBotMsgByUser.get(userId) || lastMsgOverall;

      continue;
    }

    if (t.type === 'visual' || t.type === 'image') {
      const url = t.payload?.image || t.payload?.url || t.payload?.src;
      if (DEBUG_MEDIA) console.log('[visual] url=', url);
      if (url) {
        const msg = await sendMediaWithCaption(ctx, url, undefined);
        if (msg) {
          lastMsgOverall = { chatId: msg.chat.id, message_id: msg.message_id, keyboard: 'none' };
          lastBotMsgByUser.set(userId, lastMsgOverall);
        }
      }
      continue;
    }

    // ------- CardV2 -------
    if (t.type === 'cardV2') {
      const title = t.payload?.title || '';
      const descText =
        (typeof t.payload?.description === 'string' ? t.payload?.description : t.payload?.description?.text) || '';
      const mediaUrl = t.payload?.imageUrl || '';

      const buttons = Array.isArray(t.payload?.buttons) ? t.payload.buttons : [];
      const kb = buttons.length ? makeCardV2Keyboard(userId, buttons) : null;
      const replyMarkup = kb ? { inline_keyboard: kb } : null;

      let msgRef = null;

      if (descText && mediaUrl) {
        msgRef = await safeReplyHtml(ctx, mdToHtml(normalizeSpacing(descText)));
        if (msgRef) {
          lastMsgOverall = { chatId: msgRef.chat.id, message_id: msgRef.message_id, keyboard: 'none' };
          lastBotMsgByUser.set(userId, lastMsgOverall);
        }
      } else if (!mediaUrl && (title || descText)) {
        msgRef = await safeReplyHtml(ctx, mdToHtml(normalizeSpacing([title, descText].filter(Boolean).join('\n\n'))), {
          reply_markup: replyMarkup || undefined,
        });
      }

      if (mediaUrl) {
        const mediaMsg = await sendMediaWithCaption(
          ctx,
          mediaUrl,
          title ? mdToHtml(title) : undefined,
          replyMarkup || undefined
        );
        if (mediaMsg) msgRef = mediaMsg;
      }

      if (msgRef) {
        if (kb?.length) await ensureKeyboardOnMessage(ctx, msgRef, kb);
        lastMsgOverall = { chatId: msgRef.chat.id, message_id: msgRef.message_id, keyboard: kb?.length ? 'card' : 'none' };
        lastBotMsgByUser.set(userId, lastMsgOverall);
      }

      continue;
    }

    // ------- Legacy card -------
    if (t.type === 'card') {
      const title = t.payload?.title || '';
      const desc = t.payload?.description || '';
      const link = t.payload?.url;
      const mediaUrl = t.payload?.image || t.payload?.imageUrl || t.payload?.thumbnail || t.payload?.media;

      let topText = '';
      if (mediaUrl) {
        topText = normalizeSpacing([desc, link && !looksLikeMedia(link) ? link : null].filter(Boolean).join('\n'));
      } else {
        topText = normalizeSpacing([title, desc, link && !looksLikeMedia(link) ? link : null].filter(Boolean).join('\n'));
      }

      if (topText) {
        const msg = await safeReplyHtml(ctx, mdToHtml(topText));
        if (msg) {
          lastMsgOverall = { chatId: msg.chat.id, message_id: msg.message_id, keyboard: 'none' };
          lastBotMsgByUser.set(userId, lastMsgOverall);
        }
      }

      if (mediaUrl) {
        const mediaMsg = await sendMediaWithCaption(ctx, mediaUrl, title ? mdToHtml(title) : undefined);
        if (mediaMsg) {
          lastMsgOverall = { chatId: mediaMsg.chat.id, message_id: mediaMsg.message_id, keyboard: 'none' };
          lastBotMsgByUser.set(userId, lastMsgOverall);
        }
      }
      continue;
    }

    // ------- Carousel → sequential cards -------
    if (t.type === 'carousel') {
      const cards = Array.isArray(t.payload?.cards) ? t.payload.cards : [];
      if (!cards.length) continue;

      for (let idx = 0; idx < cards.length; idx += 1) {
        const c = cards[idx] || {};
        const title = c.title || '';
        const desc = (typeof c.description === 'string' ? c.description : c.description?.text) || '';
        const mediaUrl = c.imageUrl || c.image || c.mediaUrl || c.thumbnail;

        const captionHtml = mdToHtml(normalizeSpacing([title, desc].filter(Boolean).join('\n\n'))) || undefined;

        const buttons = Array.isArray(c.buttons) ? c.buttons : [];
        const kb = buttons.length ? makeCardV2Keyboard(userId, buttons) : null;
        const replyMarkup = kb ? { inline_keyboard: kb } : null;

        let msgRef = null;
        if (mediaUrl) {
          msgRef = await sendMediaWithCaption(ctx, mediaUrl, captionHtml, replyMarkup || undefined);
        } else if (captionHtml) {
          msgRef = await safeReplyHtml(ctx, captionHtml, { reply_markup: replyMarkup || undefined });
        }

        if (msgRef) {
          if (kb?.length) await ensureKeyboardOnMessage(ctx, msgRef, kb);
          lastMsgOverall = { chatId: msgRef.chat.id, message_id: msgRef.message_id, keyboard: kb?.length ? 'card' : 'none' };
          lastBotMsgByUser.set(userId, lastMsgOverall);
        }

        await new Promise((r) => setTimeout(r, 250));
      }
      continue;
    }

    // Ignore completion traces here (handled realtime)
    if (t.type === 'completion') continue;
  }
}

// =====================
// Session & auto-reset
// =====================
const sessions = new Map(); // userId -> { lastTs, lastDay }

function localDayStamp(tsMs) {
  const d = new Date(tsMs + LOCAL_UTC_OFFSET_HOURS * 3600 * 1000);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  const day = String(d.getUTCDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
}

function shouldResetConversationFor(userId) {
  const s = sessions.get(userId);
  if (!s) return true;
  const now = Date.now();
  if (SESSION_RESET_HOURS > 0 && now - s.lastTs > SESSION_RESET_HOURS * 3600 * 1000) return true;
  if (SESSION_RESET_ON_DAY_CHANGE && localDayStamp(now) !== s.lastDay) return true;
  return false;
}

function touchSession(userId) {
  const now = Date.now();
  sessions.set(userId, { lastTs: now, lastDay: localDayStamp(now) });
}

async function maybeAutoResetLaunch(ctx) {
  const userId = ctx.from.id;
  if (shouldResetConversationFor(userId)) {
    await resetVoiceflow(userId);
    const traces = await launchVoiceflow(ctx, userId);
    await sendVFToTelegram(ctx, traces);
    touchSession(userId);
    return true;
  }
  return false;
}

// =====================
// Streaming interaction
// =====================
async function streamVoiceflowInteraction(ctx, userId, action) {
  const res = await api.post(
    streamUrl(userId),
    { action },
    {
      headers: vfHeaders({ stream: true }),
      responseType: 'stream',
    }
  );

  const traces = [];
  let finished = false;

  // Force realtime completion processing to be sequential,
  // and WAIT for it before returning (prevents races with buttons).
  // OPTIMIZATION: Use a counter to skip redundant intermediate text edits.
  let realtimeChain = Promise.resolve();
  let latestContentIdx = -1;

  const finish = async (resolve) => {
    if (finished) return;
    finished = true;
    try {
      await realtimeChain;
    } catch { }
    resolve(traces);
  };

  return await new Promise((resolve, reject) => {
    parseSseStream(res.data, ({ event, data }) => {
      if (event === 'trace' && data && typeof data === 'object') {
        const trace = data;
        traces.push(trace);

        const currentIdx = traces.length - 1;
        if (trace.type === 'completion' && trace.payload?.state === 'content') {
          latestContentIdx = currentIdx;
        }

        realtimeChain = realtimeChain.then(async () => {
          // Optimization: Skip rendering if a newer content trace has arrived.
          // handleTraceRealtime will still update the accumulated buffer.
          const isContent = trace.type === 'completion' && trace.payload?.state === 'content';
          const skipRendering = isContent && currentIdx < latestContentIdx;

          await handleTraceRealtime(ctx, trace, { skipRendering });
        }).catch(() => { });
        return;
      }
      if (event === 'end' || event === 'end-of-stream') {
        finish(resolve).catch(() => resolve(traces));
      }
    });

    res.data.on('error', (err) => reject(err));
    res.data.on('end', () => {
      finish(resolve).catch(() => resolve(traces));
    });
  });
}

async function launchVoiceflow(ctx, userId) {
  return await streamVoiceflowInteraction(ctx, userId, { type: 'launch' });
}

async function interactVoiceflow(ctx, userId, text) {
  return await streamVoiceflowInteraction(ctx, userId, { type: 'text', payload: text });
}

async function sendRequestToVoiceflow(ctx, userId, request) {
  return await streamVoiceflowInteraction(ctx, userId, request);
}

// =====================
// ROUTES
// =====================
function wrap(fn) {
  return async (ctx, next) => {
    try {
      await fn(ctx, next);
    } catch (err) {
      console.error('❌ Handler error:', err?.stack || err);
      try {
        await ctx.reply('Sorry, something went wrong. Please try again.');
      } catch { }
    }
  };
}

bot.start(
  wrap(async (ctx) => {
    const userId = ctx.from.id;
    await resetVoiceflow(userId);
    const stop = keepTyping(ctx);
    try {
      const traces = await launchVoiceflow(ctx, userId);
      await sendVFToTelegram(ctx, traces);
      touchSession(userId);
    } finally {
      stop();
    }
  })
);

bot.hears(
  '/start',
  wrap(async (ctx) => {
    const userId = ctx.from.id;
    await resetVoiceflow(userId);
    const stop = keepTyping(ctx);
    try {
      const traces = await launchVoiceflow(ctx, userId);
      await sendVFToTelegram(ctx, traces);
      touchSession(userId);
    } finally {
      stop();
    }
  })
);

bot.on(
  'callback_query',
  wrap(async (ctx) => {
    const userId = ctx.from.id;
    let data = ctx.callbackQuery?.data;

    await ctx.answerCbQuery().catch(() => { });
    const stop = keepTyping(ctx);

    if (await maybeAutoResetLaunch(ctx)) {
      stop();
      return;
    }

    if (typeof data === 'string' && data.startsWith(CALLBACK_PREFIX)) data = stashTake(data, userId) ?? '';

    if (typeof data === 'string' && data.startsWith(REQUEST_PREFIX)) {
      try {
        const req = JSON.parse(data.slice(REQUEST_PREFIX.length));
        const traces = await sendRequestToVoiceflow(ctx, userId, req);
        await sendVFToTelegram(ctx, traces);
        touchSession(userId);
        stop();
        return;
      } catch { }
    }

    if (typeof data === 'string' && data.trim().startsWith('{')) {
      try {
        const obj = JSON.parse(data);
        if (obj && typeof obj === 'object' && obj.type) {
          const traces = await sendRequestToVoiceflow(ctx, userId, obj);
          await sendVFToTelegram(ctx, traces);
          touchSession(userId);
          stop();
          return;
        }
      } catch { }
    }

    if (typeof data !== 'string') data = String(data ?? '');
    try {
      const traces = await interactVoiceflow(ctx, userId, data);
      await sendVFToTelegram(ctx, traces);
      touchSession(userId);
    } finally {
      stop();
    }
  })
);

bot.on(
  'voice',
  wrap(async (ctx) => {
    const userId = ctx.from.id;
    const voice = ctx.message.voice;

    if (!openai) {
      return await ctx.reply('Voice messages are not supported (OpenAI STT not configured).');
    }

    const stop = keepTyping(ctx);

    if (await maybeAutoResetLaunch(ctx)) {
      stop();
      return;
    }

    try {
      console.log(`[stt] processing voice from ${userId}, file_id: ${voice.file_id}`);

      const fileLink = await ctx.telegram.getFileLink(voice.file_id);
      const url = fileLink.toString();

      const response = await api.get(url, { responseType: 'arraybuffer' });
      const buffer = Buffer.from(response.data);
      console.log(`[stt] downloaded ${buffer.length} bytes`);

      console.log('[stt] sending to OpenAI Whisper API via axios...');

      const formData = new FormData();
      formData.append('file', buffer, { filename: `voice_${userId}.ogg`, contentType: 'audio/ogg' });
      formData.append('model', 'whisper-1');

      const sttRes = await api.post('https://api.openai.com/v1/audio/transcriptions', formData, {
        headers: {
          'Authorization': `Bearer ${OPENAI_API_KEY}`,
          ...formData.getHeaders()
        }
      });

      const text = sttRes.data.text;
      console.log(`[stt] success: "${text}"`);

      if (!text || !text.trim()) {
        await ctx.reply("Sorry, I couldn't hear what you said. Could you try again?");
        return;
      }

      // Inform the user what we heard (as requested: "sent as a text")
      await ctx.reply(`<i>" ${text} "</i>`, { parse_mode: 'HTML' });

      const traces = await interactVoiceflow(ctx, userId, text);
      await sendVFToTelegram(ctx, traces);
      touchSession(userId);
    } catch (err) {
      const errDetail = err?.response?.data?.error?.message || err?.message || String(err);
      console.error('❌ STT error:', errDetail, err?.response?.data || '');
      await ctx.reply(`Sorry, I had trouble processing your voice message: ${errDetail}`);
    } finally {
      stop();
    }
  })
);

bot.on(
  'text',
  wrap(async (ctx) => {
    const userId = ctx.from.id;
    const text = ctx.message.text;
    if (text.trim() === '/start') return;

    const stop = keepTyping(ctx);

    if (await maybeAutoResetLaunch(ctx)) {
      stop();
      return;
    }

    try {
      const traces = await interactVoiceflow(ctx, userId, text);
      await sendVFToTelegram(ctx, traces);
      touchSession(userId);
    } finally {
      stop();
    }
  })
);

// =====================
// START
// =====================
bot.launch({ polling: { timeout: 60 } });
console.log('✅ Telegram ↔ Voiceflow bridge running (STREAMING)');

bot.catch((err, ctx) => {
  console.error('❌ Telegraf caught error for update:', JSON.stringify(ctx.update || {}));
  console.error(err?.stack || err);
});

process.on('unhandledRejection', (reason) => {
  console.error('UNHANDLED REJECTION:', reason);
});
process.on('uncaughtException', (err) => {
  console.error('UNCAUGHT EXCEPTION:', err?.stack || err);
});
process.once('SIGINT', () => bot.stop('SIGINT'));
process.once('SIGTERM', () => bot.stop('SIGTERM'));
