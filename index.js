// path: index.js
import dotenv from 'dotenv';
dotenv.config({ override: true });

import axios from 'axios';
import http from 'http';
import https from 'https';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { Telegraf } from 'telegraf';
import { randomUUID } from 'crypto';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// =====================
// ENV
// =====================
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const VF_API_KEY = process.env.VF_API_KEY;

const VF_VERSION_ID = process.env.VF_VERSION_ID || '';
const VF_USE_VERSION_HEADER = /^true$/i.test(process.env.VF_USE_VERSION_HEADER || '');
const MEDIA_FORCE_UPLOAD = process.env.MEDIA_FORCE_UPLOAD
  ? /^true$/i.test(process.env.MEDIA_FORCE_UPLOAD)
  : true;
const DEBUG_MEDIA = /^true$/i.test(process.env.DEBUG_MEDIA || '');
const DEBUG_BUTTONS = /^true$/i.test(process.env.DEBUG_BUTTONS || '');

// Session/auto-reset config
const SESSION_RESET_HOURS = parseFloat(process.env.SESSION_RESET_HOURS || '24');
const SESSION_RESET_ON_DAY_CHANGE = /^true$/i.test(process.env.SESSION_RESET_ON_DAY_CHANGE || 'true');
const LOCAL_UTC_OFFSET_HOURS = parseFloat(process.env.LOCAL_UTC_OFFSET_HOURS || '0');

// Media cache config
const MEDIA_CACHE_PATH = process.env.MEDIA_CACHE_PATH || path.join(__dirname, 'media-cache.json');
const MEDIA_CACHE_MAX_ENTRIES = parseInt(process.env.MEDIA_CACHE_MAX_ENTRIES || '1000', 10);

if (!TELEGRAM_BOT_TOKEN || !VF_API_KEY) {
  console.error('❌ Missing env. Need TELEGRAM_BOT_TOKEN and VF_API_KEY');
  process.exit(1);
}

// Tripled handler timeout (helps on slow VF turns)
const bot = new Telegraf(TELEGRAM_BOT_TOKEN, { handlerTimeout: 45_000 });
console.log(
  (VF_USE_VERSION_HEADER
    ? `🔒 VF pinned versionID=${VF_VERSION_ID || '(empty)'}`
    : '🚀 VF Published (no version header)') +
    ` | Media force upload: ${MEDIA_FORCE_UPLOAD}`
);

// =====================
// HTTP (keep-alive)
// =====================
const httpAgent = new http.Agent({ keepAlive: true, keepAliveMsecs: 10_000, maxSockets: 50 });
const httpsAgent = new https.Agent({ keepAlive: true, keepAliveMsecs: 10_000, maxSockets: 50 });
const api = axios.create({
  timeout: 45_000,
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
// Voiceflow helpers
// =====================
const userStateBase = (userId) => `https://general-runtime.voiceflow.com/state/user/telegram_${userId}`;
const interactUrl = (userId) => `${userStateBase(userId)}/interact`;

function vfHeaders() {
  const headers = { Authorization: VF_API_KEY, 'Content-Type': 'application/json' };
  if (VF_USE_VERSION_HEADER && VF_VERSION_ID) headers.versionID = VF_VERSION_ID;
  return headers;
}

async function resetVoiceflow(userId) {
  try {
    await api.delete(userStateBase(userId), { headers: vfHeaders() });
  } catch {}
}

async function launchVoiceflow(userId) {
  const { data } = await api.post(interactUrl(userId), { action: { type: 'launch' } }, { headers: vfHeaders() });
  return data;
}

async function interactVoiceflow(userId, text) {
  const { data } = await api.post(
    interactUrl(userId),
    { action: { type: 'text', payload: text } },
    { headers: vfHeaders() }
  );
  return data;
}

async function sendRequestToVoiceflow(userId, request) {
  const { data } = await api.post(interactUrl(userId), { request }, { headers: vfHeaders() });
  return data;
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

function isHttpUrl(s) {
  if (typeof s !== 'string') return false;
  const v = s.trim();
  return /^https?:\/\/\S+$/i.test(v);
}

function stripTags(s) {
  return String(s || '').replace(/<[^>]*>/g, '');
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

// Markdown → Telegram HTML (bold/italic + hyperlinks ONLY)
function mdToHtml(input) {
  if (!input) return '';

  let raw = unescapeVfHtmlArtifacts(input);
  raw = normalizeSpacing(raw);
  raw = htmlAnchorsToMarkdown(raw);
  raw = compactLabelUrlLines(raw);
  raw = linkifyBareUrlsToMarkdown(raw);

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
  return t?.payload?.message ?? slateToText(t?.payload?.slate) ?? '';
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
  } catch {}
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
      } catch {}
      try {
        const m = await ctx.replyWithDocument(direct, opts);
        extractAndCacheFileId(direct, 'document', m);
        return m;
      } catch {}
      try {
        const m = await ctx.replyWithPhoto(direct, opts);
        extractAndCacheFileId(direct, 'photo', m);
        return m;
      } catch {}
    } else {
      try {
        const m = await ctx.replyWithPhoto(direct, opts);
        extractAndCacheFileId(direct, 'photo', m);
        return m;
      } catch {}
      try {
        const m = await ctx.replyWithDocument(direct, opts);
        extractAndCacheFileId(direct, 'document', m);
        return m;
      } catch {}
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
      } catch {}
      try {
        const m = await ctx.replyWithDocument(input, opts);
        extractAndCacheFileId(direct, 'document', m);
        return m;
      } catch {}
      try {
        const m = await ctx.replyWithPhoto(input, opts);
        extractAndCacheFileId(direct, 'photo', m);
        return m;
      } catch {}
    } else {
      try {
        const m = await ctx.replyWithPhoto(input, opts);
        extractAndCacheFileId(direct, 'photo', m);
        return m;
      } catch {}
      try {
        const m = await ctx.replyWithDocument(input, opts);
        extractAndCacheFileId(direct, 'document', m);
        return m;
      } catch {}
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

    if (url) {
      rows.push([{ text, url }]);
      continue;
    }

    let data = btnPayload(b) || text;
    if (Buffer.byteLength(data, 'utf8') > 64) data = stashPut(userId, data);
    rows.push([{ text, callback_data: data }]);
  }
  return rows;
}

function makeCardV2Keyboard(userId, buttons = []) {
  const rows = [];
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

    if (url) {
      rows.push([{ text: label, url }]);
      continue;
    }

    let data = `${REQUEST_PREFIX}${JSON.stringify(b?.request || {})}`;
    if (Buffer.byteLength(data, 'utf8') > 64) data = stashPut(userId, data);
    rows.push([{ text: label, callback_data: data }]);
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
    } catch {}
    setTimeout(loop, 4500).unref();
  })();
  return () => {
    stop = true;
  };
}

// =====================
// Rendering
// =====================
/**
 * Track the last message AND whether we attached an inline keyboard to it.
 * This is critical to prevent VF 'choice' from overwriting card buttons.
 */
const lastBotMsgByUser = new Map(); // userId -> { chatId, message_id, hasKeyboard }
function tracesOf(vf) {
  return Array.isArray(vf) ? vf : Array.isArray(vf?.traces) ? vf.traces : [];
}

async function sendChoiceAsNewMessage(ctx, inlineKeyboard) {
  if (!inlineKeyboard?.length) return null;
  return await ctx.reply('Choose an option:', { reply_markup: { inline_keyboard: inlineKeyboard } });
}

async function attachChoiceKeyboard(ctx, target, inlineKeyboard) {
  if (!inlineKeyboard?.length) return false;

  // If target already has keyboard, do NOT overwrite it: send choice separately.
  if (target?.hasKeyboard) {
    await sendChoiceAsNewMessage(ctx, inlineKeyboard);
    return true;
  }

  if (target?.chatId && target?.message_id) {
    try {
      await ctx.telegram.editMessageReplyMarkup(target.chatId, target.message_id, undefined, {
        inline_keyboard: inlineKeyboard,
      });
      return true;
    } catch (e) {
      if (DEBUG_BUTTONS) console.log('[choice-edit] failed:', e?.message || e);
    }
  }

  await sendChoiceAsNewMessage(ctx, inlineKeyboard);
  return true;
}

/**
 * Ensure card keyboard without creating duplicate "small button" messages:
 * - retry edit once or twice
 * - never send fallback choice message from here
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
    // Telegram returns variants of "message is not modified" when markup already matches.
    if (/message is not modified|MESSAGE_NOT_MODIFIED/i.test(m1)) return true;
    if (DEBUG_BUTTONS) console.log('[ensure-edit] failed(1):', m1);
  }

  // short delay and retry once (helps with last-message timing)
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

async function renderTextChoiceGalleryAndButtonsLast(ctx, raw, maybeChoice) {
  let lastMsg = null;
  const { head, items, tail } = parseGalleryBlocks(raw);

  if (head) {
    lastMsg = await ctx.reply(mdToHtml(head), { parse_mode: 'HTML', disable_web_page_preview: true });
    if (lastMsg) lastBotMsgByUser.set(ctx.from.id, { chatId: lastMsg.chat.id, message_id: lastMsg.message_id, hasKeyboard: false });
  }

  for (const it of items) {
    const titleHtml = mdToHtml(it.title || '');
    const menuHtml = it.menuUrl ? `\n<a href="${esc(it.menuUrl)}">View Menu</a>` : '';
    const caption = (titleHtml + menuHtml).trim();
    const msg = await sendMediaWithCaption(ctx, it.imageUrl, caption);
    if (msg) {
      lastMsg = msg;
      lastBotMsgByUser.set(ctx.from.id, { chatId: msg.chat.id, message_id: msg.message_id, hasKeyboard: false });
    }
  }

  if (tail) {
    lastMsg = await ctx.reply(mdToHtml(tail), { parse_mode: 'HTML', disable_web_page_preview: true });
    if (lastMsg) lastBotMsgByUser.set(ctx.from.id, { chatId: lastMsg.chat.id, message_id: lastMsg.message_id, hasKeyboard: false });
  }

  let consumed = false;
  const buttons = maybeChoice?.payload?.buttons || [];
  if (buttons.length) {
    const kb = makeKeyboard(ctx.from.id, buttons);
    const target = lastMsg
      ? { chatId: lastMsg.chat.id, message_id: lastMsg.message_id, hasKeyboard: false }
      : lastBotMsgByUser.get(ctx.from.id) || null;

    await attachChoiceKeyboard(ctx, target, kb);
    consumed = true;
  }

  return { consumed };
}

async function sendVFToTelegram(ctx, vfResp) {
  const traces = tracesOf(vfResp);
  let lastMsgOverall = lastBotMsgByUser.get(ctx.from.id) || null;

  for (let i = 0; i < traces.length; i += 1) {
    const t = traces[i];
    if (!t) continue;

    if (t.type === 'text') {
      const raw = textOfTrace(t).trim();
      const next = traces[i + 1];
      const { consumed } = await renderTextChoiceGalleryAndButtonsLast(ctx, raw, next?.type === 'choice' ? next : null);
      if (consumed) {
        i += 1;
        lastMsgOverall = lastBotMsgByUser.get(ctx.from.id) || lastMsgOverall;
        continue;
      }
      lastMsgOverall = lastBotMsgByUser.get(ctx.from.id) || lastMsgOverall;
      continue;
    }

    if (t.type === 'choice') {
      const buttons = t.payload?.buttons || [];
      if (!buttons.length) continue;

      const kb = makeKeyboard(ctx.from.id, buttons);
      const target = lastMsgOverall || lastBotMsgByUser.get(ctx.from.id) || null;

      await attachChoiceKeyboard(ctx, target, kb);
      continue;
    }

    if (t.type === 'visual' || t.type === 'image') {
      const url = t.payload?.image || t.payload?.url || t.payload?.src;
      if (DEBUG_MEDIA) console.log('[visual] url=', url);
      if (url) {
        const msg = await sendMediaWithCaption(ctx, url, undefined);
        if (msg) {
          lastMsgOverall = { chatId: msg.chat.id, message_id: msg.message_id, hasKeyboard: false };
          lastBotMsgByUser.set(ctx.from.id, lastMsgOverall);
        }
      }
      continue;
    }

    // ------- CardV2 -------
    if (t.type === 'cardV2') {
      const title = t.payload?.title || '';
      const descText =
        (typeof t.payload?.description === 'string'
          ? t.payload?.description
          : t.payload?.description?.text) || '';
      const mediaUrl = t.payload?.imageUrl || '';

      const buttons = Array.isArray(t.payload?.buttons) ? t.payload.buttons : [];
      const kb = buttons.length ? makeCardV2Keyboard(ctx.from.id, buttons) : null;
      const replyMarkup = kb ? { inline_keyboard: kb } : null;

      let msgRef = null;

      // Keep existing behavior: description text as separate message when both desc+media exist.
      if (descText && mediaUrl) {
        msgRef = await ctx.reply(mdToHtml(normalizeSpacing(descText)), {
          parse_mode: 'HTML',
          disable_web_page_preview: true,
        });
        if (msgRef) {
          lastMsgOverall = { chatId: msgRef.chat.id, message_id: msgRef.message_id, hasKeyboard: false };
          lastBotMsgByUser.set(ctx.from.id, lastMsgOverall);
        }
      } else if (!mediaUrl && (title || descText)) {
        msgRef = await ctx.reply(mdToHtml(normalizeSpacing([title, descText].filter(Boolean).join('\n\n'))), {
          parse_mode: 'HTML',
          disable_web_page_preview: true,
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
        lastMsgOverall = { chatId: msgRef.chat.id, message_id: msgRef.message_id, hasKeyboard: !!kb?.length };
        lastBotMsgByUser.set(ctx.from.id, lastMsgOverall);
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
        const msg = await ctx.reply(mdToHtml(topText), { parse_mode: 'HTML', disable_web_page_preview: true });
        if (msg) {
          lastMsgOverall = { chatId: msg.chat.id, message_id: msg.message_id, hasKeyboard: false };
          lastBotMsgByUser.set(ctx.from.id, lastMsgOverall);
        }
      }

      if (mediaUrl) {
        const mediaMsg = await sendMediaWithCaption(ctx, mediaUrl, title ? mdToHtml(title) : undefined);
        if (mediaMsg) {
          lastMsgOverall = { chatId: mediaMsg.chat.id, message_id: mediaMsg.message_id, hasKeyboard: false };
          lastBotMsgByUser.set(ctx.from.id, lastMsgOverall);
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
        const kb = buttons.length ? makeCardV2Keyboard(ctx.from.id, buttons) : null;
        const replyMarkup = kb ? { inline_keyboard: kb } : null;

        let msgRef = null;
        if (mediaUrl) {
          msgRef = await sendMediaWithCaption(ctx, mediaUrl, captionHtml, replyMarkup || undefined);
        } else if (captionHtml) {
          msgRef = await ctx.reply(captionHtml, {
            parse_mode: 'HTML',
            disable_web_page_preview: true,
            reply_markup: replyMarkup || undefined,
          });
        }

        if (msgRef) {
          if (kb?.length) await ensureKeyboardOnMessage(ctx, msgRef, kb);
          lastMsgOverall = { chatId: msgRef.chat.id, message_id: msgRef.message_id, hasKeyboard: !!kb?.length };
          lastBotMsgByUser.set(ctx.from.id, lastMsgOverall);
        }

        await new Promise((r) => setTimeout(r, 250));
      }
      continue;
    }
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
    const vf = await launchVoiceflow(userId);
    await sendVFToTelegram(ctx, vf);
    touchSession(userId);
    return true;
  }
  return false;
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
      } catch {}
    }
  };
}

bot.start(
  wrap(async (ctx) => {
    const userId = ctx.from.id;
    await resetVoiceflow(userId);
    const stop = keepTyping(ctx);
    try {
      const vf = await launchVoiceflow(userId);
      await sendVFToTelegram(ctx, vf);
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
      const vf = await launchVoiceflow(userId);
      await sendVFToTelegram(ctx, vf);
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

    await ctx.answerCbQuery().catch(() => {});
    const stop = keepTyping(ctx);

    if (await maybeAutoResetLaunch(ctx)) {
      stop();
      return;
    }

    if (typeof data === 'string' && data.startsWith(CALLBACK_PREFIX)) data = stashTake(data, userId) ?? '';

    if (typeof data === 'string' && data.startsWith(REQUEST_PREFIX)) {
      try {
        const req = JSON.parse(data.slice(REQUEST_PREFIX.length));
        const vfResp = await sendRequestToVoiceflow(userId, req);
        await sendVFToTelegram(ctx, vfResp);
        touchSession(userId);
        stop();
        return;
      } catch {}
    }

    if (typeof data === 'string' && data.trim().startsWith('{')) {
      try {
        const obj = JSON.parse(data);
        if (obj && typeof obj === 'object' && obj.type) {
          const vfResp = await sendRequestToVoiceflow(userId, obj);
          await sendVFToTelegram(ctx, vfResp);
          touchSession(userId);
          stop();
          return;
        }
      } catch {}
    }

    if (typeof data !== 'string') data = String(data ?? '');
    try {
      const vfResp = await interactVoiceflow(userId, data);
      await sendVFToTelegram(ctx, vfResp);
      touchSession(userId);
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
      const vfResp = await interactVoiceflow(userId, text);
      await sendVFToTelegram(ctx, vfResp);
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
console.log('✅ Telegram ↔ Voiceflow bridge running');

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
