require('dotenv').config();

const express = require('express');
const multer = require('multer');
const WebSocket = require('ws');
const ffmpeg = require('fluent-ffmpeg');
const { PassThrough } = require('stream');
const { Buffer } = require('buffer');
const { randomUUID } = require('crypto');
const { gzipSync, gunzipSync } = require('zlib');

const app = express();
const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: Number(process.env.MAX_UPLOAD_BYTES || 25 * 1024 * 1024)
  }
});

const PORT = Number(process.env.PORT || 8080);

// SeedASR 2.0 / v3 WebSocket 鉴权：需要 App ID + Access Token。
// 为兼容旧 .env，VOLC_API_KEY 仅作为 VOLC_ACCESS_KEY 的别名；它不能代替 VOLC_APP_KEY。
const VOLC_APP_KEY = process.env.VOLC_APP_KEY || process.env.VOLC_APP_ID || '';
const VOLC_ACCESS_KEY = process.env.VOLC_ACCESS_KEY || '';
const VOLC_API_KEY = process.env.VOLC_API_KEY || ''; // 仅用于兼容旧代理鉴权方式，官方方式优先使用 App ID + Access Token。
const VOLC_WS_URL = process.env.VOLC_WS_URL || 'wss://openspeech.bytedance.com/api/v3/sauc/bigmodel_nostream';
const VOLC_RESOURCE_ID = process.env.VOLC_RESOURCE_ID || 'volc.seedasr.sauc.duration';
const MODEL_NAME = process.env.MODEL_NAME || process.env.VOLC_MODEL_NAME || 'bigmodel';

// 可选：给本代理本身加 Bearer Token；测试阶段留空即可。
const PROXY_API_KEY = process.env.PROXY_API_KEY || '';
const CORS_ENABLE = String(process.env.CORS_ENABLE || 'true').toLowerCase() === 'true';
const LOG_LEVEL = String(process.env.LOG_LEVEL || 'info').toLowerCase();
const SEGMENT_DURATION_MS = Math.max(100, Number(process.env.SEGMENT_DURATION_MS || 200));
const SEND_INTERVAL_MS = Math.max(0, Number(process.env.SEND_INTERVAL_MS || 0));
const REQUEST_TIMEOUT_MS = Math.max(5000, Number(process.env.REQUEST_TIMEOUT_MS || 90000));
const HANDSHAKE_TIMEOUT_MS = Math.max(3000, Number(process.env.HANDSHAKE_TIMEOUT_MS || 15000));
const SHOW_UTTERANCES = String(process.env.SHOW_UTTERANCES || 'false').toLowerCase() === 'true';
const RESULT_TYPE = process.env.RESULT_TYPE || 'full';

const LOG_LEVEL_PRIORITY = {
  debug: 10,
  info: 20,
  warn: 30,
  error: 40
};

const MSG_TYPE = {
  CLIENT_FULL_REQUEST: 0b0001,
  CLIENT_AUDIO_ONLY_REQUEST: 0b0010,
  SERVER_FULL_RESPONSE: 0b1001,
  SERVER_ERROR_RESPONSE: 0b1111
};

const FLAGS = {
  POS_SEQUENCE: 0b0001,
  NEG_WITH_SEQUENCE: 0b0011
};

const SERIALIZATION = {
  NONE: 0b0000,
  JSON: 0b0001
};

const COMPRESSION = {
  NONE: 0b0000,
  GZIP: 0b0001
};

const PROTOCOL_VERSION = 0b0001;

function normalizeLogLevel(level) {
  return LOG_LEVEL_PRIORITY[level] ? level : 'info';
}

const normalizedLogLevel = normalizeLogLevel(LOG_LEVEL);
const activeLogLevelPriority = LOG_LEVEL_PRIORITY[normalizedLogLevel];

function redactSensitiveContext(ctx) {
  if (!ctx || typeof ctx !== 'object') return ctx;
  const cloned = { ...ctx };
  for (const key of ['apiKey', 'accessKey', 'appKey', 'authorization', 'token']) {
    if (typeof cloned[key] === 'string' && cloned[key]) cloned[key] = '***';
  }
  return cloned;
}

function log(level, message, context = {}) {
  const priority = LOG_LEVEL_PRIORITY[level] || LOG_LEVEL_PRIORITY.info;
  if (priority < activeLogLevelPriority) return;

  const payload = {
    ts: new Date().toISOString(),
    level,
    message,
    ...redactSensitiveContext(context)
  };

  const line = JSON.stringify(payload);
  if (level === 'error') return console.error(line);
  if (level === 'warn') return console.warn(line);
  console.log(line);
}

const upstreamAuthMode = VOLC_APP_KEY && VOLC_ACCESS_KEY
  ? 'app_access'
  : (VOLC_API_KEY ? 'legacy_api_key' : 'missing');

if (upstreamAuthMode === 'missing') {
  console.error('错误：请配置 VOLC_APP_KEY + VOLC_ACCESS_KEY（推荐/官方），或至少保留旧 VOLC_API_KEY 供兼容测试');
  process.exit(1);
}

if (upstreamAuthMode === 'legacy_api_key') {
  console.warn('警告：当前仅检测到 VOLC_API_KEY，将使用旧 X-Api-Key 鉴权做兼容测试；若握手失败，请改用 VOLC_APP_KEY + VOLC_ACCESS_KEY');
}

if (MODEL_NAME !== 'bigmodel') {
  console.error('错误：MODEL_NAME/VOLC_MODEL_NAME 当前应配置为 bigmodel');
  process.exit(1);
}

if (CORS_ENABLE) {
  app.use((req, res, next) => {
    const origin = req.headers.origin || '*';
    res.setHeader('Access-Control-Allow-Origin', origin);
    res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type,Authorization,X-Request-Id');

    if (req.method === 'OPTIONS') return res.sendStatus(204);
    next();
  });
}

function buildRequestId() {
  return `seedasr-${Date.now()}-${Math.random().toString(16).slice(2, 10)}`;
}

function getRequestMeta(req) {
  return {
    method: req.method,
    path: req.originalUrl || req.url,
    ip: req.headers['x-forwarded-for'] || req.socket.remoteAddress || '',
    userAgent: req.headers['user-agent'] || ''
  };
}

app.use((req, res, next) => {
  const requestId = req.headers['x-request-id'] || buildRequestId();
  req.requestId = requestId;
  res.setHeader('X-Request-Id', requestId);

  const startAt = Date.now();
  const reqMeta = getRequestMeta(req);
  log('info', 'request.received', { requestId, ...reqMeta });

  res.on('finish', () => {
    log('info', 'request.completed', {
      requestId,
      ...reqMeta,
      statusCode: res.statusCode,
      durationMs: Date.now() - startAt
    });
  });

  next();
});

function assertProxyAuth(req) {
  if (!PROXY_API_KEY) return true;
  const auth = String(req.headers.authorization || '');
  const expected = `Bearer ${PROXY_API_KEY}`;
  return auth === expected;
}

function transcodeToPcm16kMono(inputBuffer) {
  return new Promise((resolve, reject) => {
    const pcmChunks = [];
    const pass = new PassThrough();
    pass.end(inputBuffer);

    const command = ffmpeg(pass)
      .audioFrequency(16000)
      .audioChannels(1)
      .audioCodec('pcm_s16le')
      .format('s16le')
      .on('error', reject);

    const output = command.pipe();
    output.on('data', (buf) => pcmChunks.push(buf));
    output.on('error', reject);
    output.on('end', () => resolve(Buffer.concat(pcmChunks)));
  });
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function buildHeader(messageType, messageTypeSpecificFlags, serialization, compression) {
  const header = Buffer.alloc(4);
  // 高 4 bit = protocol version(1)，低 4 bit = header size，以 4 bytes 为单位，这里固定 1。
  header[0] = (PROTOCOL_VERSION << 4) | 0b0001;
  header[1] = (messageType << 4) | messageTypeSpecificFlags;
  header[2] = (serialization << 4) | compression;
  header[3] = 0x00;
  return header;
}

function buildFullClientRequest(seq, payloadObj) {
  const payload = gzipSync(Buffer.from(JSON.stringify(payloadObj), 'utf8'));
  const header = buildHeader(
    MSG_TYPE.CLIENT_FULL_REQUEST,
    FLAGS.POS_SEQUENCE,
    SERIALIZATION.JSON,
    COMPRESSION.GZIP
  );

  const seqBuf = Buffer.alloc(4);
  seqBuf.writeInt32BE(seq, 0);

  const sizeBuf = Buffer.alloc(4);
  sizeBuf.writeUInt32BE(payload.length, 0);

  return Buffer.concat([header, seqBuf, sizeBuf, payload]);
}

function buildAudioOnlyRequest(seq, audioChunk, isLast) {
  const flags = isLast ? FLAGS.NEG_WITH_SEQUENCE : FLAGS.POS_SEQUENCE;
  const actualSeq = isLast ? -seq : seq;
  const payload = gzipSync(audioChunk);

  const header = buildHeader(
    MSG_TYPE.CLIENT_AUDIO_ONLY_REQUEST,
    flags,
    SERIALIZATION.NONE,
    COMPRESSION.GZIP
  );

  const seqBuf = Buffer.alloc(4);
  seqBuf.writeInt32BE(actualSeq, 0);

  const sizeBuf = Buffer.alloc(4);
  sizeBuf.writeUInt32BE(payload.length, 0);

  return Buffer.concat([header, seqBuf, sizeBuf, payload]);
}

function decodePayload(serialization, compression, payload) {
  let decoded = payload;
  if (compression === COMPRESSION.GZIP && payload.length > 0) {
    decoded = gunzipSync(payload);
  }

  if (serialization === SERIALIZATION.JSON && decoded.length > 0) {
    return JSON.parse(decoded.toString('utf8'));
  }

  return decoded;
}

function parseServerFrame(frame) {
  const msg = Buffer.isBuffer(frame) ? frame : Buffer.from(frame);
  if (msg.length < 4) throw new Error('SeedASR 响应帧过短');

  const headerSizeWords = msg[0] & 0x0f;
  const headerSize = headerSizeWords * 4;
  const messageType = msg[1] >> 4;
  const flags = msg[1] & 0x0f;
  const serialization = msg[2] >> 4;
  const compression = msg[2] & 0x0f;

  if (headerSize < 4 || msg.length < headerSize) {
    throw new Error(`SeedASR 非法 header size: ${headerSize}`);
  }

  let offset = headerSize;
  let sequence = null;
  const hasSequence = (flags & 0x01) !== 0;
  const isLast = (flags & 0x02) !== 0;

  if (hasSequence) {
    if (msg.length < offset + 4) throw new Error('SeedASR 响应缺少 sequence');
    sequence = msg.readInt32BE(offset);
    offset += 4;
  }

  if (messageType === MSG_TYPE.SERVER_FULL_RESPONSE) {
    if (msg.length < offset + 4) throw new Error('SeedASR 响应缺少 payload size');
    const payloadSize = msg.readUInt32BE(offset);
    offset += 4;
    if (msg.length < offset + payloadSize) throw new Error('SeedASR 响应 payload 长度不足');

    const payload = msg.subarray(offset, offset + payloadSize);
    return {
      messageType,
      sequence,
      isLast,
      payload: decodePayload(serialization, compression, payload)
    };
  }

  if (messageType === MSG_TYPE.SERVER_ERROR_RESPONSE) {
    if (msg.length < offset + 8) throw new Error('SeedASR 错误响应帧长度不足');
    const errorCode = msg.readInt32BE(offset);
    offset += 4;
    const payloadSize = msg.readUInt32BE(offset);
    offset += 4;
    const payload = msg.subarray(offset, offset + payloadSize);

    let detail;
    try {
      detail = decodePayload(serialization, compression, payload);
    } catch (_) {
      detail = payload.toString('utf8');
    }

    return {
      messageType,
      sequence,
      isLast,
      errorCode,
      error: detail
    };
  }

  return { messageType, sequence, isLast, payload: null };
}

function extractResultText(payload) {
  if (!payload || typeof payload !== 'object') return '';

  // SeedASR v3 常见结构：payload.result.text
  const result = payload.result || payload.payload_msg?.result || payload.payload?.result;
  if (!result) return '';

  if (typeof result.text === 'string' && result.text.trim()) {
    return result.text.trim();
  }

  if (Array.isArray(result.utterances)) {
    const texts = result.utterances
      .filter((item) => item && typeof item.text === 'string' && item.text.trim())
      .map((item) => item.text.trim());
    if (texts.length) return texts.join(' ');
  }

  return '';
}

function isNostreamEndpoint() {
  return VOLC_WS_URL.toLowerCase().includes('/bigmodel_nostream');
}

// 官方文档对 bigmodel_nostream 的 language 使用 locale 形式。
// 中文/方言默认就在自动识别范围内，因此 zh/zh-CN 不强塞 language，避免服务端参数不兼容。
function normalizeSeedAsrLanguage(language) {
  const raw = String(language || '').trim();
  if (!raw) return '';

  const lower = raw.toLowerCase();
  if (lower === 'zh' || lower.startsWith('zh-') || lower === 'yue' || lower === 'cmn') {
    return '';
  }

  const map = {
    en: 'en-US',
    'en-us': 'en-US',
    ja: 'ja-JP',
    'ja-jp': 'ja-JP',
    id: 'id-ID',
    'id-id': 'id-ID',
    es: 'es-MX',
    'es-mx': 'es-MX'
  };

  return map[lower] || raw;
}

function sendWsFrame(ws, frame) {
  return new Promise((resolve, reject) => {
    if (ws.readyState !== WebSocket.OPEN) {
      return reject(new Error(`WebSocket 未打开，state=${ws.readyState}`));
    }

    ws.send(frame, { binary: true }, (err) => {
      if (err) return reject(err);
      resolve();
    });
  });
}

async function runSeedAsr(pcmRaw, options = {}) {
  const connectId = randomUUID();
  const wsStartedAt = Date.now();
  let responseLogId = '';

  const headers = upstreamAuthMode === 'app_access'
    ? {
        'X-Api-App-Key': VOLC_APP_KEY,
        'X-Api-Access-Key': VOLC_ACCESS_KEY,
        'X-Api-Resource-Id': VOLC_RESOURCE_ID,
        'X-Api-Connect-Id': connectId
      }
    : {
        // 兼容旧 server.js 的鉴权头，仅用于快速验证；SeedASR 2.0 官方接入优先使用上面的 App/Access 方式。
        'X-Api-Key': VOLC_API_KEY,
        'X-Api-Resource-Id': VOLC_RESOURCE_ID,
        'X-Api-Request-Id': connectId
      };

  log('info', 'transcription.upstream.connecting', {
    requestId: options.requestId,
    connectId,
    wsUrl: VOLC_WS_URL,
    resourceId: VOLC_RESOURCE_ID,
    authMode: upstreamAuthMode
  });

  const ws = new WebSocket(VOLC_WS_URL, {
    headers,
    handshakeTimeout: HANDSHAKE_TIMEOUT_MS
  });

  let finalText = '';
  let seenLast = false;
  let seq = 1;

  const completion = new Promise((resolve, reject) => {
    let settled = false;

    const timer = setTimeout(() => {
      rejectOnce(new Error(`SeedASR 请求超时（${REQUEST_TIMEOUT_MS}ms）`));
      try { ws.close(); } catch (_) {}
    }, REQUEST_TIMEOUT_MS);
    timer.unref();

    const cleanup = () => {
      clearTimeout(timer);
      ws.off('message', onMessage);
      ws.off('error', onError);
      ws.off('close', onClose);
    };

    const resolveOnce = (value) => {
      if (settled) return;
      settled = true;
      cleanup();
      resolve(value);
    };

    const rejectOnce = (err) => {
      if (settled) return;
      settled = true;
      cleanup();
      reject(err);
    };

    const onMessage = (raw) => {
      try {
        const parsed = parseServerFrame(raw);

        if (parsed.messageType === MSG_TYPE.SERVER_ERROR_RESPONSE) {
          const err = new Error(`SeedASR 协议错误 code=${parsed.errorCode}`);
          err.errorCode = parsed.errorCode;
          err.detail = parsed.error;
          err.connectId = connectId;
          err.logid = responseLogId;
          rejectOnce(err);
          try { ws.close(); } catch (_) {}
          return;
        }

        if (parsed.messageType !== MSG_TYPE.SERVER_FULL_RESPONSE) return;

        const text = extractResultText(parsed.payload);
        if (text) {
          finalText = text;
          log('debug', 'transcription.partial.updated', {
            requestId: options.requestId,
            connectId,
            textLength: finalText.length,
            sequence: parsed.sequence,
            isLast: parsed.isLast
          });
        }

        if (parsed.isLast) {
          seenLast = true;
          resolveOnce({ text: finalText, connectId, logid: responseLogId });
          try { ws.close(); } catch (_) {}
        }
      } catch (err) {
        rejectOnce(err);
        try { ws.close(); } catch (_) {}
      }
    };

    const onError = (err) => rejectOnce(err);

    const onClose = (code, reasonBuf) => {
      const reason = Buffer.isBuffer(reasonBuf) ? reasonBuf.toString('utf8') : String(reasonBuf || '');
      if (!seenLast) {
        const err = new Error(`SeedASR WebSocket 在最终结果前关闭 code=${code} reason=${reason}`);
        err.connectId = connectId;
        err.logid = responseLogId;
        rejectOnce(err);
      }
    };

    ws.on('message', onMessage);
    ws.on('error', onError);
    ws.on('close', onClose);
  });

  // 防止在 await open 阶段 completion 先 reject 而产生未处理 rejection。
  completion.catch(() => {});

  ws.on('upgrade', (response) => {
    responseLogId = String(response.headers['x-tt-logid'] || '');
    log('info', 'transcription.upstream.connected', {
      requestId: options.requestId,
      connectId,
      logid: responseLogId,
      connectDurationMs: Date.now() - wsStartedAt
    });
  });

  await new Promise((resolve, reject) => {
    const cleanupOpen = () => {
      ws.off('open', onOpen);
      ws.off('error', onOpenError);
      ws.off('unexpected-response', onOpenUnexpectedResponse);
    };
    const onOpen = () => {
      cleanupOpen();
      resolve();
    };
    const onOpenError = (err) => {
      cleanupOpen();
      reject(err);
    };
    const onOpenUnexpectedResponse = (_request, response) => {
      const statusCode = response.statusCode;
      const statusMessage = response.statusMessage || '';
      const err = new Error(`SeedASR WebSocket 握手失败 HTTP ${statusCode} ${statusMessage}`.trim());
      err.connectId = connectId;
      err.logid = String(response.headers['x-tt-logid'] || '');
      response.resume();
      cleanupOpen();
      reject(err);
    };
    ws.once('open', onOpen);
    ws.once('error', onOpenError);
    ws.once('unexpected-response', onOpenUnexpectedResponse);
  });

  const normalizedLanguage = isNostreamEndpoint()
    ? normalizeSeedAsrLanguage(options.language)
    : '';

  const audioPayload = {
    format: 'pcm',
    codec: 'raw',
    rate: 16000,
    bits: 16,
    channel: 1
  };

  if (normalizedLanguage) audioPayload.language = normalizedLanguage;

  const fullPayload = {
    user: {
      uid: options.uid || 'vexa-seedasr-proxy'
    },
    audio: audioPayload,
    request: {
      model_name: MODEL_NAME,
      enable_punc: true,
      enable_itn: true,
      enable_ddc: true,
      show_utterances: SHOW_UTTERANCES,
      result_type: RESULT_TYPE,
      ...(options.prompt ? { corpus: { context: options.prompt } } : {})
    }
  };

  log('debug', 'transcription.upstream.full_request', {
    requestId: options.requestId,
    connectId,
    languageRequested: options.language || '',
    languageSent: normalizedLanguage || '(auto)',
    pcmBytes: pcmRaw.length
  });

  await sendWsFrame(ws, buildFullClientRequest(seq, fullPayload));
  seq += 1;

  // 16kHz * 16bit mono = 32 bytes/ms；默认 200ms = 6400 bytes/包。
  const bytesPerMs = (16000 * 2) / 1000;
  const chunkSize = Math.max(1, Math.floor(bytesPerMs * SEGMENT_DURATION_MS));
  let chunkCount = 0;

  if (pcmRaw.length === 0) {
    await sendWsFrame(ws, buildAudioOnlyRequest(seq, Buffer.alloc(0), true));
    chunkCount = 1;
  } else {
    for (let offset = 0; offset < pcmRaw.length; offset += chunkSize) {
      const end = Math.min(offset + chunkSize, pcmRaw.length);
      const chunk = pcmRaw.subarray(offset, end);
      const isLast = end >= pcmRaw.length;

      await sendWsFrame(ws, buildAudioOnlyRequest(seq, chunk, isLast));
      chunkCount += 1;

      if (!isLast) seq += 1;
      if (!isLast && SEND_INTERVAL_MS > 0) await sleep(SEND_INTERVAL_MS);
    }
  }

  log('debug', 'transcription.upstream.audio.sent', {
    requestId: options.requestId,
    connectId,
    chunkCount,
    chunkSize,
    segmentDurationMs: SEGMENT_DURATION_MS,
    sendIntervalMs: SEND_INTERVAL_MS
  });

  const result = await completion;

  log('info', 'transcription.upstream.completed', {
    requestId: options.requestId,
    connectId,
    logid: responseLogId,
    textLength: result.text.length,
    totalDurationMs: Date.now() - wsStartedAt
  });

  return result;
}

function sendError(res, requestId, statusCode, message, extra = {}) {
  if (res.headersSent) return;
  log('error', 'transcription.failed', {
    requestId,
    statusCode,
    error: message,
    ...extra
  });
  res.status(statusCode).json({ error: message });
}

// OpenAI-compatible: POST /v1/audio/transcriptions
app.post('/v1/audio/transcriptions', upload.single('file'), async (req, res) => {
  const requestId = req.requestId || buildRequestId();
  const startedAt = Date.now();

  if (!assertProxyAuth(req)) {
    return sendError(res, requestId, 401, '代理鉴权失败');
  }

  try {
    if (!req.file) {
      return sendError(res, requestId, 400, '缺少 file 参数');
    }

    const audioBuffer = req.file.buffer || Buffer.alloc(0);
    if (!audioBuffer.length) {
      return sendError(res, requestId, 400, '音频为空，无法执行识别');
    }

    log('info', 'transcription.started', {
      requestId,
      ...getRequestMeta(req),
      fileName: req.file.originalname || '',
      mimeType: req.file.mimetype || '',
      inputBytes: audioBuffer.length,
      model: req.body?.model || '',
      language: req.body?.language || '',
      responseFormat: req.body?.response_format || 'json'
    });

    const transcodeStartedAt = Date.now();
    const pcmRaw = await transcodeToPcm16kMono(audioBuffer);

    if (!pcmRaw.length) {
      return sendError(res, requestId, 400, '音频转 PCM 后为空，无法执行识别');
    }

    log('info', 'transcription.transcode.completed', {
      requestId,
      inputBytes: audioBuffer.length,
      outputBytes: pcmRaw.length,
      durationMs: Date.now() - transcodeStartedAt
    });

    const result = await runSeedAsr(pcmRaw, {
      requestId,
      language: req.body?.language || '',
      prompt: req.body?.prompt || ''
    });

    const text = result.text || '';
    const responseFormat = String(req.body?.response_format || 'json').toLowerCase();

    log('info', 'transcription.response.sent', {
      requestId,
      textLength: text.length,
      connectId: result.connectId,
      logid: result.logid,
      totalDurationMs: Date.now() - startedAt
    });

    if (responseFormat === 'text') {
      res.type('text/plain').send(text);
      return;
    }

    // Vexa v0.12.26 即使请求 verbose_json，也能在无 segments 时读取 data.text。
    res.json({ text });
  } catch (err) {
    const message = err && err.message ? err.message : String(err);
    const statusCode = err && (err.errorCode || err.connectId || /SeedASR|WebSocket/.test(message)) ? 502 : 500;

    return sendError(res, requestId, statusCode, message, {
      errorCode: err?.errorCode || null,
      connectId: err?.connectId || null,
      logid: err?.logid || null,
      detail: err?.detail || null,
      totalDurationMs: Date.now() - startedAt
    });
  }
});

// multer 自身的文件过大等错误统一转 JSON。
app.use((err, req, res, next) => {
  if (!err) return next();
  const requestId = req.requestId || buildRequestId();
  if (err.code === 'LIMIT_FILE_SIZE') {
    return sendError(res, requestId, 413, '上传音频超过 MAX_UPLOAD_BYTES 限制');
  }
  return sendError(res, requestId, 500, err.message || String(err));
});

app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    model: MODEL_NAME,
    resource_id: VOLC_RESOURCE_ID,
    protocol: 'seedasr-v3-binary',
    ws_url: VOLC_WS_URL,
    auth_configured: upstreamAuthMode !== 'missing',
    auth_mode: upstreamAuthMode
  });
});

app.listen(PORT, () => {
  log('info', 'server.started', {
    port: PORT,
    modelName: MODEL_NAME,
    resourceId: VOLC_RESOURCE_ID,
    corsEnabled: CORS_ENABLE,
    wsUrl: VOLC_WS_URL,
    logLevel: normalizedLogLevel,
    segmentDurationMs: SEGMENT_DURATION_MS,
    sendIntervalMs: SEND_INTERVAL_MS,
    protocol: 'seedasr-v3-binary',
    authMode: upstreamAuthMode
  });
});
