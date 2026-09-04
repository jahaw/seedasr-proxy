require('dotenv').config();
const express = require('express');
const multer = require('multer');
const WebSocket = require('ws');
const ffmpeg = require('fluent-ffmpeg');
const { PassThrough } = require('stream');
const { Buffer } = require('buffer');

const app = express();
const upload = multer();

const PORT = Number(process.env.PORT || 8080);
const VOLC_API_KEY = process.env.VOLC_API_KEY;
const VOLC_WS_URL = process.env.VOLC_WS_URL || 'wss://openspeech.bytedance.com/api/v3/sauc/bigmodel_async';
const VOLC_RESOURCE_ID = process.env.VOLC_RESOURCE_ID || 'volc.seedasr.sauc.duration';
const MODEL_NAME = process.env.MODEL_NAME || 'bigmodel';
const CORS_ENABLE = String(process.env.CORS_ENABLE || 'true').toLowerCase() === 'true';
const LOG_LEVEL = String(process.env.LOG_LEVEL || 'info').toLowerCase();

const LOG_LEVEL_PRIORITY = {
  debug: 10,
  info: 20,
  warn: 30,
  error: 40
};

function normalizeLogLevel(level) {
  if (LOG_LEVEL_PRIORITY[level]) {
    return level;
  }

  return 'info';
}

const normalizedLogLevel = normalizeLogLevel(LOG_LEVEL);
const activeLogLevelPriority = LOG_LEVEL_PRIORITY[normalizedLogLevel];

function redactSensitiveContext(ctx) {
  if (!ctx || typeof ctx !== 'object') {
    return ctx;
  }

  const cloned = { ...ctx };
  if (typeof cloned.apiKey === 'string' && cloned.apiKey) {
    cloned.apiKey = '***';
  }

  return cloned;
}

function log(level, message, context = {}) {
  const priority = LOG_LEVEL_PRIORITY[level] || LOG_LEVEL_PRIORITY.info;
  if (priority < activeLogLevelPriority) {
    return;
  }

  const payload = {
    ts: new Date().toISOString(),
    level,
    message,
    ...redactSensitiveContext(context)
  };

  const line = JSON.stringify(payload);
  if (level === 'error') {
    console.error(line);
    return;
  }

  if (level === 'warn') {
    console.warn(line);
    return;
  }

  console.log(line);
}

if (!VOLC_API_KEY) {
  console.error('错误：必须配置 VOLC_API_KEY');
  process.exit(1);
}

// 兼容浏览器调用的 CORS 头，保证 Vexa 管理端可跨域访问该中转服务。
if (CORS_ENABLE) {
  app.use((req, res, next) => {
    const origin = req.headers.origin || '*';
    res.setHeader('Access-Control-Allow-Origin', origin);
    res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type,Authorization,X-Api-Key,X-Api-Resource-Id,X-Api-Request-Id');

    if (req.method === 'OPTIONS') {
      return res.sendStatus(204);
    }

    next();
  });
}

function buildRequestId() {
  return `seedasr-${Date.now()}-${Math.random().toString(16).slice(2, 10)}`;
}

// 统一抽取请求来源信息，减少各路由重复拼装日志字段。
function getRequestMeta(req) {
  return {
    method: req.method,
    path: req.originalUrl || req.url,
    ip: req.headers['x-forwarded-for'] || req.socket.remoteAddress || '',
    userAgent: req.headers['user-agent'] || ''
  };
}

// 请求级追踪中间件：生成 requestId，贯穿入口、业务处理与出口日志。
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

function extractResultText(rawMessage) {
  try {
    const msg = JSON.parse(rawMessage.toString());
    const payload = msg.payload_msg || msg.payload || msg;
    const result = payload.result || payload;

    if (!result) {
      return '';
    }

    // 火山返回的最终文本通常在 payload_msg.result.text 中，分句结果则在 utterances[].text。
    if (typeof result.text === 'string' && result.text.trim()) {
      return result.text.trim();
    }

    if (Array.isArray(result.utterances)) {
      const texts = result.utterances
        .filter((item) => item && typeof item.text === 'string' && item.text.trim())
        .map((item) => item.text.trim());

      if (texts.length > 0) {
        return texts.join(' ');
      }
    }

    return '';
  } catch (error) {
    // 二进制音频消息或非 JSON 包会忽略，避免中断识别链路。
    return '';
  }
}

// OpenAI兼容接口 POST /v1/audio/transcriptions
app.post('/v1/audio/transcriptions', upload.single('file'), async (req, res) => {
  const requestId = req.requestId || buildRequestId();
  const startedAt = Date.now();

  try {
    if (!req.file) {
      log('warn', 'transcription.file.missing', { requestId, ...getRequestMeta(req) });
      return res.status(400).json({ error: '缺少 file 参数' });
    }

    const audioBuffer = req.file.buffer || Buffer.alloc(0);
    if (!audioBuffer.length) {
      log('warn', 'transcription.audio.empty', {
        requestId,
        ...getRequestMeta(req),
        fileName: req.file.originalname || '',
        mimeType: req.file.mimetype || ''
      });
      return res.status(400).json({ error: '音频为空，无法执行识别' });
    }

    log('info', 'transcription.started', {
      requestId,
      ...getRequestMeta(req),
      fileName: req.file.originalname || '',
      mimeType: req.file.mimetype || '',
      inputBytes: audioBuffer.length
    });

    let finalText = '';
    let responseSent = false;

    const sendResponse = (text) => {
      if (responseSent || res.headersSent) {
        return;
      }

      responseSent = true;
      log('info', 'transcription.response.sent', {
        requestId,
        textLength: text.length,
        totalDurationMs: Date.now() - startedAt
      });
      res.json({ text });
    };

    const sendError = (statusCode, message, errorContext = {}) => {
      if (responseSent || res.headersSent) {
        return;
      }

      responseSent = true;
      log('error', 'transcription.failed', {
        requestId,
        statusCode,
        error: message,
        totalDurationMs: Date.now() - startedAt,
        ...errorContext
      });
      res.status(statusCode).json({ error: message });
    };

    // 1. FFmpeg转码：任意音频 → 16000Hz 16bit 单声道 PCM
    const pcmChunks = [];
    const pass = new PassThrough();
    pass.end(audioBuffer);

    const transcodeStartedAt = Date.now();
    await new Promise((resolve, reject) => {
      ffmpeg(pass)
        .audioFrequency(16000)
        .audioChannels(1)
        .audioCodec('pcm_s16le')
        .format('s16le')
        .on('error', reject)
        .on('end', resolve)
        .pipe()
        .on('data', (buf) => pcmChunks.push(buf));
    });
    const pcmRaw = Buffer.concat(pcmChunks);

    log('info', 'transcription.transcode.completed', {
      requestId,
      inputBytes: audioBuffer.length,
      outputBytes: pcmRaw.length,
      durationMs: Date.now() - transcodeStartedAt
    });

    if (!pcmRaw.length) {
      log('warn', 'transcription.transcode.empty', { requestId });
      return res.status(400).json({ error: '音频转 PCM 后为空，无法执行识别' });
    }

    // 2. 连接火山 SeedASR 2.0 WebSocket
    const upstreamRequestId = buildRequestId();
    const ws = new WebSocket(VOLC_WS_URL, {
      headers: {
        'X-Api-Key': VOLC_API_KEY,
        'X-Api-Resource-Id': VOLC_RESOURCE_ID,
        'X-Api-Request-Id': upstreamRequestId
      }
    });

    const wsStartedAt = Date.now();
    log('info', 'transcription.upstream.connecting', {
      requestId,
      upstreamRequestId,
      wsUrl: VOLC_WS_URL,
      resourceId: VOLC_RESOURCE_ID
    });

    ws.on('open', () => {
      const initPayload = {
        audio: { format: 'pcm', codec: 'raw', rate: 16000, bits: 16, channel: 1 },
        request: {
          model_name: MODEL_NAME,
          enable_punc: true,
          enable_itn: true,
          enable_ddc: true,
          show_utterances: true,
          enable_nonstream: false
        }
      };

      // 保持现有消息帧结构，兼容现有中转逻辑，并补充火山 2.0 所必需的资源 ID / 请求 ID。
      ws.send(JSON.stringify({ type: 'config', data: initPayload }));

      // 分片发送PCM音频（每3200字节一包）
      const chunkSize = 3200;
      const chunkCount = Math.ceil(pcmRaw.length / chunkSize);
      log('info', 'transcription.upstream.connected', {
        requestId,
        upstreamRequestId,
        connectDurationMs: Date.now() - wsStartedAt,
        pcmBytes: pcmRaw.length,
        chunkSize,
        chunkCount
      });

      for (let offset = 0; offset < pcmRaw.length; offset += chunkSize) {
        const slice = pcmRaw.subarray(offset, offset + chunkSize);
        ws.send(slice);
      }

      // 发送结束标记，让服务端完成最终识别和返回。
      ws.send(JSON.stringify({ type: 'finish' }));
      log('debug', 'transcription.upstream.audio.sent', {
        requestId,
        upstreamRequestId,
        chunkCount
      });
    });

    ws.on('message', (raw) => {
      const text = extractResultText(raw);
      if (text) {
        if (text !== finalText) {
          log('debug', 'transcription.partial.updated', {
            requestId,
            upstreamRequestId,
            textLength: text.length
          });
        }

        finalText = text;
      }
    });

    ws.on('close', () => {
      log('info', 'transcription.upstream.closed', {
        requestId,
        upstreamRequestId,
        textLength: finalText.length,
        upstreamDurationMs: Date.now() - wsStartedAt
      });
      sendResponse(finalText);
    });

    ws.on('error', (err) => {
      sendError(500, `火山ASR连接失败: ${err.message}`, {
        upstreamRequestId,
        upstreamDurationMs: Date.now() - wsStartedAt
      });
    });
  } catch (err) {
    log('error', 'transcription.exception', {
      requestId,
      error: err && err.message ? err.message : String(err),
      stack: err && err.stack ? err.stack : ''
    });
    res.status(500).json({ error: err.message });
  }
});

// 健康检查
app.get('/health', (req, res) => {
  res.json({ status: 'ok', model: MODEL_NAME, resource_id: VOLC_RESOURCE_ID });
});

app.listen(PORT, () => {
  log('info', 'server.started', {
    port: PORT,
    modelName: MODEL_NAME,
    resourceId: VOLC_RESOURCE_ID,
    corsEnabled: CORS_ENABLE,
    wsUrl: VOLC_WS_URL,
    logLevel: normalizedLogLevel
  });
});
