import crypto from "node:crypto"
import WebSocket from "ws"

const DNSE_STREAM_URL = "wss://ws-openapi.dnse.com.vn/v1/stream?encoding=json"
const AUTH_TIMEOUT_MS = 10_000
const WELCOME_TIMEOUT_MS = 10_000

function normalizeSymbols(symbols) {
  return [...new Set(symbols.map((symbol) => String(symbol).trim().toUpperCase()))]
    .filter(Boolean)
    .sort()
}

function toMessageText(raw) {
  if (typeof raw === "string") {
    return raw
  }

  if (raw instanceof Buffer) {
    return raw.toString("utf8")
  }

  if (Array.isArray(raw)) {
    return Buffer.concat(raw).toString("utf8")
  }

  return Buffer.from(raw).toString("utf8")
}

function parseJsonFrame(raw) {
  const text = toMessageText(raw)

  try {
    return JSON.parse(text)
  } catch {
    return null
  }
}

function authMessage(apiKey, apiSecret) {
  const timestamp = Math.floor(Date.now() / 1000)
  const nonce = `${Date.now()}${Math.floor(Math.random() * 1000)
    .toString()
    .padStart(3, "0")}`
  const signature = crypto
    .createHmac("sha256", apiSecret)
    .update(`${apiKey}:${timestamp}:${nonce}`)
    .digest("hex")

  return {
    action: "auth",
    api_key: apiKey,
    signature,
    timestamp,
    nonce,
  }
}

function reconnectDelaySeconds(attempt) {
  return Math.min(2 ** (attempt - 1), 60)
}

export function createDnseWsClient({
  apiKey,
  apiSecret,
  channels,
  heartbeatMs,
  logger,
  onMessage,
  onReady,
  onDisconnect,
}) {
  let socket = null
  let stopped = false
  let isAuthenticated = false
  let welcomeSeen = false
  let reconnectAttempt = 0
  let reconnectTimer = null
  let heartbeatTimer = null
  let authTimer = null
  let welcomeTimer = null
  let lastPongTime = 0
  let reconnectPending = false

  function clearTimers() {
    if (reconnectTimer) {
      clearTimeout(reconnectTimer)
      reconnectTimer = null
    }

    if (heartbeatTimer) {
      clearInterval(heartbeatTimer)
      heartbeatTimer = null
    }

    if (authTimer) {
      clearTimeout(authTimer)
      authTimer = null
    }

    if (welcomeTimer) {
      clearTimeout(welcomeTimer)
      welcomeTimer = null
    }
  }

  function sendJson(payload) {
    if (!socket || socket.readyState !== WebSocket.OPEN) {
      return false
    }

    socket.send(JSON.stringify(payload))
    return true
  }

  function scheduleReconnect(reason) {
    if (stopped || reconnectTimer) {
      return
    }

    reconnectAttempt += 1
    reconnectPending = false
    const delaySeconds = reconnectDelaySeconds(reconnectAttempt)

    logger.warn("ws.reconnect_scheduled", {
      reason,
      attempt: reconnectAttempt,
      delay_seconds: delaySeconds,
    })

    reconnectTimer = setTimeout(() => {
      reconnectTimer = null
      connect()
    }, delaySeconds * 1000)
  }

  function handleDisconnect(reason) {
    if (stopped || reconnectPending) {
      return
    }

    reconnectPending = true
    isAuthenticated = false
    welcomeSeen = false
    clearTimers()

    if (onDisconnect) {
      onDisconnect(reason)
    }

    if (
      socket &&
      socket.readyState !== WebSocket.CLOSED &&
      socket.readyState !== WebSocket.CLOSING
    ) {
      socket.terminate()
      return
    }

    scheduleReconnect(reason)
  }

  function startHeartbeat() {
    if (heartbeatTimer) {
      clearInterval(heartbeatTimer)
    }

    heartbeatTimer = setInterval(() => {
      if (!isLive()) {
        return
      }

      if (Date.now() - lastPongTime > heartbeatMs * 2) {
        logger.warn("ws.stale_pong", {
          elapsed_ms: Date.now() - lastPongTime,
          heartbeat_ms: heartbeatMs,
        })
        handleDisconnect("stale_pong")
        return
      }

      sendJson({ action: "ping" })
    }, heartbeatMs)
  }

  function sendAuth() {
    if (welcomeTimer) {
      clearTimeout(welcomeTimer)
      welcomeTimer = null
    }

    sendJson(authMessage(apiKey, apiSecret))
    authTimer = setTimeout(() => {
      logger.warn("ws.auth_timeout")
      handleDisconnect("auth_timeout")
    }, AUTH_TIMEOUT_MS)
  }

  function connect() {
    if (stopped) {
      return
    }

    logger.info("ws.connecting", {
      url: DNSE_STREAM_URL,
    })

    isAuthenticated = false
    welcomeSeen = false
    reconnectPending = false
    socket = new WebSocket(DNSE_STREAM_URL)

    socket.on("open", () => {
      logger.info("ws.open")
      welcomeTimer = setTimeout(() => {
        logger.warn("ws.welcome_timeout")
        handleDisconnect("welcome_timeout")
      }, WELCOME_TIMEOUT_MS)
    })

    socket.on("message", (raw) => {
      const message = parseJsonFrame(raw)

      if (!message) {
        logger.warn("ws.invalid_frame")
        return
      }

      if (message.action === "ping") {
        sendJson({ action: "pong" })
        return
      }

      if (message.action === "pong") {
        lastPongTime = Date.now()
        return
      }

      if (!welcomeSeen) {
        welcomeSeen = true
        logger.info("ws.welcome", {
          session_id: message.session_id ?? message.sid ?? null,
        })
        sendAuth()
        return
      }

      if (message.action === "auth_success") {
        if (authTimer) {
          clearTimeout(authTimer)
          authTimer = null
        }

        isAuthenticated = true
        reconnectAttempt = 0
        lastPongTime = Date.now()
        logger.info("ws.auth_success")
        startHeartbeat()

        if (onReady) {
          Promise.resolve(onReady()).catch((error) => {
            logger.error("ws.on_ready_failed", error)
          })
        }

        return
      }

      if (message.action === "auth_error" || message.action === "error") {
        logger.error("ws.auth_error", message)
        handleDisconnect("auth_error")
        return
      }

      if (message.action === "subscribed" || message.action === "unsubscribed") {
        logger.info("ws.subscription_ack", message)
        return
      }

      lastPongTime = Date.now()
      if (onMessage) {
        void onMessage(message)
      }
    })

    socket.on("pong", () => {
      lastPongTime = Date.now()
    })

    socket.on("error", (error) => {
      logger.warn("ws.error", {
        error_message: error.message,
      })
      handleDisconnect("socket_error")
    })

    socket.on("close", (code, reasonBuffer) => {
      const reason = reasonBuffer.toString("utf8") || "socket_closed"
      logger.warn("ws.closed", {
        code,
        reason,
      })
      clearTimers()
      socket = null
      isAuthenticated = false
      welcomeSeen = false

      if (stopped) {
        return
      }

      if (onDisconnect) {
        onDisconnect(reason)
      }

      scheduleReconnect(reason)
    })
  }

  function subscribeToChannels() {
    if (!isLive()) {
      return
    }

    for (const channel of channels) {
      logger.info("ws.subscription_send", {
        action: "subscribe",
        channel: channel.name,
      })

      sendJson({
        action: "subscribe",
        channels: [channel],
      })
    }
  }

  function subscribe(symbols) {
    if (!isLive()) {
      return false
    }

    const normalizedSymbols = normalizeSymbols(symbols)
    if (normalizedSymbols.length === 0) {
      return false
    }

    const ohlcChannel = channels.find((ch) => ch.name.startsWith("ohlc_closed"))
    if (!ohlcChannel) {
      return false
    }

    logger.info("ws.subscription_send", {
      action: "subscribe",
      channel: ohlcChannel.name,
      symbols: normalizedSymbols,
    })

    return sendJson({
      action: "subscribe",
      channels: [{ name: ohlcChannel.name, symbols: normalizedSymbols }],
    })
  }

  function unsubscribe(symbols) {
    if (!isLive()) {
      return false
    }

    const normalizedSymbols = normalizeSymbols(symbols)
    if (normalizedSymbols.length === 0) {
      return false
    }

    const ohlcChannel = channels.find((ch) => ch.name.startsWith("ohlc_closed"))
    if (!ohlcChannel) {
      return false
    }

    logger.info("ws.subscription_send", {
      action: "unsubscribe",
      channel: ohlcChannel.name,
      symbols: normalizedSymbols,
    })

    return sendJson({
      action: "unsubscribe",
      channels: [{ name: ohlcChannel.name, symbols: normalizedSymbols }],
    })
  }

  function start() {
    stopped = false
    connect()
  }

  function stop() {
    stopped = true
    clearTimers()

    if (socket) {
      socket.close(1000, "shutdown")
      socket = null
    }
  }

  function isLive() {
    return Boolean(
      socket && socket.readyState === WebSocket.OPEN && isAuthenticated,
    )
  }

  return {
    start,
    stop,
    isLive,
    subscribe,
    unsubscribe,
    subscribeToChannels,
  }
}
