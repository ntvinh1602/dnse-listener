import process from "node:process"

import { createLogger } from "./log.js"
import { createOrderSink } from "./order-sink.js"
import { createIntradaySink } from "./m1-ohlc-sink.js"
import { SubscriptionRegistry } from "./subscriptions.js"
import { fetchActiveSymbols } from "./symbols.js"
import { createDnseWsClient } from "./ws.js"
import { createTelegramNotifier } from "./telegram.js"

function requireEnv(name) {
  const value = process.env[name]
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`)
  }

  return value
}

function parsePositiveInteger(name, value, fallback) {
  const raw = value ?? fallback
  const parsed = Number.parseInt(String(raw), 10)

  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`Invalid ${name} value: ${raw}`)
  }

  return parsed
}

function loadConfig() {
  return {
    dnseApiKey: requireEnv("DNSE_API_KEY"),
    dnseApiSecret: requireEnv("DNSE_API_SECRET"),
    supabaseUrl: requireEnv("SUPABASE_URL").replace(/\/$/, ""),
    serviceRoleKey: requireEnv("SUPABASE_SERVICE_ROLE_KEY"),
    resolution: String(process.env.OHLC_RESOLUTION ?? "1").trim(),
    refreshMs: parsePositiveInteger(
      "REFRESH_MS",
      process.env.REFRESH_MS,
      "300000",
    ),
    heartbeatMs: parsePositiveInteger(
      "HEARTBEAT_MS",
      process.env.HEARTBEAT_MS,
      "25000",
    ),
    telegramBotToken: process.env.TELEGRAM_BOT_TOKEN ?? "",
    telegramChatId: process.env.TELEGRAM_CHAT_ID ?? "",
  }
}

const config = loadConfig()
const logger = createLogger({ service: "dnse-listener" })
const registry = new SubscriptionRegistry()
const telegram = createTelegramNotifier({
  botToken: config.telegramBotToken,
  chatId: config.telegramChatId,
  logger: logger.child({ component: "telegram" }),
})
const sink = createIntradaySink({
  supabaseUrl: config.supabaseUrl,
  serviceRoleKey: config.serviceRoleKey,
  logger: logger.child({ component: "sink" }),
  notify: telegram.send,
})

const orderSink = createOrderSink({
  supabaseUrl: config.supabaseUrl,
  serviceRoleKey: config.serviceRoleKey,
  logger: logger.child({ component: "order_sink" }),
  notify: telegram.send,
})

const channels = [
  { name: `ohlc_closed.${config.resolution}.json` },
  { name: "order.STOCK.json" },
]

let refreshTimer = null
let shuttingDown = false

const wsClient = createDnseWsClient({
  apiKey: config.dnseApiKey,
  apiSecret: config.dnseApiSecret,
  channels,
  heartbeatMs: config.heartbeatMs,
  logger: logger.child({ component: "ws" }),
  onMessage(message) {
    if (message.T === "bc") {
      void sink.upsertIntradayClose(message)
      return
    }

    if (message.T === "b") {
      return
    }

    void orderSink.upsertOrderEvent(message)
  },
  onReady() {
    wsClient.subscribeToChannels()

    const symbols = registry.getDesiredSymbols()
    if (symbols.length === 0) {
      logger.info("subscriptions.idle")
      return
    }

    wsClient.subscribe(symbols)
    logger.info("subscriptions.replayed", {
      symbol_count: symbols.length,
      symbols,
    })
  },
})

async function refreshSymbols(reason) {
  const symbols = await fetchActiveSymbols({
    supabaseUrl: config.supabaseUrl,
    serviceRoleKey: config.serviceRoleKey,
    logger: logger.child({ component: "symbols" }),
  })
  const { desired, add, remove } = registry.replaceDesired(symbols)

  logger.info("subscriptions.reconciled", {
    reason,
    desired_count: desired.length,
    add_count: add.length,
    remove_count: remove.length,
  })

  if (!wsClient.isLive()) {
    return
  }

  if (remove.length > 0) {
    wsClient.unsubscribe(remove)
  }

  if (add.length > 0) {
    wsClient.subscribe(add)
  }
}

function scheduleRefresh(delayMs) {
  if (shuttingDown) {
    return
  }

  refreshTimer = setTimeout(async () => {
    refreshTimer = null

    try {
      await refreshSymbols("interval")
    } catch (error) {
      logger.error("symbols.refresh_failed", error)
    } finally {
      scheduleRefresh(config.refreshMs)
    }
  }, delayMs)
}

async function shutdown(signal) {
  if (shuttingDown) {
    return
  }

  shuttingDown = true
  if (refreshTimer) {
    clearTimeout(refreshTimer)
    refreshTimer = null
  }

  logger.info("app.shutdown", { signal })
  wsClient.stop()
}

async function main() {
  logger.info("app.start", {
    resolution: config.resolution,
    refresh_ms: config.refreshMs,
    heartbeat_ms: config.heartbeatMs,
  })

  try {
    await refreshSymbols("startup")
  } catch (error) {
    logger.error("symbols.initial_fetch_failed", error)
  }

  wsClient.start()
  scheduleRefresh(config.refreshMs)
}

process.on("SIGINT", () => {
  void shutdown("SIGINT")
})

process.on("SIGTERM", () => {
  void shutdown("SIGTERM")
})

void main().catch((error) => {
  logger.error("app.fatal", error)
  process.exitCode = 1
})
