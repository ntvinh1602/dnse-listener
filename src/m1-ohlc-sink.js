const MAX_UPSERT_ATTEMPTS = 3
const RETRY_DELAYS_MS = [250, 750]
const OHLC_NOTIFY_COOLDOWN_MS = 30 * 60 * 1000

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms)
  })
}

function requireFiniteNumber(value, fieldName) {
  const parsed = Number(value)

  if (!Number.isFinite(parsed)) {
    throw new Error(`Invalid ${fieldName} value: ${value}`)
  }

  return parsed
}

function toIsoTimestamp(seconds, fieldName) {
  return new Date(requireFiniteNumber(seconds, fieldName) * 1000).toISOString()
}

export function toIntradayRow(message) {
  return {
    symbol: String(message.symbol ?? "").trim().toUpperCase(),
    close: requireFiniteNumber(message.close, "close"),
    volume: Math.trunc(requireFiniteNumber(message.volume, "volume")),
    last_updated: toIsoTimestamp(message.lastUpdated, "lastUpdated"),
  }
}

export function createIntradaySink({ supabaseUrl, serviceRoleKey, logger, notify }) {
  let lastNotifiedAt = 0

  async function upsertIntradayClose(message) {
    let row

    try {
      row = toIntradayRow(message)
    } catch (error) {
      logger.error("sink.invalid_bar", error)
      return false
    }

    if (!row.symbol) {
      logger.error("sink.invalid_bar_identity", {
        symbol: row.symbol,
      })
      return false
    }

    for (let attempt = 1; attempt <= MAX_UPSERT_ATTEMPTS; attempt += 1) {
      try {
        const response = await fetch(
          `${supabaseUrl}/rest/v1/dnse_m1_close`,
          {
            method: "POST",
            headers: {
              apikey: serviceRoleKey,
              Authorization: `Bearer ${serviceRoleKey}`,
              "Content-Type": "application/json",
              Prefer: "resolution=merge-duplicates,return=minimal",
            },
            body: JSON.stringify(row),
          },
        )

        if (response.ok) {
          logger.debug("sink.upsert_success", {
            symbol: row.symbol,
            last_updated: row.last_updated,
          })
          return true
        }

        const body = await response.text()
        if (attempt === MAX_UPSERT_ATTEMPTS) {
          logger.error("sink.drop_bar", {
            attempt,
            symbol: row.symbol,
            last_updated: row.last_updated,
            status: response.status,
            body,
          })
          if (Date.now() - lastNotifiedAt > OHLC_NOTIFY_COOLDOWN_MS) {
            lastNotifiedAt = Date.now()
            void notify(`[DNSE] OHLC upsert failed — ${row.symbol} (${response.status})`)
          }
          return false
        }

        logger.warn("sink.retry_bar", {
          attempt,
          symbol: row.symbol,
          last_updated: row.last_updated,
          status: response.status,
          body,
        })
      } catch (error) {
        if (attempt === MAX_UPSERT_ATTEMPTS) {
          logger.error("sink.drop_bar", {
            attempt,
            symbol: row.symbol,
            last_updated: row.last_updated,
            error_name: error instanceof Error ? error.name : "Error",
            error_message:
              error instanceof Error ? error.message : "Unknown sink error",
          })
          if (Date.now() - lastNotifiedAt > OHLC_NOTIFY_COOLDOWN_MS) {
            lastNotifiedAt = Date.now()
            void notify(`[DNSE] OHLC upsert failed — ${row.symbol} (${error instanceof Error ? error.message : "error"})`)
          }
          return false
        }

        logger.warn("sink.retry_bar", {
          attempt,
          symbol: row.symbol,
          last_updated: row.last_updated,
          error_message:
            error instanceof Error ? error.message : "Unknown sink error",
        })
      }

      await sleep(RETRY_DELAYS_MS[attempt - 1] ?? RETRY_DELAYS_MS.at(-1) ?? 1000)
    }

    return false
  }

  return {
    upsertIntradayClose,
  }
}
