const MAX_UPSERT_ATTEMPTS = 3
const RETRY_DELAYS_MS = [250, 750]
const ORDER_NOTIFY_COOLDOWN_MS = 60 * 1000

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms)
  })
}

export function toOrderEventRow(message) {
  const o = message.order ?? message
  return {
    id: Number(o.id),
    side: o.side == "NB" ? "buy" : "sell",
    account_no: String(o.accountNo ?? "").trim(),
    symbol: String(o.symbol ?? "").trim().toUpperCase(),
    order_type: String(o.orderType ?? "").trim(),
    price: Number(o.price),
    avg_price: Number(o.averagePrice),
    quantity: Math.trunc(Number(o.quantity)),
    fill_quantity: Math.trunc(Number(o.fillQuantity ?? 0)),
    canceled_quantity: Math.trunc(Number(o.canceledQuantity ?? 0)),
    leave_quantity: Math.trunc(Number(o.leaveQuantity ?? 0)),
    order_status: String(o.orderStatus ?? "").trim(),
    loan_package_id: o.loanPackageId != null ? Number(o.loanPackageId) : null,
    modified_date: o.modifiedDate,
  }
}

export function createOrderSink({ supabaseUrl, serviceRoleKey, logger, notify }) {
  let lastNotifiedAt = 0

  async function upsertOrderEvent(message) {
    let row

    try {
      row = toOrderEventRow(message)
    } catch (error) {
      logger.error("order_sink.invalid_event", error)
      return false
    }

    if (!row.id || !row.symbol) {
      logger.error("order_sink.invalid_event_identity", {
        id: row.id,
        symbol: row.symbol,
        keys: Object.keys(message).join(","),
      })
      return false
    }

    for (let attempt = 1; attempt <= MAX_UPSERT_ATTEMPTS; attempt += 1) {
      try {
        const response = await fetch(
          `${supabaseUrl}/rest/v1/dnse_order_events`,
          {
            method: "POST",
            headers: {
              apikey: serviceRoleKey,
              Authorization: `Bearer ${serviceRoleKey}`,
              "Content-Type": "application/json",
              Prefer: "return=minimal",
            },
            body: JSON.stringify(row),
          },
        )

        if (response.ok) {
          logger.debug("order_sink.upsert_success", {
            id: row.id,
            symbol: row.symbol,
            order_status: row.order_status,
          })
          return true
        }

        const body = await response.text()
        if (attempt === MAX_UPSERT_ATTEMPTS) {
          logger.error("order_sink.drop_event", {
            attempt,
            id: row.id,
            symbol: row.symbol,
            status: response.status,
            body,
          })
          if (Date.now() - lastNotifiedAt > ORDER_NOTIFY_COOLDOWN_MS) {
            lastNotifiedAt = Date.now()
            void notify(`[DNSE] Order upsert failed — #${row.id} ${row.symbol} (${response.status})`)
          }
          return false
        }

        logger.warn("order_sink.retry_event", {
          attempt,
          id: row.id,
          symbol: row.symbol,
          status: response.status,
          body,
        })
      } catch (error) {
        if (attempt === MAX_UPSERT_ATTEMPTS) {
          logger.error("order_sink.drop_event", {
            attempt,
            id: row.id,
            symbol: row.symbol,
            error_name: error instanceof Error ? error.name : "Error",
            error_message:
              error instanceof Error ? error.message : "Unknown order sink error",
          })
          if (Date.now() - lastNotifiedAt > ORDER_NOTIFY_COOLDOWN_MS) {
            lastNotifiedAt = Date.now()
            void notify(`[DNSE] Order upsert failed — #${row.id} ${row.symbol} (${error instanceof Error ? error.message : "error"})`)
          }
          return false
        }

        logger.warn("order_sink.retry_event", {
          attempt,
          id: row.id,
          symbol: row.symbol,
          error_message:
            error instanceof Error ? error.message : "Unknown order sink error",
        })
      }

      await sleep(RETRY_DELAYS_MS[attempt - 1] ?? RETRY_DELAYS_MS.at(-1) ?? 1000)
    }

    return false
  }

  return {
    upsertOrderEvent,
  }
}
