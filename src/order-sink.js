import { getOrderDetails } from "./dnse-api/trading-api.js"

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
    id: o.id,
    side: o.side == "NB" ? "buy" : "sell",
    account_no: o.accountNo,
    symbol: o.symbol,
    order_type: o.orderType,
    price: o.price,
    avg_price: o.averagePrice,
    quantity: o.quantity,
    fill_quantity: o.fillQuantity,
    canceled_quantity: o.canceledQuantity,
    leave_quantity: o.leaveQuantity,
    order_status: o.orderStatus,
    loan_package_id: o.loanPackageId,
    modified_date: o.modifiedDate,
    tax: null,
    fee: null,
  }
}

export function createOrderSink({
  supabaseUrl,
  serviceRoleKey,
  logger,
  notify,
}) {
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

    if (row.order_status === "Filled") {
      try {
        const details = await getOrderDetails(row.account_no, row.id)
        const fillQty = row.fill_quantity
        const avgPrice = row.avg_price
        row.tax = details.taxRate * fillQty * avgPrice
        row.fee =
          details.feeRate * fillQty * avgPrice +
          (row.side === "sell" ? fillQty * 0.3 : 0)
        logger.debug("order_sink.tax_fee_enriched", {
          id: row.id,
          symbol: row.symbol,
          tax: row.tax,
          fee: row.fee,
        })
      } catch (error) {
        logger.warn("order_sink.tax_fee_fetch_failed", {
          id: row.id,
          symbol: row.symbol,
          error_message:
            error instanceof Error ? error.message : "Unknown error",
        })
      }
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
              "Content-Profile": "ods",
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
            void notify(
              `[DNSE] Order upsert failed — #${row.id} ${row.symbol} (${response.status})`,
            )
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
              error instanceof Error
                ? error.message
                : "Unknown order sink error",
          })
          if (Date.now() - lastNotifiedAt > ORDER_NOTIFY_COOLDOWN_MS) {
            lastNotifiedAt = Date.now()
            void notify(
              `[DNSE] Order upsert failed — #${row.id} ${row.symbol} (${error instanceof Error ? error.message : "error"})`,
            )
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

      await sleep(
        RETRY_DELAYS_MS[attempt - 1] ?? RETRY_DELAYS_MS.at(-1) ?? 1000,
      )
    }

    return false
  }

  return {
    upsertOrderEvent,
  }
}
