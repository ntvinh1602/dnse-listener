const TELEGRAM_API = "https://api.telegram.org"

export function createTelegramNotifier({ botToken, chatId, logger }) {
  if (!botToken || !chatId) {
    logger.warn("telegram.missing_config")
    return { send() {} }
  }

  const url = `${TELEGRAM_API}/bot${botToken}/sendMessage`

  async function send(text) {
    try {
      const response = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          chat_id: chatId,
          text,
          parse_mode: "HTML",
        }),
      })

      if (!response.ok) {
        logger.warn("telegram.send_failed", {
          status: response.status,
          body: await response.text(),
        })
      }
    } catch (error) {
      logger.warn("telegram.send_error", {
        error_message: error instanceof Error ? error.message : "Unknown error",
      })
    }
  }

  return { send }
}
