function parseSymbols(payload) {
  if (!Array.isArray(payload)) {
    throw new Error("active_stock_tickers RPC did not return an array")
  }

  return [
    ...new Set(
      payload
        .filter((item) => typeof item === "string")
        .map((ticker) => ticker.trim().toUpperCase())
        .filter(Boolean),
    ),
  ].sort()
}

export async function fetchActiveSymbols({
  supabaseUrl,
  serviceRoleKey,
  logger,
  signal,
}) {
  const response = await fetch(
    `${supabaseUrl}/rest/v1/rpc/active_stock_tickers`,
    {
      method: "POST",
      headers: {
        apikey: serviceRoleKey,
        Authorization: `Bearer ${serviceRoleKey}`,
        "Content-Type": "application/json",
      },
      body: "{}",
      signal,
    },
  )

  if (!response.ok) {
    throw new Error(
      `active_stock_tickers failed with ${response.status}: ${await response.text()}`,
    )
  }

  const payload = await response.json()
  const symbols = parseSymbols(payload)

  logger.info("symbols.fetch_success", {
    symbol_count: symbols.length,
    symbols,
  })

  return symbols
}
