import { createHmac, randomUUID } from "crypto"

export class DnseConfigError extends Error {
  constructor(message) {
    super(message)
    this.name = "DnseConfigError"
  }
}

export class DnseApiError extends Error {
  constructor(message, status, code) {
    super(message)
    this.name = "DnseApiError"
    this.status = status
    this.code = code
  }
}

function getRequiredEnv(name) {
  const value = process.env[name]
  if (!value) {
    throw new DnseConfigError(
      `Missing required DNSE environment variable: ${name}`,
    )
  }

  return value
}

function buildSignatureHeader(config, method, path, dateValue) {
  const nonce = randomUUID().replace(/-/g, "")
  const headersList = `(request-target) date`
  const signatureString = [
    `(request-target): ${method.toLowerCase()} ${path}`,
    `date: ${dateValue}`,
    `nonce: ${nonce}`,
  ].join("\n")

  const signature = encodeURIComponent(
    createHmac("sha256", Buffer.from(config.apiSecret, "utf8"))
      .update(signatureString, "utf8")
      .digest("base64"),
  )

  const value = [
    `Signature keyId="${config.apiKey}"`,
    'algorithm="hmac-sha256"',
    `headers="${headersList}"`,
    `signature="${signature}"`,
    `nonce="${nonce}"`,
  ].join(",")

  return value
}

function buildUrl(baseUrl, path, query) {
  const url = new URL(path, baseUrl)

  if (query) {
    for (const [key, value] of Object.entries(query)) {
      if (value !== undefined) {
        url.searchParams.set(key, String(value))
      }
    }
  }

  return url
}

function parseErrorPayload(payload) {
  if (!payload || typeof payload !== "object") {
    return null
  }

  return payload
}

export async function requestDnse(method, path, options = {}) {
  const config = {
    apiKey: getRequiredEnv("DNSE_API_KEY"),
    apiSecret: getRequiredEnv("DNSE_API_SECRET"),
    baseUrl: getRequiredEnv("DNSE_API_BASE_URL"),
    apiVersion: getRequiredEnv("DNSE_API_VERSION"),
  }
  const url = buildUrl(config.baseUrl, path, options.query)
  const dateValue = new Date().toUTCString().replace("GMT", "+0000")
  const signatureHeader = buildSignatureHeader(config, method, path, dateValue)

  const headers = new Headers({
    Accept: "application/json",
    date: dateValue,
    "X-API-Key": config.apiKey,
    "X-Signature": signatureHeader,
    version: config.apiVersion,
  })

  if (options.body !== undefined) {
    headers.set("Content-Type", "application/json")
  }

  if (options.headers) {
    for (const [key, value] of Object.entries(options.headers)) {
      headers.set(key, value)
    }
  }

  let response
  try {
    response = await fetch(url, {
      method,
      headers,
      body:
        options.body !== undefined ? JSON.stringify(options.body) : undefined,
      cache: "no-store",
    })
  } catch (err) {
    const message =
      err instanceof Error ? err.message : "Network request failed"
    throw new DnseApiError(`DNSE network error: ${message}`, 0, "NETWORK_ERROR")
  }

  const text = await response.text()
  const payload = text ? tryParseJson(text) : null

  if (!response.ok) {
    const errorPayload = parseErrorPayload(payload)
    throw new DnseApiError(
      errorPayload?.message ??
        `DNSE request failed with status ${response.status}`,
      response.status,
      errorPayload?.code,
    )
  }

  return payload
}

function tryParseJson(text) {
  try {
    return JSON.parse(text)
  } catch {
    return text
  }
}
