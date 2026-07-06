function normalizeMeta(meta) {
  if (!meta) {
    return {}
  }

  if (meta instanceof Error) {
    return {
      error_name: meta.name,
      error_message: meta.message,
      error_stack: meta.stack,
    }
  }

  if (typeof meta === "object") {
    return meta
  }

  return { value: meta }
}

function write(level, event, context, meta) {
  const entry = {
    ts: new Date().toISOString(),
    level,
    event,
    ...context,
    ...normalizeMeta(meta),
  }
  const line = JSON.stringify(entry)

  if (level === "error") {
    console.error(line)
    return
  }

  console.log(line)
}

export function createLogger(context = {}) {
  return {
    child(childContext = {}) {
      return createLogger({ ...context, ...childContext })
    },
    debug(event, meta) {
      write("debug", event, context, meta)
    },
    info(event, meta) {
      write("info", event, context, meta)
    },
    warn(event, meta) {
      write("warn", event, context, meta)
    },
    error(event, meta) {
      write("error", event, context, meta)
    },
  }
}
