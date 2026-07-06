function normalizeSymbols(symbols) {
  return [...new Set(symbols.map((symbol) => String(symbol).trim().toUpperCase()))]
    .filter(Boolean)
    .sort()
}

export class SubscriptionRegistry {
  constructor() {
    this.desiredSymbols = new Set()
  }

  replaceDesired(symbols) {
    const nextSymbols = normalizeSymbols(symbols)
    const nextSet = new Set(nextSymbols)
    const add = nextSymbols.filter((symbol) => !this.desiredSymbols.has(symbol))
    const remove = [...this.desiredSymbols].filter((symbol) => !nextSet.has(symbol))

    this.desiredSymbols = nextSet

    return {
      desired: nextSymbols,
      add,
      remove,
    }
  }

  getDesiredSymbols() {
    return [...this.desiredSymbols].sort()
  }
}
