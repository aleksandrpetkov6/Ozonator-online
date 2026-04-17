import type { Secrets } from './types'
import { ozonPostingFboGetCompat } from './ozon'

function uniquePostingNumbers(values: string[]): string[] {
  const out: string[] = []
  const seen = new Set<string>()
  for (const value of values) {
    const postingNumber = String(value ?? '').trim()
    if (!postingNumber || seen.has(postingNumber)) continue
    seen.add(postingNumber)
    out.push(postingNumber)
  }
  return out
}

function chunk<T>(arr: T[], size: number): T[][] {
  const out: T[][] = []
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size))
  return out
}

function extractResult(payload: any): any {
  if (payload?.result && typeof payload.result === 'object') return payload.result
  return payload && typeof payload === 'object' ? payload : null
}

async function fetchOneCompat(secrets: Secrets, postingNumber: string): Promise<any | null> {
  const bodies = [
    { posting_number: postingNumber, with: { financial_data: true, analytics_data: true } },
    { posting_number: postingNumber, with: { financial_data: true } },
    { posting_number: postingNumber },
  ]

  for (const body of bodies) {
    try {
      // ВАЖНО (РД 2.4.4): никаких прямых fetch к Seller API.
      // Все запросы идут через ozonRequest/ozonPost, чтобы заполнялись raw-cache и endpoint registry.
      const payload = await ozonPostingFboGetCompat(secrets, body)
      const result = extractResult(payload)
      if (result) return result
    } catch {
      // пробуем следующий совместимый body
    }
  }

  return null
}

export async function fetchFboPostingDetailsCompat(secrets: Secrets, postingNumbers: string[]): Promise<Map<string, any>> {
  const list = uniquePostingNumbers(postingNumbers)
  if (list.length === 0) return new Map<string, any>()

  const out = new Map<string, any>()
  for (const batch of chunk(list, 10)) {
    const settled = await Promise.allSettled(batch.map(async (postingNumber) => ({
      postingNumber,
      payload: await fetchOneCompat(secrets, postingNumber),
    })))
    for (const result of settled) {
      if (result.status !== 'fulfilled') continue
      const postingNumber = String(result.value?.postingNumber ?? '').trim()
      const payload = result.value?.payload ?? null
      if (!postingNumber || !payload) continue
      out.set(postingNumber, payload)
    }
  }

  return out
}
