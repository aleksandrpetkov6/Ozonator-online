import { safeStorage } from 'electron'
import { existsSync, readFileSync, writeFileSync, unlinkSync } from 'fs'
import { z } from 'zod'
import type { Secrets } from '../types'
import { ensurePersistentStorageReady, getPersistentSecretsPath } from './paths'

const SecretsFileSchema = z.object({
  clientIdEncB64: z.string(),
  apiKeyEncB64: z.string(),
  // не секрет, поэтому хранится в открытом виде
  storeName: z.string().optional().nullable(),
})

type SecretsFile = z.infer<typeof SecretsFileSchema>

function secretsPath() {
  return getPersistentSecretsPath()
}

function ensureSecretsReady() {
  ensurePersistentStorageReady()
}

function writeSecretsFile(payload: SecretsFile) {
  ensurePersistentStorageReady()
  writeFileSync(secretsPath(), JSON.stringify(payload, null, 2), 'utf-8')
}

export function hasSecrets(): boolean {
  ensureSecretsReady()
  return existsSync(secretsPath())
}

export function saveSecrets(secrets: Secrets) {
  if (!safeStorage.isEncryptionAvailable()) {
    throw Object.assign(new Error('Шифрование safeStorage недоступно на этой машине. Нельзя безопасно сохранить ключи.'), {
      details: { code: 'ENCRYPTION_UNAVAILABLE' },
    })
  }

  const clientIdEnc = safeStorage.encryptString(secrets.clientId)
  const apiKeyEnc = safeStorage.encryptString(secrets.apiKey)

  // При сохранении ключей всегда сбрасываем storeName.
  // После успешной проверки доступа/синхронизации мы подтянем имя заново.
  const payload: SecretsFile = {
    clientIdEncB64: clientIdEnc.toString('base64'),
    apiKeyEncB64: apiKeyEnc.toString('base64'),
    storeName: null,
  }

  writeSecretsFile(payload)
}

export function loadSecrets(): Secrets {
  ensureSecretsReady()

  if (!existsSync(secretsPath())) {
    throw Object.assign(new Error('Ключи не сохранены. Откройте Настройки → Магазин и сохраните Client-Id и Api-Key.'), {
      details: { code: 'NO_SECRETS' },
    })
  }
  const raw = readFileSync(secretsPath(), 'utf-8')
  const parsed = SecretsFileSchema.parse(JSON.parse(raw))

  if (!safeStorage.isEncryptionAvailable()) {
    throw Object.assign(new Error('Шифрование safeStorage недоступно на этой машине. Нельзя расшифровать ключи.'), {
      details: { code: 'ENCRYPTION_UNAVAILABLE' },
    })
  }

  const clientId = safeStorage.decryptString(Buffer.from(parsed.clientIdEncB64, 'base64'))
  const apiKey = safeStorage.decryptString(Buffer.from(parsed.apiKeyEncB64, 'base64'))
  return { clientId, apiKey, storeName: parsed.storeName ?? null }
}

export function updateStoreName(storeName: string | null) {
  ensureSecretsReady()
  if (!existsSync(secretsPath())) return

  try {
    const raw = readFileSync(secretsPath(), 'utf-8')
    const parsed = SecretsFileSchema.parse(JSON.parse(raw))
    writeSecretsFile({ ...parsed, storeName: (storeName ?? null) })
  } catch {
    // не критично
  }
}

export function deleteSecrets() {
  ensureSecretsReady()
  if (existsSync(secretsPath())) {
    unlinkSync(secretsPath())
  }
}
