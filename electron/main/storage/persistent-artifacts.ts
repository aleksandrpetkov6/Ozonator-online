import { existsSync, readdirSync, rmSync } from 'node:fs'
import { join } from 'node:path'
import { getPersistentRootDir } from './paths'

export type CurrentPersistentArtifactMergeMode = 'replace' | 'csv_append_missing'

export type CurrentPersistentArtifactInput = {
  groupPath: string[]
  slot: string
  content: string
  extension?: string
  suffix?: string
  headers?: string[]
  mergeMode?: CurrentPersistentArtifactMergeMode
  identityHeaders?: string[]
  preserveOtherFiles?: boolean
}

export type CurrentPersistentArtifactSaved = {
  path: string
  slot: string
  fileName: string
  headers: string[]
}

export type SaveCurrentPersistentArtifactsResult = {
  saved: CurrentPersistentArtifactSaved[]
  cleanedLegacyFilesCount: number
}

function normalizeText(value: unknown): string {
  if (typeof value === 'string') return value.trim()
  if (typeof value === 'number' || typeof value === 'boolean') return String(value).trim()
  return ''
}

function cleanupLegacyCsvArtifacts(groupPath: string[]): number {
  const safeGroupPath = Array.isArray(groupPath) ? groupPath.map((part) => normalizeText(part)).filter(Boolean) : []
  const root = join(getPersistentRootDir(), ...safeGroupPath)
  if (!existsSync(root)) return 0

  let removed = 0
  try {
    for (const fileName of readdirSync(root)) {
      const lower = fileName.toLowerCase()
      if (!lower.endsWith('.csv')) continue
      if (!lower.startsWith('current__')) continue
      try {
        rmSync(join(root, fileName), { force: true })
        removed += 1
      } catch {
        // ignore a single locked file and continue cleaning the rest
      }
    }
  } catch {
    return removed
  }
  return removed
}

export function saveCurrentPersistentArtifacts(artifacts: CurrentPersistentArtifactInput[]): SaveCurrentPersistentArtifactsResult {
  const safeArtifacts = Array.isArray(artifacts) ? artifacts : []
  const firstGroupPath = Array.isArray(safeArtifacts[0]?.groupPath) ? safeArtifacts[0].groupPath : []
  const cleanedLegacyFilesCount = cleanupLegacyCsvArtifacts(firstGroupPath)

  // Online mode: Ozon CSV reports are parsed in memory and are not kept on disk.
  // This prevents current__/timestamp CSV files from multiplying between refreshes.
  return { saved: [], cleanedLegacyFilesCount }
}
