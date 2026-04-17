import { app } from 'electron'
import { mkdirSync } from 'fs'
import { dirname, join } from 'path'

const PERSISTENT_ROOT_SEGMENTS = ['Ozonator']
const INSTALL_LOCAL_STORAGE_DIRNAME = 'data'
const LEGACY_INSTALL_SUBDIR_NAMES = new Set([
  'ozonator',
  'resources',
  'app',
  'win-unpacked',
])

function normalizeLastPathSegment(input: string) {
  return input.replace(/[\\\/]+$/, '').split(/[\\\/]/).pop()?.trim().toLowerCase() ?? ''
}

export function getLegacyUserDataDir() {
  return app.getPath('userData')
}

export function getLegacyPersistentRootDir() {
  return join(app.getPath('appData'), ...PERSISTENT_ROOT_SEGMENTS)
}

export function getLifecycleMarkerRootDir() {
  return getLegacyPersistentRootDir()
}

function getInstallRootDir() {
  const exeDir = dirname(app.getPath('exe'))
  const exeDirName = normalizeLastPathSegment(exeDir)

  if (LEGACY_INSTALL_SUBDIR_NAMES.has(exeDirName)) {
    return dirname(exeDir)
  }

  return exeDir
}

export function getPersistentRootDir() {
  if (app.isPackaged) {
    return join(getInstallRootDir(), INSTALL_LOCAL_STORAGE_DIRNAME)
  }
  return getLegacyPersistentRootDir()
}

export function getPersistentSecretsPath() {
  return join(getPersistentRootDir(), 'secrets.json')
}

export function getLifecycleMarkerPath(kind: 'installer' | 'uninstall') {
  return join(getLifecycleMarkerRootDir(), `${kind}.marker`)
}

export function ensurePersistentStorageReady() {
  mkdirSync(getPersistentRootDir(), { recursive: true })
}

export function readPersistentStorageBootstrapState() {
  const root = getPersistentRootDir()
  const secretsPath = join(root, 'secrets.json')

  return {
    root,
    dbPath: null,
    secretsPath,
  }
}
