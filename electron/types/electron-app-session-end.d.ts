import 'electron'

declare module 'electron' {
  interface App {
    on(event: 'session-end', listener: () => void): this
    once(event: 'session-end', listener: () => void): this
    addListener(event: 'session-end', listener: () => void): this
    removeListener(event: 'session-end', listener: () => void): this
  }
}
