import ReactDOM from 'react-dom/client'
import { HashRouter } from 'react-router-dom'
import App from './ui/App'
import './ui/styles.css'

// В dev-режиме React.StrictMode намеренно монтирует компоненты дважды,
// из-за чего эффекты (например автосинхронизация) могут «дёргаться»/повторяться.
// Для Electron-приложения это мешает — поэтому рендерим без StrictMode.
ReactDOM.createRoot(document.getElementById('root')!).render(
  <HashRouter>
    <App />
  </HashRouter>
)
