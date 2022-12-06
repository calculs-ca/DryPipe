import React from "react"
import { createRoot } from 'react-dom/client'
import 'regenerator-runtime/runtime'

import App from "./App"

const mountNode = document.getElementById("app")
createRoot(mountNode).render(<App/>)

