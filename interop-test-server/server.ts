import express from "express"
import { WebSocketServer } from "ws"
import { Repo } from "@automerge/automerge-repo"
import { NodeWSServerAdapter } from "@automerge/automerge-repo-network-websocket"

class Server {
  #socket: WebSocketServer

  #server: ReturnType<import("express").Express["listen"]> 

  #repo: Repo

  constructor(port: number) {
    this.#socket = new WebSocketServer({ noServer: true })

    const PORT = port
    const app = express()
    app.use(express.static("public"))

    const config = {
      network: [new NodeWSServerAdapter(this.#socket)],
      /** @ts-ignore @type {(import("automerge-repo").PeerId)}  */
      peerId: `storage-server` as PeerId,
      // Since this is a server, we don't share generously — meaning we only sync documents they already
      // know about and can ask for by ID.
      sharePolicy: async () => false,
    }
    const serverRepo = new Repo(config)
    this.#repo = serverRepo

    app.get("/", (req, res) => {
      res.send(`👍 @automerge/automerge-repo-sync-server is running`)
    })

    this.#server = app.listen(PORT, () => {
      console.log(`Listening on port ${this.#server.address().port}`)
    })

    this.#server.on("upgrade", (request, socket, head) => {
      console.log("upgrading to websocket")
      this.#socket.handleUpgrade(request, socket, head, (socket) => {
        this.#socket.emit("connection", socket, request)
      })
    })
  }

  close() {
    this.#socket.close()
    this.#server.close()
  }
}

const port = process.argv[2] ? parseInt(process.argv[2]) : 8080
const server = new Server(port)

process.on("SIGINT", () => {
    server.close()
    process.exit(0)
})
