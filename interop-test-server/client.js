import { Repo, isValidAutomergeUrl } from "@automerge/automerge-repo";
import { BrowserWebSocketClientAdapter } from "@automerge/automerge-repo-network-websocket";
import { command, run, string, positional, number, subcommands } from "cmd-ts";
import { next as A } from "@automerge/automerge";
const create = command({
    name: "create",
    args: {
        port: positional({
            type: number,
            displayName: "port",
            description: "The port to connect to",
        }),
    },
    handler: ({ port }) => {
        const repo = new Repo({
            network: [new BrowserWebSocketClientAdapter(`ws://localhost:${port}`)]
        });
        const doc = repo.create();
        doc.change(d => d.foo = "bar");
        console.log(doc.url);
        console.log(A.getHeads(doc.docSync()).join(","));
    }
});
const fetch = command({
    name: "fetch",
    args: {
        port: positional({
            type: number,
            displayName: "port",
            description: "The port to connect to",
        }),
        docUrl: positional({
            type: string,
            displayName: "docUrl",
            description: "The document url to fetch",
        }),
    },
    handler: ({ port, docUrl }) => {
        const repo = new Repo({
            network: [new BrowserWebSocketClientAdapter(`ws://localhost:${port}`)]
        });
        if (isValidAutomergeUrl(docUrl)) {
        }
        else {
            throw new Error("Invalid docUrl");
        }
        const doc = repo.find(docUrl);
        doc.doc().then(doc => console.log(A.getHeads(doc).join(",")));
    }
});
const sendEphemeral = command({
    name: 'send-ephemeral',
    args: {
        port: positional({
            type: number,
            displayName: "port",
            description: "The port to connect to",
        }),
        docUrl: positional({
            type: string,
            displayName: "docUrl",
            description: "The document url to fetch",
        }),
        message: positional({
            type: string,
            displayName: "message",
            description: "The message to send",
        }),
    },
    handler: ({ port, docUrl, message }) => {
        const repo = new Repo({
            network: [new BrowserWebSocketClientAdapter(`ws://localhost:${port}`)]
        });
        if (!isValidAutomergeUrl(docUrl)) {
            throw new Error("Invalid docUrl");
        }
        const doc = repo.find(docUrl);
        doc.whenReady().then(() => {
            doc.broadcast({ message });
            process.exit(0);
        });
    }
});
const receiveEphemeral = command({
    name: 'receive-ephemeral',
    args: {
        port: positional({
            type: number,
            displayName: "port",
            description: "The port to connect to",
        }),
        docUrl: positional({
            type: string,
            displayName: "docUrl",
            description: "The document url to fetch",
        }),
    },
    handler: ({ port, docUrl }) => {
        const repo = new Repo({
            network: [new BrowserWebSocketClientAdapter(`ws://localhost:${port}`)]
        });
        if (!isValidAutomergeUrl(docUrl)) {
            throw new Error("Invalid docUrl");
        }
        const doc = repo.find(docUrl);
        doc.whenReady().then(() => {
            doc.on("ephemeral-message", ({ message }) => {
                if (typeof message === 'object' && "message" in message) {
                    console.log(message.message);
                }
            });
        });
    }
});
const app = subcommands({
    name: "client",
    cmds: { create, fetch, 'send-ephemeral': sendEphemeral, 'receive-ephemeral': receiveEphemeral },
});
run(app, process.argv.slice(2));
