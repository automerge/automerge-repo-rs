use std::{io::IsTerminal, path::PathBuf};

use automerge_repo::DocumentId;
use futures::{Stream, StreamExt};
use tokio::{io::AsyncBufReadExt, process::Command};

const INTEROP_SERVER_PATH: &str = "interop-test-server";

pub(super) struct JsWrapper;

impl JsWrapper {
    pub(super) async fn create() -> eyre::Result<Self> {
        ensure_js_deps().await?;
        Ok(Self)
    }

    pub(super) async fn start_server(&self) -> eyre::Result<RunningJsServer> {
        println!(
            "js server: Building and starting JS interop server in {}",
            interop_server_path().display()
        );

        let mut proc = run_in_js_project(
            tokio::process::Command::new("node")
                .args(["server.js", "0"])
                .kill_on_drop(true)
                .env("DEBUG", "WebsocketServer,automerge-repo:*"),
            "js server",
        ).await?;

        // wait for the server to log its port
        let port: u16 = match proc.stdout.as_mut().unwrap().next().await {
            None => return Err(eyre::eyre!("JS server exited before logging port")),
            Some(Err(e)) => return Err(eyre::eyre!("Error reading from JS server stdout: {}", e)),
            Some(Ok(line)) => {
                line
                    .strip_prefix("Listening on port ")
                    .ok_or_else(|| eyre::eyre!("Unexpected output from JS server: {}", line))?
                    .parse()
                    .map_err(|_| eyre::eyre!("unable to parse port from JS server output: {}", line))?
            }
        };

        proc.forward_stdout();

        Ok(RunningJsServer { child: proc.child, port })
    }

    /// Runs `node client.js create <port>` and returns the doc id and heads
    pub(super) async fn create_doc(&self, port: u16) -> eyre::Result<(DocumentId, Vec<automerge::ChangeHash>, JsProcess)> {

        let mut proc = run_in_js_project(
            tokio::process::Command::new("node")
                .args(["client.js", "create", &port.to_string()])
                .env("DEBUG", "WebsocketClient,automerge-repo:*")
                .kill_on_drop(true),
                "js create",
            ).await?;
        
        // Read the first line of output from the child, which will be the doc id
        let line = proc.stdout.as_mut().unwrap().next().await
            .ok_or_else(|| eyre::eyre!("No first line from JS client"))?
            .map_err(|e| eyre::eyre!("Error reading from JS client stdout: {}", e))?;
        let doc_id = parse_doc_url(line).map_err(|e| eyre::eyre!("Error parsing doc id from JS client: {}", e))?;

        // read the second line, which will be the heads of the document
        let line = proc.stdout.as_mut().unwrap().next().await
            .ok_or_else(|| eyre::eyre!("No second line from JS client"))?
            .map_err(|e| eyre::eyre!("Error reading from JS client stdout: {}", e))?;
        let hashes = line
            .split(",")
            .map(|s| s.parse::<automerge::ChangeHash>())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| eyre::eyre!("Error parsing heads from JS client: {}", e))?;

        proc.forward_stdout();

        Ok((doc_id, hashes, proc))
    }

    pub(super) async fn fetch_doc(&self, port: u16, doc_id: DocumentId) -> eyre::Result<Vec<automerge::ChangeHash>> {
        let doc_url = format!("automerge:{}", doc_id);
        let mut proc = run_in_js_project(Command::new("node")
            .args(["client.js", "fetch", &port.to_string(), &doc_url])
            .env("DEBUG", "WebsocketClient,automerge-repo:*")
            .current_dir(interop_server_path()),
            "js fetch",
        ).await?;

        let line = proc.stdout.as_mut().unwrap().next().await
            .ok_or_else(|| eyre::eyre!("No first line from JS client"))?
            .map_err(|e| eyre::eyre!("Error reading from JS client stdout: {}", e))?;
        let hashes = line
            .split(",")
            .map(|s| s.parse::<automerge::ChangeHash>())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| eyre::eyre!("Error parsing heads from JS client: {}", e))?;

        proc.forward_stdout();
        proc.child.kill().await?;

        Ok(hashes)
    }

    pub(super) async fn send_ephemeral_message(&self, port: u16, doc: DocumentId, message: &str) -> eyre::Result<()> {
        let doc_url = format!("automerge:{}", doc);
        let proc = run_in_js_project(Command::new("node")
            .args(["client.js", "send-ephemeral", &port.to_string(), &doc_url, message])
            .env("DEBUG", "WebsocketClient,automerge-repo:*"),
            "js send ephemera",
        ).await?;

        let output = proc.child.wait_with_output().await?;
        if !output.status.success() {
            return Err(eyre::eyre!("JS client exited with status {}", output.status));
        }
        Ok(())
    }

    pub(super) async fn receive_ephemera(&self, port: u16, doc: DocumentId) -> eyre::Result<impl Stream<Item=Result<String, std::io::Error>>> {
        let doc_url = format!("automerge:{}", doc);
        let mut proc = run_in_js_project(
            Command::new("node")
                .args(["client.js", "receive-ephemeral", &port.to_string(), &doc_url])
                .env("DEBUG", "WebsocketClient,automerge-repo:*")
                .kill_on_drop(true),
            "js receive ephemera",
        ).await?;

        Ok(JsEphemera {
            _child: proc.child,
            stdout: proc.stdout.take().unwrap(),
        })
    }

}


pub(super) struct RunningJsServer {
    pub(super) child: tokio::process::Child,
    pub(super) port: u16,
}


async fn ensure_js_deps() -> eyre::Result<()> {
    npm_install().await?;
    npm_build().await?;
    Ok(())
}

async fn npm_install() -> eyre::Result<()>{
    println!("running npm install");
    let mut proc = run_in_js_project(
        tokio::process::Command::new("npm").arg("install"),
        "npm install"
    ).await?;
    let status = proc.child.wait().await?;
    if !status.success() {
        return Err(eyre::eyre!("npm install failed"));
    }
    Ok(())
}

async fn npm_build() -> eyre::Result<()> {
    println!("npm run build");
    let mut proc =
        run_in_js_project(
            tokio::process::Command::new("npm")
                .args(["run", "build"]),
            "npm run build",
        ).await?;
    let status = proc.child.wait().await?;
    if !status.success() {
        return Err(eyre::eyre!("npm run build failed"));
    }
    Ok(())
}

/// Run a command in the interop server directory and forward stderr and stdout
///
/// The main reason to use this is that the rust test runner doesn't hide output which is written
/// directly to stdout/stderr but rather only output written using print! and friends. This means
/// that rather than spawning a subprocess and inheriting stdout and stderr from us we need to pipe
/// the output of the subprocess and spawn a few tasks to log it using println! so that our test
/// output looks normal.
///
/// ## Arguments
///
/// * `cmd` - The command to run
/// * `name` - The name of the command, used in the log messages
async fn run_in_js_project(
    cmd: &mut tokio::process::Command,
    name: &'static str,
) -> eyre::Result<JsProcess> {
    if std::io::stdout().is_terminal() {
        cmd.env("DEBUG_COLORS", "1");
    }
    let mut child = cmd
        .current_dir(interop_server_path())
        .stderr(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()?;

    let mut stderr = tokio::io::BufReader::new(child.stderr.take().unwrap()).lines();
    tokio::spawn(async move {
        while let Ok(Some(line)) = stderr.next_line().await {
            eprintln!("{}: {}", name, line);
        }
    });

    let stdout = tokio::io::BufReader::new(child.stdout.take().unwrap()).lines();
    Ok(JsProcess {
        name,
        child,
        stdout: Some(tokio_stream::wrappers::LinesStream::new(stdout)),
    })
}

fn interop_server_path() -> PathBuf {
    let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    d.push(INTEROP_SERVER_PATH);
    d
}


/// A running child process and a stream of lines from it's output
pub(super) struct JsProcess {
    name: &'static str,
    pub(super) child: tokio::process::Child,
    pub(super) stdout: Option<tokio_stream::wrappers::LinesStream<tokio::io::BufReader<tokio::process::ChildStdout>>>,
}

impl JsProcess {
    fn forward_stdout(&mut self) {
        if let Some(mut stdout) = self.stdout.take() {
            let name = self.name;
            tokio::spawn(async move {
                while let Some(line) = stdout.next().await {
                    println!("{}: {}", name, line.unwrap());
                }
            });
        }
    }
}

fn parse_doc_url(url: String) -> eyre::Result<DocumentId> {
    if let Some((_, doc_id)) = url.split_once(":") {
        Ok(doc_id
            .parse()
            .map_err(|e| eyre::eyre!("Error parsing doc id: {}", e))?)
    } else {
        Err(eyre::eyre!("Error parsing doc id from url: {}", url))
    }
}

pub(super) struct JsEphemera {
    _child: tokio::process::Child,
    stdout: tokio_stream::wrappers::LinesStream<tokio::io::BufReader<tokio::process::ChildStdout>>,
}

impl Stream for JsEphemera {
    type Item = Result<String, std::io::Error>;

    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        // pin stdout then poll it
        let stdout = unsafe { self.map_unchecked_mut(|s| &mut s.stdout) };
        stdout.poll_next(cx)
    }
}
