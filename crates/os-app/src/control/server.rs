// Copyright (C) 2026 thacer13
// Copyright (C) 2026 Ammanuel Getenet
// SPDX-License-Identifier: GPL-3.0-or-later
// This file is part of Lemon Studio: https://github.com/ItsAlphaNeon/Lemon-Studio

//! The local socket clients connect to (`--control <where>`): a Unix socket at a path (Linux, macOS), or a TCP port on
//! 127.0.0.1 (Windows, or a number given). One JSON-RPC request per line, one answer per line. Each connection has a
//! thread that reads it; requests go to the UI thread, which answers them on its tick (`App::control_tick`), so they
//! see and change the project as the interface does. Only this machine can connect.

use std::io::{self, BufRead, BufReader, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{
    Arc,
    mpsc::{Receiver, SyncSender, sync_channel},
};
use std::time::{Duration, Instant};

use super::Level;
use super::rpc::Client;

pub const MAX_CLIENTS: usize = 16;
pub const MAX_LINE_BYTES: usize = 1024 * 1024;
const IO_TIMEOUT: Duration = Duration::from_secs(30);
const IDLE_TIMEOUT: Duration = Duration::from_secs(5 * 60);

/// The connection owns its client state. No disconnected clients accumulate in the UI thread.
pub struct Request {
    pub conn: u64,
    pub line: String,
    pub client: Option<Client>,
    pub reply: SyncSender<(Option<String>, Client)>,
}

pub struct Server {
    pub rx: Receiver<Request>,
    pub level: Level,
    pub headless: bool,
    pub at: String,
    pub token: Option<String>,
}

fn new_token() -> io::Result<String> {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).map_err(io::Error::other)?;
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}

/// Starts a bounded local control listener. Existing live Unix sockets are never unlinked.
pub fn start(at: &str, level: Level, wake: fn()) -> io::Result<Server> {
    let (tx, rx) = sync_channel::<Request>(MAX_CLIENTS);
    let active = Arc::new(AtomicUsize::new(0));
    let shown;
    let token = if at.parse::<u16>().is_ok() { Some(new_token()?) } else { None };
    #[cfg(unix)]
    {
        if at.parse::<u16>().is_ok() {
            shown = tcp(at, tx, active, wake)?;
        } else {
            use std::os::unix::{
                fs::{FileTypeExt, PermissionsExt},
                net::{UnixListener, UnixStream},
            };
            if std::fs::symlink_metadata(at).is_ok_and(|m| m.file_type().is_socket()) {
                match UnixStream::connect(at) {
                    Ok(_) => return Err(io::Error::new(io::ErrorKind::AddrInUse, "control socket is already serving")),
                    Err(e) if e.kind() == io::ErrorKind::ConnectionRefused => std::fs::remove_file(at)?,
                    Err(e) => return Err(e),
                }
            }
            let l = UnixListener::bind(at)?;
            std::fs::set_permissions(at, std::fs::Permissions::from_mode(0o600))?;
            shown = at.to_string();
            std::thread::Builder::new().name("control".into()).spawn(move || {
                for (conn, s) in l.incoming().flatten().enumerate() {
                    let Some(permit) = Permit::take(&active) else { continue };
                    if s.set_read_timeout(Some(IO_TIMEOUT)).is_err() || s.set_write_timeout(Some(IO_TIMEOUT)).is_err() {
                        continue;
                    }
                    if let Ok(w) = s.try_clone() {
                        serve(conn as u64 + 1, BufReader::new(s), w, tx.clone(), permit, wake);
                    }
                }
            })?;
        }
    }
    #[cfg(not(unix))]
    {
        shown = tcp(at, tx, active, wake)?;
    }
    Ok(Server {
        rx,
        level,
        headless: false,
        at: shown,
        token,
    })
}

fn tcp(at: &str, tx: SyncSender<Request>, active: Arc<AtomicUsize>, wake: fn()) -> io::Result<String> {
    let port: u16 = at.parse().map_err(|_| io::Error::other(format!("not a port: {at}")))?;
    let l = std::net::TcpListener::bind(("127.0.0.1", port))?;
    let shown = l.local_addr()?.to_string();
    std::thread::Builder::new().name("control".into()).spawn(move || {
        for (conn, s) in l.incoming().flatten().enumerate() {
            let Some(permit) = Permit::take(&active) else { continue };
            if s.set_read_timeout(Some(IO_TIMEOUT)).is_err() || s.set_write_timeout(Some(IO_TIMEOUT)).is_err() {
                continue;
            }
            if let Ok(w) = s.try_clone() {
                serve(conn as u64 + 1, BufReader::new(s), w, tx.clone(), permit, wake);
            }
        }
    })?;
    Ok(shown)
}

struct Permit(Arc<AtomicUsize>);
impl Permit {
    fn take(active: &Arc<AtomicUsize>) -> Option<Self> {
        active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| (n < MAX_CLIENTS).then_some(n + 1))
            .ok()?;
        Some(Self(active.clone()))
    }
}
impl Drop for Permit {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Bound allocation before JSON parsing, even for a peer that never sends a newline.
fn read_line(r: &mut impl BufRead) -> io::Result<Option<String>> {
    let waiting = Instant::now();
    let mut started: Option<Instant> = None;
    let mut bytes = Vec::new();
    loop {
        if started.is_some_and(|s| s.elapsed() >= IO_TIMEOUT) {
            return Err(io::Error::new(io::ErrorKind::TimedOut, "request took too long"));
        }
        let buf = match r.fill_buf() {
            Err(e)
                if bytes.is_empty()
                    && matches!(e.kind(), io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock)
                    && waiting.elapsed() < IDLE_TIMEOUT =>
            {
                continue;
            }
            other => other?,
        };
        if buf.is_empty() {
            return if bytes.is_empty() {
                Ok(None)
            } else {
                Err(io::Error::new(io::ErrorKind::UnexpectedEof, "incomplete request"))
            };
        }
        started.get_or_insert_with(Instant::now);
        let newline = buf.iter().position(|b| *b == b'\n');
        let n = newline.map_or(buf.len(), |n| n + 1);
        if bytes.len() + n > MAX_LINE_BYTES {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "request exceeds 1 MiB"));
        }
        bytes.extend_from_slice(&buf[..n]);
        r.consume(n);
        if newline.is_some() {
            return String::from_utf8(bytes)
                .map(Some)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e));
        }
    }
}

fn serve<R: BufRead + Send + 'static, W: Write + Send + 'static>(
    conn: u64,
    mut r: R,
    mut w: W,
    tx: SyncSender<Request>,
    permit: Permit,
    wake: fn(),
) {
    let _ = std::thread::Builder::new().name(format!("control {conn}")).spawn(move || {
        let _permit = permit;
        let mut client = None;
        while let Ok(Some(line)) = read_line(&mut r) {
            if line.trim().is_empty() {
                continue;
            }
            if serde_json::from_str::<serde_json::Value>(&line).is_err() {
                let _ = writeln!(
                    w,
                    "{}",
                    serde_json::json!({"jsonrpc": "2.0", "id": null, "error": {"code": -32700, "message": "not JSON: closing"}})
                );
                break;
            }
            let (reply, answer) = sync_channel(1);
            if tx
                .try_send(Request {
                    conn,
                    line,
                    client: client.take(),
                    reply,
                })
                .is_err()
            {
                break;
            }
            wake();
            let Ok((a, c)) = answer.recv_timeout(IO_TIMEOUT) else { break };
            client = Some(c);
            if let Some(a) = a {
                if writeln!(w, "{a}").and_then(|_| w.flush()).is_err() {
                    break;
                }
            }
        }
    });
}

impl crate::app::App {
    pub fn listen(&mut self, server: Server) {
        self.rpc_server = Some(server);
        self.state.control.watched = true;
    }

    pub fn control_tick(&mut self) {
        let Some(server) = self.rpc_server.take() else { return };
        // A busy client cannot keep this tick draining forever.
        for _ in 0..MAX_CLIENTS {
            let Ok(req) = server.rx.try_recv() else { break };
            let mut client = req.client.unwrap_or_else(|| Client {
                name: format!("Client {}", req.conn),
                level: server.level,
                saves: server.headless,
                token: server.token.clone(),
            });
            let answer = self.rpc_line(&mut client, &req.line);
            let _ = req.reply.send((answer, client));
        }
        self.rpc_server = Some(server);
    }
}

#[cfg(test)]
mod limits_tests {
    use super::*;

    #[test]
    fn frames_are_bounded_before_a_newline_and_incomplete_frames_are_refused() {
        let mut oversize = io::Cursor::new(vec![b'x'; MAX_LINE_BYTES + 1]);
        assert_eq!(read_line(&mut oversize).unwrap_err().kind(), io::ErrorKind::InvalidData);
        assert!(read_line(&mut io::Cursor::new(b"{}".as_slice())).is_err());
        assert_eq!(read_line(&mut io::Cursor::new(b"{}\n".as_slice())).unwrap(), Some("{}\n".into()));
        let mut maximum = vec![b' '; MAX_LINE_BYTES];
        maximum[MAX_LINE_BYTES - 1] = b'\n';
        assert_eq!(read_line(&mut io::Cursor::new(maximum)).unwrap().unwrap().len(), MAX_LINE_BYTES);
    }

    #[test]
    fn idle_timeouts_are_tolerated_but_partial_frame_timeouts_close_the_connection() {
        struct Scripted(std::collections::VecDeque<Option<Vec<u8>>>);
        impl io::Read for Scripted {
            fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
                unreachable!()
            }
        }
        impl BufRead for Scripted {
            fn fill_buf(&mut self) -> io::Result<&[u8]> {
                if self.0.front() == Some(&None) {
                    self.0.pop_front();
                    return Err(io::Error::new(io::ErrorKind::TimedOut, "socket timeout"));
                }
                Ok(self.0.front().and_then(Option::as_deref).unwrap_or_default())
            }
            fn consume(&mut self, amount: usize) {
                let Some(Some(bytes)) = self.0.front_mut() else { return };
                bytes.drain(..amount);
                if bytes.is_empty() {
                    self.0.pop_front();
                }
            }
        }
        let mut idle = Scripted([None, Some(b"{}\n".to_vec())].into());
        assert_eq!(read_line(&mut idle).unwrap(), Some("{}\n".into()));
        let mut partial = Scripted([Some(b"{".to_vec()), None].into());
        assert_eq!(read_line(&mut partial).unwrap_err().kind(), io::ErrorKind::TimedOut);
    }

    #[test]
    fn connection_slots_return_after_disconnect_or_spawn_failure() {
        let active = Arc::new(AtomicUsize::new(0));
        let mut permits: Vec<_> = (0..MAX_CLIENTS).map(|_| Permit::take(&active).unwrap()).collect();
        assert!(Permit::take(&active).is_none());
        permits.pop();
        assert!(Permit::take(&active).is_some());
        drop(permits);
        assert_eq!(active.load(Ordering::Acquire), 0);
    }
}

/// The control plane's command-line flags: `--control <where>` (listen beside the window), `--serve <where>` (listen
/// without a window), `--control-level read|propose|edit` (what clients may do; edit by default).
#[derive(Debug, Default, PartialEq)]
pub struct Flags {
    pub control: Option<String>,
    pub serve: Option<String>,
    pub level: Option<Level>,
    /// `--control-token <t>`: the token clients give in `hello` (else a port gets a new one each run, a socket path none).
    pub token: Option<String>,
}

impl Flags {
    /// Takes the flags out of `args` (the rest stay: the project to open).
    pub fn take(args: &mut Vec<String>) -> Result<Flags, String> {
        let mut f = Flags::default();
        let mut i = 0;
        while i < args.len() {
            let slot = match args[i].as_str() {
                "--control" => &mut f.control,
                "--serve" => &mut f.serve,
                "--control-token" => &mut f.token,
                "--control-level" => {
                    let v = args.get(i + 1).ok_or("--control-level needs read, propose or edit")?;
                    f.level = Some(Level::parse(v).ok_or_else(|| format!("--control-level: {v} is not read, propose or edit"))?);
                    args.drain(i..i + 2);
                    continue;
                }
                _ => {
                    i += 1;
                    continue;
                }
            };
            let v = args
                .get(i + 1)
                .filter(|v| !v.starts_with("--"))
                .ok_or_else(|| format!("{} needs a value", args[i]))?;
            *slot = Some(v.clone());
            args.drain(i..i + 2);
        }
        if f.control.is_some() && f.serve.is_some() {
            return Err("--control and --serve: one or the other".into());
        }
        Ok(f)
    }
}

impl Server {
    /// Where it listens and, on a port, the token clients give: for the hint bar and the terminal.
    pub fn describe(&self) -> String {
        match &self.token {
            Some(t) => format!("listening at {}, token {t}", self.at),
            None => format!("listening at {}", self.at),
        }
    }
}

/// `--serve`: the app without a window, `project` opened, answering clients until the process is ended.
pub fn serve_headless(at: &str, level: Level, token: Option<String>, project: Option<std::path::PathBuf>) -> ! {
    let mut app = crate::app::App::new(project);
    // The plugins of the last scan, as the window's app has them before its own scan.
    app.load_plugins();
    let mut server = match start(at, level, || {}) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Lemon Studio: could not listen at {at}: {e}");
            std::process::exit(1);
        }
    };
    server.headless = true;
    if token.is_some() {
        server.token = token;
    }
    eprintln!("Lemon Studio: control plane {} ({level:?})", server.describe());
    app.listen(server);
    loop {
        app.tick();
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
}
