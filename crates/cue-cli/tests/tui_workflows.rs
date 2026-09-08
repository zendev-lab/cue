#![cfg(all(feature = "cue-client", feature = "cue-daemon", feature = "cue-tui"))]

use std::fs::File;
use std::io::{Read as _, Write as _};
use std::os::fd::{AsRawFd as _, FromRawFd as _};
use std::os::unix::process::CommandExt as _;
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

const CUE: &str = env!("CARGO_BIN_EXE_cue");
const CLIENT: &str = env!("CARGO_BIN_EXE_cue-client");
const DAEMON: &str = env!("CARGO_BIN_EXE_cued");

struct Fixture {
    root: PathBuf,
}
impl Fixture {
    fn new() -> Self {
        let root = PathBuf::from("/tmp").join(format!("cue-tui-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        let fixture = Self { root };
        fixture.run(DAEMON, &["start", "--db", "test.db"]);
        fixture
    }
    fn command(&self, binary: &str, args: &[&str]) -> Command {
        let mut command = Command::new(binary);
        command
            .args(args)
            .current_dir(&self.root)
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("HOME", &self.root)
            .env("XDG_DATA_HOME", self.root.join("data"))
            .env("CUE_SOCKET", self.root.join("peer.sock"))
            .env("TERM", "xterm-256color");
        command
    }
    fn run(&self, binary: &str, args: &[&str]) -> Output {
        let child = self
            .command(binary, args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let output = bounded_output(child);
        assert!(output.status.success(), "{args:?}: {output:?}");
        output
    }
    fn file(&self, name: &str, content: &str) {
        std::fs::write(self.root.join(name), content).unwrap();
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let child = self
            .command(DAEMON, &["stop"])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        if bounded_output(child).status.success() {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }
}
fn bounded_output(mut child: Child) -> Output {
    let deadline = Instant::now() + Duration::from_secs(20);
    while child.try_wait().unwrap().is_none() {
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("fixture command exceeded deadline");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    child.wait_with_output().unwrap()
}

/// A real controlling terminal, parsed into cells rather than searching ANSI
/// fragments or input echoes. The child is the installed-shape `cue tui` route.
struct Ui {
    child: Child,
    master: File,
    _slave: File,
    parser: vt100::Parser,
}
impl Ui {
    fn start(fixture: &Fixture) -> Self {
        let mut master = -1;
        let mut slave = -1;
        let mut size = libc::winsize {
            ws_row: 30,
            ws_col: 100,
            ws_xpixel: 0,
            ws_ypixel: 0,
        };
        // SAFETY: valid output pointers; returned descriptors are uniquely owned below.
        assert_eq!(
            unsafe {
                libc::openpty(
                    &mut master,
                    &mut slave,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    &raw mut size,
                )
            },
            0
        );
        // SAFETY: openpty returned owned descriptors; each is wrapped exactly once.
        let (master, slave) = unsafe { (File::from_raw_fd(master), File::from_raw_fd(slave)) };
        let descriptor = slave.as_raw_fd();
        let mut command = fixture.command(CUE, &["tui"]);
        command
            .stdin(slave.try_clone().unwrap())
            .stdout(slave.try_clone().unwrap())
            .stderr(slave.try_clone().unwrap());
        // SAFETY: the post-fork hook only invokes setsid/ioctl; the inherited
        // descriptor stays open through spawn and belongs to this fixture.
        unsafe {
            command.pre_exec(move || {
                if libc::setsid() < 0 || libc::ioctl(descriptor, libc::TIOCSCTTY as _, 0) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let child = command.spawn().unwrap();
        // SAFETY: fcntl updates the valid owned master descriptor's I/O mode.
        assert_ne!(
            unsafe { libc::fcntl(master.as_raw_fd(), libc::F_SETFL, libc::O_NONBLOCK) },
            -1
        );
        Self {
            child,
            master,
            _slave: slave,
            parser: vt100::Parser::new(30, 100, 0),
        }
    }
    fn pump(&mut self) {
        let mut bytes = [0; 16 * 1024];
        loop {
            match self.master.read(&mut bytes) {
                Ok(0) => break,
                Ok(count) => self.parser.process(&bytes[..count]),
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
                    ) =>
                {
                    break;
                }
                Err(error) if error.raw_os_error() == Some(libc::EIO) => break,
                Err(error) => panic!("read test terminal: {error}"),
            }
        }
    }
    fn wait(&mut self, description: &str, predicate: impl Fn(&str) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(8);
        loop {
            self.pump();
            let screen = self.parser.screen().contents();
            if predicate(&screen) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "{description}\nSCREEN:\n{screen}"
            );
            assert!(
                self.child.try_wait().unwrap().is_none(),
                "UI exited before {description}:\n{screen}"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }
    fn keys(&mut self, bytes: &[u8]) {
        self.master.write_all(bytes).unwrap();
    }
    fn paste(&mut self, source: &str) {
        self.keys(b"\x1b[200~");
        self.keys(source.as_bytes());
        self.keys(b"\x1b[201~");
    }
    fn quit(&mut self) {
        self.keys(b"\x03");
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            self.pump();
            if let Some(status) = self.child.try_wait().unwrap() {
                assert!(status.success());
                break;
            }
            assert!(Instant::now() < deadline, "UI did not quit");
            std::thread::sleep(Duration::from_millis(20));
        }
        assert!(
            !self.parser.screen().alternate_screen(),
            "alternate screen was not restored"
        );
        let mut mode = std::mem::MaybeUninit::<libc::termios>::uninit();
        // SAFETY: tcgetattr writes one initialized termios on success.
        assert_eq!(
            unsafe { libc::tcgetattr(self.master.as_raw_fd(), mode.as_mut_ptr()) },
            0
        );
        let mode = unsafe { mode.assume_init() };
        assert_ne!(
            mode.c_lflag & libc::ICANON,
            0,
            "terminal was left in raw mode"
        );
    }
}
impl Drop for Ui {
    fn drop(&mut self) {
        if self.child.try_wait().unwrap().is_none() {
            // SAFETY: spawn created this fixture's isolated session/process group.
            unsafe {
                libc::kill(-(self.child.id() as i32), libc::SIGKILL);
            }
            let _ = self.child.wait();
        }
    }
}

#[test]
fn existing_and_external_executions_have_selectable_live_output() {
    let fixture = Fixture::new();
    fixture.file(
        "out.txt",
        &(0..70)
            .map(|i| format!("CAPTURED-ROW-{i:03}\n"))
            .collect::<String>(),
    );
    fixture.file("err.txt", "ERROR-STREAM-ONLY\n");
    fixture.run(
        CLIENT,
        &[
            "exec",
            ":run(pty=false) /bin/sh -c 'cat out.txt; cat err.txt >&2'",
        ],
    );
    let mut ui = Ui::start(&fixture);
    ui.wait("existing output without any :out command", |screen| {
        screen.contains("CAPTURED-ROW-069") && screen.contains("ERROR-STREAM-ONLY")
    });
    ui.keys(b"\x1bOR3"); // F3 output, Stderr tab.
    ui.wait("stderr tab contains no stdout", |screen| {
        screen.contains("Stderr · following")
            && screen.contains("ERROR-STREAM-ONLY")
            && !screen.contains("CAPTURED-ROW")
    });
    ui.keys(b"2\x1b[H"); // Stdout, Home pauses at the beginning.
    ui.wait("scroll to first retained line", |screen| {
        screen.contains("CAPTURED-ROW-000") && screen.contains("scroll paused")
    });
    ui.keys(b"\x1b[F");
    ui.wait("follow the tail again", |screen| {
        screen.contains("CAPTURED-ROW-069") && screen.contains("following")
    });
    fixture.file("other.txt", "EXTERNAL-CLIENT-OUTPUT\n");
    fixture.run(CLIENT, &["exec", "/bin/cat other.txt"]);
    ui.wait(
        "other client's execution appears without manual refresh",
        |screen| screen.contains("E2") && screen.contains("E1"),
    );
    assert!(
        ui.parser.screen().contents().contains("CAPTURED-ROW-069"),
        "refresh stole the selection"
    );
    ui.keys(b"\x1bOQ\x1b[A\r"); // F2 sidebar, up, Enter opens.
    ui.wait("select external output", |screen| {
        screen.contains("EXTERNAL-CLIENT-OUTPUT") && !screen.contains("CAPTURED-ROW")
    });
    ui.quit();
}

#[test]
fn input_completion_cancel_history_and_reconnect_use_real_cli_paths() {
    let fixture = Fixture::new();
    fixture.file("live.txt", "VISIBLE-BEFORE-EXIT\n");
    let mut ui = Ui::start(&fixture);
    ui.wait("empty workspace", |screen| {
        screen.contains("No executions yet")
    });
    ui.paste(":executi");
    ui.keys(b"\t");
    ui.wait("completion inserts the command", |screen| {
        screen.contains(":executions ")
    });
    ui.keys(b"\x15"); // clear input
    let source = ":run(pty=false) /bin/sh -c 'cat live.txt; sleep 30'";
    ui.paste(source);
    ui.keys(b"\r");
    ui.wait("streaming output before completion", |screen| {
        screen.contains("VISIBLE-BEFORE-EXIT") && screen.contains("Running")
    });
    ui.keys(b"\x1bOQ\x1b[3~"); // F2, Delete => cancel selected execution.
    ui.wait("cancel action completes", |screen| {
        screen.contains("Cancelled")
    });
    ui.keys(b"\x1bOS\x1b[A"); // F4, history up
    ui.wait("history recalls the submitted command", |screen| {
        screen.contains(source)
    });
    ui.keys(b"\x15");
    ui.paste("unsubmitted draft");
    fixture.run(DAEMON, &["stop"]);
    ui.wait("disconnect stays visible", |screen| {
        screen.contains("disconnected") && screen.contains("unsubmitted draft")
    });
    fixture.run(DAEMON, &["start", "--db", "test.db"]);
    ui.wait("reconnect preserves the draft", |screen| {
        screen.contains("· connected") && screen.contains("unsubmitted draft")
    });
    let list = fixture.run(CLIENT, &["list"]);
    let list: serde_json::Value = serde_json::from_slice(&list.stdout).unwrap();
    assert_eq!(
        list["payload"]["executions"].as_array().unwrap().len(),
        1,
        "reconnect resubmitted work"
    );
    ui.quit();
    let history =
        std::fs::read_to_string(fixture.root.join("data/cue/input-history-v4.jsonl")).unwrap();
    assert!(
        history
            .lines()
            .any(|line| serde_json::from_str::<String>(line).unwrap() == source)
    );
}

#[test]
fn pty_passthrough_detaches_back_into_the_workbench() {
    let fixture = Fixture::new();
    fixture.file("terminal.txt", "PTY-ONLY-RESPONSE\n");
    let mut ui = Ui::start(&fixture);
    ui.wait("workspace", |screen| screen.contains("No executions yet"));
    ui.paste(":run(pty=true) /bin/sh");
    ui.keys(b"\r");
    ui.wait("interactive execution", |screen| screen.contains("Running"));
    ui.keys(b"\x1bORf");
    ui.wait("passthrough leaves the alternate screen", |screen| {
        !screen.contains("CUE")
    });
    ui.keys(b"/bin/cat terminal.txt\r");
    ui.wait("PTY input reaches the child", |screen| {
        screen.contains("PTY-ONLY-RESPONSE")
    });
    ui.keys(b"\x1d");
    ui.wait("Ctrl-] returns to the workbench", |screen| {
        screen.contains("CUE") && screen.contains("Executions")
    });
    ui.quit();
}
