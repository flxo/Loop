use anyhow::{Context, Result};
use clap::Parser;
use futures::{
    future::{self, Either},
    stream::{self, StreamExt},
};
use humantime::{parse_duration, parse_rfc3339_weak};
use portable_pty::{native_pty_system, CommandBuilder, PtySize};
use regex::Regex;
use std::{
    io::{BufRead, BufReader},
    time::{Duration, SystemTime},
};
use tokio::{pin, select, sync::mpsc, task, time};
use tokio_util::codec::{FramedRead, LinesCodec};

// Same exit code as the `timeout` shell command.
const TIMEOUT_EXIT_CODE: i32 = 124;

#[cfg(unix)]
const SHELL: [&str; 2] = ["sh", "-c"];
#[cfg(windows)]
const SHELL: [&str; 2] = ["cmd.exe", "/c"];

// Default PTY dimensions when we cannot detect the host terminal size. 80x24
// matches the historic VT100 default; programs use this for line wrapping but
// not much else.
const DEFAULT_PTY_SIZE: PtySize = PtySize {
    rows: 24,
    cols: 80,
    pixel_width: 0,
    pixel_height: 0,
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let mut opt = Opt::parse();

    if opt.input.is_empty() {
        eprintln!("Missing command");
        std::process::exit(1);
    }

    // Overall deadline: --for-duration and/or --until-time, whichever fires first.
    let for_duration = opt
        .for_duration
        .map(time::sleep)
        .map(Either::Left)
        .unwrap_or_else(|| Either::Right(future::pending::<()>()));
    pin!(for_duration);
    let until_time = opt
        .until_time
        .map(|r| {
            r.duration_since(SystemTime::now())
                .context("--until-time is in the past")
        })
        .transpose()?
        .map(time::sleep)
        .map(Either::Left)
        .unwrap_or_else(|| Either::Right(future::pending::<()>()));
    pin!(until_time);
    let mut until = future::select(for_duration, until_time);

    let mut summary = opt.summary.then(Summary::default);

    // Counter exposed via the $COUNT / $ACTUALCOUNT env vars.
    let mut iteration = 0u64;

    // --stdin and --for: source of $ITEM values.
    let mut items = if opt.stdin {
        let stdin = tokio::io::stdin();
        let lines = FramedRead::new(stdin, LinesCodec::new());
        let lines = lines.filter_map(|l| future::ready(l.ok()));
        Some(Either::Left(lines))
    } else if let Some(values) = opt.for_values.as_ref() {
        let iter = values.split(',').map(ToString::to_string);
        Some(Either::Right(stream::iter(iter)))
    } else {
        None
    };

    // --only-last: buffer of the final iteration's output.
    let mut output = opt.only_last.then(Vec::new);

    // Last seen line, needed for --until-same / --until-changes. With a single
    // PTY stream we can no longer distinguish stdout from stderr.
    let mut last_line = None;

    let pty_system = native_pty_system();

    let exit = 'outer: loop {
        // --num: cap total iterations. Checked before spawning so we never
        // start a process that we have no intention of running to completion.
        if let Some(n) = opt.num.as_mut() {
            if *n == 0 {
                break 0;
            }
            *n -= 1;
        }

        let count = opt.offset.unwrap_or_default() + iteration as f64 * opt.count_by;
        let mut builder = CommandBuilder::new(SHELL[0]);
        builder.arg(SHELL[1]);
        builder.arg(opt.input.join(" "));
        // CommandBuilder doesn't inherit the parent's cwd or env; without this
        // the child spawns in $HOME with an empty environment.
        if let Ok(cwd) = std::env::current_dir() {
            builder.cwd(cwd);
        }
        for (k, v) in std::env::vars_os() {
            builder.env(k, v);
        }
        builder.env("COUNT", count.to_string());
        builder.env("ACTUALCOUNT", iteration.to_string());

        // Pull the next item from --for / --stdin, if any.
        if let Some(items) = items.as_mut() {
            match items.next().await {
                Some(f) => builder.env("ITEM", f),
                None => break 0,
            }
        }

        let pair = pty_system
            .openpty(DEFAULT_PTY_SIZE)
            .context("failed to open pty")?;
        let child = pair
            .slave
            .spawn_command(builder)
            .context("failed to spawn command")?;
        // Drop the parent's copy of the slave so EOF propagates on the master
        // once the child exits.
        drop(pair.slave);

        let mut killer = child.clone_killer();
        let reader = pair
            .master
            .try_clone_reader()
            .context("failed to clone pty reader")?;

        // Drain the master in a blocking thread, forwarding lines through a
        // channel. The PTY's line discipline appends \r before \n; strip both.
        let (line_tx, mut line_rx) = mpsc::unbounded_channel::<String>();
        let reader_task = task::spawn_blocking(move || {
            let mut reader = BufReader::new(reader);
            let mut buf = String::new();
            loop {
                buf.clear();
                match reader.read_line(&mut buf) {
                    Ok(0) => break,
                    Ok(_) => {
                        let line = buf.trim_end_matches(['\r', '\n']).to_string();
                        if line_tx.send(line).is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });

        // wait() is blocking; park it on the blocking pool and select on the
        // resulting JoinHandle.
        let mut child = child;
        let mut wait_handle = task::spawn_blocking(move || child.wait());

        let mut output_closed = false;
        let mut running = true;

        if let Some(l) = output.as_mut() {
            l.clear();
        }

        let need_last = opt.until_same || opt.until_changes;

        'inner: loop {
            select! {
                _ = &mut until => {
                    if running {
                        let _ = killer.kill();
                    }
                    if opt.error_duration {
                        break 'outer TIMEOUT_EXIT_CODE;
                    } else {
                        break 'outer 0;
                    }
                }
                line = line_rx.recv() => {
                    let line = match line {
                        Some(l) => l,
                        None => {
                            output_closed = true;
                            // Wait for the child to finish before exiting the inner loop.
                            if !running {
                                break 'inner;
                            } else {
                                continue 'inner;
                            }
                        }
                    };

                    let do_break = {
                        let last = need_last
                            .then(|| last_line.replace(line.clone()))
                            .flatten();
                        check_line(&opt, &line, last.as_deref())
                    };

                    if let Some(output) = output.as_mut() {
                        output.push(line);
                    } else {
                        println!("{line}");
                    }

                    if do_break {
                        if running {
                            let _ = killer.kill();
                        }
                        break 'outer 0;
                    }
                }
                wait_res = &mut wait_handle, if running => {
                    running = false;

                    let status = wait_res
                        .context("wait task panicked")?
                        .context("failed to wait for child")?;
                    let exit_code = status.exit_code() as i32;

                    if let Some(summary) = summary.as_mut() {
                        if status.success() {
                            summary.successes += 1;
                        } else {
                            summary.failures.push(exit_code);
                        }
                    }

                    if opt.until_fail && !status.success() {
                        break 'outer exit_code;
                    }

                    if opt.until_success && status.success() {
                        break 'outer exit_code;
                    }

                    if opt.until_code == Some(exit_code) {
                        break 'outer exit_code;
                    }

                    if output_closed {
                        break 'inner;
                    }
                }
                else => (),
            }
        }

        // Join the reader thread before reusing per-iteration resources.
        let _ = reader_task.await;
        drop(pair.master);

        iteration += 1;

        if let Some(every) = opt.every {
            time::sleep(every).await;
        } else {
            // Yield to keep the runtime responsive in a tight loop.
            tokio::task::yield_now().await;
        }
    };

    if let Some(lines) = output {
        for line in lines {
            println!("{line}");
        }
    }

    if let Some(summary) = summary {
        summary.print();
    }

    std::process::exit(exit);
}

#[derive(Parser, Debug)]
#[command(
    name = "loop",
    author = "Rich Jones <miserlou@gmail.com>",
    version,
    about = "UNIX's missing `loop` command",
    trailing_var_arg = true
)]
struct Opt {
    /// Number of iterations to execute
    #[arg(short = 'n', long = "num")]
    num: Option<u32>,

    /// Amount to increment the counter by. Sets $COUNT to the number of iterations done
    #[arg(short = 'b', long = "count-by", default_value_t = 1.0)]
    count_by: f64,

    /// Amount to offset the initial counter by
    #[arg(short = 'o', long = "offset")]
    offset: Option<f64>,

    /// How often to iterate. ex., 5s, 1h1m1s1ms1us
    #[arg(short = 'e', long = "every", value_parser = parse_duration)]
    every: Option<Duration>,

    /// A comma-separated list of values, placed into $ITEM. ex., red,green,blue
    #[arg(long = "for", conflicts_with = "stdin")]
    for_values: Option<String>,

    /// Keep going until the duration has elapsed (example 1m30s)
    #[arg(short = 'd', long = "for-duration", value_parser = parse_duration)]
    for_duration: Option<Duration>,

    /// Keep going until the output contains this string
    #[arg(short = 'c', long = "until-contains")]
    until_contains: Option<String>,

    /// Keep going until the output changes
    #[arg(short = 'C', long = "until-changes")]
    until_changes: bool,

    /// Keep going until the output stays the same
    #[arg(short = 'S', long = "until-same")]
    until_same: bool,

    /// Keep going until the output matches this regular expression
    #[arg(short = 'm', long = "until-match", value_parser = Regex::new)]
    until_match: Option<Regex>,

    /// Keep going until a future time, ex. "2018-04-20 04:20:00" (Times in UTC.)
    #[arg(short = 't', long = "until-time", value_parser = parse_rfc3339_weak)]
    until_time: Option<SystemTime>,

    /// Keep going until the command exit status is the value given
    #[arg(short = 'r', long = "until-code")]
    until_code: Option<i32>,

    /// Keep going until the command exit status is zero
    #[arg(short = 's', long = "until-success")]
    until_success: bool,

    /// Keep going until the command exit status is non-zero
    #[arg(short = 'f', long = "until-fail")]
    until_fail: bool,

    /// Only print the output of the last execution of the command
    #[arg(short = 'l', long = "only-last")]
    only_last: bool,

    /// Read from standard input
    #[arg(short = 'i', long = "stdin", conflicts_with = "for_values")]
    stdin: bool,

    /// Exit with timeout error code on duration
    #[arg(short = 'D', long = "error-duration")]
    error_duration: bool,

    /// Provide a summary
    #[arg(long = "summary")]
    summary: bool,

    /// The command to be looped
    #[arg(allow_hyphen_values = true)]
    input: Vec<String>,
}

/// Check a single line for an aborting condition. Returns true if it is met.
fn check_line(opt: &Opt, line: &str, last: Option<&str>) -> bool {
    if let Some(last) = last {
        if opt.until_changes && last != line {
            return true;
        }
        if opt.until_same && last == line {
            return true;
        }
    }

    if opt.until_match.as_ref().is_some_and(|r| r.is_match(line)) {
        return true;
    }

    if opt
        .until_contains
        .as_ref()
        .is_some_and(|s| line.contains(s))
    {
        return true;
    }

    false
}

#[derive(Debug, Default)]
struct Summary {
    successes: u32,
    failures: Vec<i32>,
}

impl Summary {
    fn print(&self) {
        println!(
            "Total runs:\t{}",
            self.successes + self.failures.len() as u32
        );
        println!("Successes:\t{}", self.successes);
        if self.failures.is_empty() {
            println!("Failures:\t0");
        } else {
            println!(
                "Failures:\t{} ({})",
                self.failures.len(),
                self.failures
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
    }
}
