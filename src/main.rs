use anyhow::{Context, Result};
use futures::{
    future::{self, Either},
    stream::{self, StreamExt},
};
use humantime::{parse_duration, parse_rfc3339_weak};
use regex::Regex;
use std::{
    process::Stdio,
    time::{Duration, SystemTime},
};
use structopt::StructOpt;
use tokio::{pin, process, select, time};
use tokio_util::codec::{FramedRead, LinesCodec, LinesCodecError};

// Same exit code as use of `timeout` shell command
static TIMEOUT_EXIT_CODE: i32 = 124;

#[cfg(unix)]
const SHELL: [&str; 2] = ["sh", "-c"];
#[cfg(windows)]
const SHELL: [&str; 2] = ["cmd.exe", "/c"];

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let mut opt = Opt::from_args();

    if opt.input.is_empty() {
        eprintln!("Missing command");
        std::process::exit(1);
    }

    // Until duration of systemtime
    let for_duration = opt
        .for_duration
        .map(time::sleep)
        .map(Either::Left)
        .unwrap_or_else(|| Either::Right(future::pending::<()>()));
    pin!(for_duration);
    let until_time = opt
        .until_time
        .map(|r| r.duration_since(SystemTime::now()).expect("Invalid time"))
        .map(time::sleep)
        .map(Either::Left)
        .unwrap_or_else(|| Either::Right(future::pending::<()>()));
    pin!(until_time);
    let mut until = future::select(for_duration, until_time);

    let mut summary = opt.summary.then(Summary::default);

    // Counter in env variable LOOP_COUNT
    let mut iteration = 0u64;

    // opt.stdin and opt.ffor
    let mut items = if opt.stdin {
        let stdin = tokio::io::stdin();
        let lines = FramedRead::new(stdin, LinesCodec::new());
        let lines = lines.filter_map(|l| future::ready(l.ok()));
        Some(Either::Left(lines))
    } else if let Some(ffor) = opt.ffor.as_ref() {
        let iter = ffor.split(',').map(ToString::to_string);
        Some(Either::Right(stream::iter(iter)))
    } else {
        None
    };

    // opt.only_last
    let mut output = opt.only_last.then(Vec::new);

    // Cach last lines of stdout and stderr if needed
    let mut last_stdout = None;
    let mut last_stderr = None;

    let exit = 'outer: loop {
        let mut command = process::Command::new(SHELL[0]);
        command.arg(SHELL[1]);
        command.arg(&opt.input.join(" "));
        command.stdout(Stdio::piped());
        command.stderr(Stdio::piped());

        // opt.count_by
        let count = opt.offset.unwrap_or_default() + iteration as f64 * opt.count_by;
        command.env("COUNT", count.to_string());
        command.env("ACTUALCOUNT", iteration.to_string());

        // ffor and stdint
        if let Some(items) = items.as_mut() {
            if let Some(f) = items.next().await {
                command.env("ITEM", f);
            } else {
                // Last item already used - exit...
                break 0;
            }
        }

        // spawn
        let mut child = command.spawn()?;
        let mut output_closed = false;
        let mut running = true;

        // opt.num
        if let Some(n) = opt.num.as_mut() {
            if *n == 0 {
                break 0;
            } else {
                *n -= 1;
            }
        }

        // opt.only_last
        if let Some(l) = output.as_mut() {
            l.clear()
        }

        // output streams
        let stdout = child.stdout.take().context("failed to get stdout")?;
        let stdout = FramedRead::new(stdout, LinesCodec::new()).map(Line::Stdout);

        let stderr = child.stderr.take().expect("failed to get stderr");
        let stderr = FramedRead::new(stderr, LinesCodec::new()).map(Line::Stderr);

        // Stream containing stdout and stderr
        let mut stdout_err = stream::select(stdout, stderr);

        // Need to store the last stdout and stderr line
        let need_last = opt.until_same || opt.until_changes;

        'inner: loop {
            select! {
                _ = &mut until => {
                    child.kill().await.context("failed to terminate child process")?;
                    if opt.error_duration {
                        break 'outer TIMEOUT_EXIT_CODE;
                    } else {
                        break 'outer 0;
                    }
                }
                stdout_err = stdout_err.next() => {
                    let (stdout_err, do_break) = match stdout_err {
                        Some(output) => match output {
                            Line::Stdout(Ok(ref l)) => {
                                let last = need_last.then(|| last_stdout.replace(l.to_string())).flatten();
                                let do_break = check_line(&opt, &l, last.as_ref());
                                (output, do_break)
                            },
                            Line::Stderr(Ok(ref l)) => {
                                let last = need_last.then(|| last_stderr.replace(l.to_string())).flatten();
                                let do_break = check_line(&opt, &l, last.as_ref());
                                (output, do_break)
                            }
                            Line::Stdout(e) | Line::Stderr(e) => return e.map(drop).context("io error"),
                        }
                        None => {
                            output_closed = true;
                            // Just break if the child already exited. Otherwise set the flag
                            // and the waitpid will do the rest.
                            if !running {
                                break 'inner;
                            } else {
                                continue 'inner;
                            }
                        }
                    };


                    // Print it
                    if let Some(output) = output.as_mut() {
                        output.push(stdout_err);
                    } else {
                        stdout_err.println();
                    }

                    if do_break {
                        break 'outer 0;
                    }
                }

                exit = child.wait(), if running => {
                    running = false;

                    let exit_status = exit.context("failed to get process exist status")?;
                    let exit_code = exit_status.code().context("failed to get exit code")?;

                    // update summary
                    if let Some(ref mut summary) = summary {
                        if exit_status.success() {
                            summary.successes += 1;
                        } else {
                            summary.failures.push(exit_code);
                        }
                    }

                    // opt.until_fail
                    if opt.until_fail && !exit_status.success() {
                        break 'outer exit_code;
                    }

                    // opt.until_sucess
                    if opt.until_success && exit_status.success() {
                        break 'outer exit_code;
                    }

                    // opt.until_error
                    if opt.until_code == Some(exit_code) {
                        break 'outer exit_code;
                    }

                    // Break the inner loop if the reading stdout and stderr is complete
                    if output_closed {
                        break 'inner;
                    }
                }
                else => (),
            }
        }

        iteration += 1;

        if let Some(every) = opt.every {
            time::sleep(every).await;
        }
    };

    if let Some(lines) = output {
        for line in lines {
            line.println();
        }
    }

    if let Some(_summary) = summary {
        Summary::print(_summary)
    }

    std::process::exit(exit);
}

#[derive(StructOpt, Debug)]
#[structopt(
    name = "loop",
    author = "Rich Jones <miserlou@gmail.com>",
    about = "UNIX's missing `loop` command"
)]
struct Opt {
    /// Number of iterations to execute
    #[structopt(short = "n", long = "num")]
    num: Option<u32>,

    /// Amount to increment the counter by. Set $COUNT to the number of interations done
    #[structopt(short = "b", long = "count-by", default_value = "1")]
    count_by: f64,

    /// Amount to offset the initial counter by
    #[structopt(short = "o", long = "offset")]
    offset: Option<f64>,

    /// How often to iterate. ex., 5s, 1h1m1s1ms1us
    #[structopt(short = "e", long = "every", parse(try_from_str = parse_duration))]
    every: Option<Duration>,

    /// A comma-separated list of values, placed into $ITEM. ex., red,green,blue
    #[structopt(long = "for", conflicts_with = "stdin")]
    ffor: Option<String>,

    /// Keep going until the duration has elapsed (example 1m30s)
    #[structopt(short = "d", long = "for-duration", parse(try_from_str = parse_duration))]
    for_duration: Option<Duration>,

    /// Keep going until the output contains this string
    #[structopt(short = "c", long = "until-contains")]
    until_contains: Option<String>,

    /// Keep going until the output changes
    #[structopt(short = "C", long = "until-changes")]
    until_changes: bool,

    /// Keep going until the output changes
    #[structopt(short = "S", long = "until-same")]
    until_same: bool,

    /// Keep going until the output matches this regular expression
    #[structopt(short = "m", long = "until-match", parse(try_from_str = Regex::new))]
    until_match: Option<Regex>,

    /// Keep going until a future time, ex. "2018-04-20 04:20:00" (Times in UTC.)
    #[structopt(short = "t", long = "until-time", parse(try_from_str = parse_rfc3339_weak))]
    until_time: Option<SystemTime>,

    /// Keep going until the command exit status is the value given
    #[structopt(short = "r", long = "until-code")]
    until_code: Option<i32>,

    /// Keep going until the command exit status is zero
    #[structopt(short = "s", long = "until-success")]
    until_success: bool,

    /// Keep going until the command exit status is non-zero
    #[structopt(short = "f", long = "until-fail")]
    until_fail: bool,

    /// Only print the output of the last execution of the command
    #[structopt(short = "l", long = "only-last")]
    only_last: bool,

    /// Read from standard input
    #[structopt(short = "i", long = "stdin", conflicts_with = "for")]
    stdin: bool,

    /// Exit with timeout error code on duration
    #[structopt(short = "D", long = "error-duration")]
    error_duration: bool,

    /// Provide a summary
    #[structopt(long = "summary")]
    summary: bool,

    /// The command to be looped
    input: Vec<String>,
}

/// Check a single line for a aborting condition. Return true if abortion condition is met.
fn check_line(opt: &Opt, line: &str, last: Option<&String>) -> bool {
    if let Some(last) = last {
        if opt.until_changes && (last != line) {
            return true;
        }

        if opt.until_same && (last == line) {
            return true;
        }
    }

    if opt
        .until_match
        .as_ref()
        .map(|r| r.is_match(&line))
        .unwrap_or(false)
    {
        return true;
    }

    if opt
        .until_contains
        .as_ref()
        .map(|r| line.contains(r))
        .unwrap_or(false)
    {
        return true;
    }

    false
}

/// Output result on stdout or stderr
#[derive(Debug)]
enum Line {
    Stdout(Result<String, LinesCodecError>),
    Stderr(Result<String, LinesCodecError>),
}

impl Line {
    /// Print this line on stdout or stderr
    fn println(&self) {
        match self {
            Line::Stdout(Ok(l)) => println!("{}", l),
            Line::Stderr(Ok(l)) => eprintln!("{}", l),
            _ => (),
        }
    }
}

/// Summary
#[derive(Debug, Default)]
struct Summary {
    successes: u32,
    failures: Vec<i32>,
}

impl Summary {
    /// print summary
    fn print(self) {
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
