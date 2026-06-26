# SSH Remote Execution Extension

Edit locally, test/run remotely. Adds `ssh_*` tools
(read/write/edit/secret_write/grep/find/ls/bash/process/push/pull/sync/tunnel)
that operate on an active SSH remote while the built-in
`read`/`write`/`edit`/`bash` stay local.

Zero third-party runtime dependencies — a single `index.ts` over node builtins
and pi's bundled core packages, sharing one OpenSSH ControlMaster connection.

## Install

```bash
pi install git:github.com/HCAHOI/pi-ssh
# or try it for one run without installing:
pi -e git:github.com/HCAHOI/pi-ssh
```

Then connect with `/ssh user@host[:/abs/path]` (see below). Requires key-based
SSH auth (no password prompts) and `bash` on the remote; `python3` on the remote
enables the efficient in-place `ssh_edit`.

## Connect

```
/ssh -i /path/key.pem root@host:/abs/work        # connect, set remote cwd
/ssh root@host                                    # use remote pwd as cwd
/ssh cd subdir/or/abs/path                        # move remote cwd (no reconnect)
/ssh profiles                                     # list saved profiles
/ssh off                                          # disconnect
/ssh                                              # open the interactive dashboard
```

### Dashboard (`/ssh` with no args)

Bare `/ssh` opens an interactive overlay instead of a one-line status:

- connection block (remote, cwd, python3, activation, env, tunnels, sync state)
- a live process table (refreshes every ~2s) with status, runtime, pid
- keys: `↑↓` select · `enter` live output (esc back) · `k` kill · `c` clear
  finished · `r` refresh · `n` connect (profile picker) · `d` disconnect ·
  `w` cwd · `y` toggle sync · `q`/`esc` close

When connected, a compact `ssh: N running` widget shows above the editor whenever
remote jobs are running, so activity is visible without opening the dashboard.
The agent-facing `ssh_*` tools are unchanged — this is a human/TUI affordance.
Free-form connect strings and `cd` targets prefill the editor with `/ssh …`.

### Connection profiles

Save the active connection (key, host, cwd, activation, env) and reconnect with a
short handle. Profiles live in `~/.pi/ssh-profiles.json`:

```
/ssh save benchmark            # save current connection as @benchmark
/ssh @benchmark                # reconnect from the saved profile
/ssh @benchmark --env FOO=bar  # trailing tokens override the saved string
```

### Persistent activation / env

`--activate <cmd>` and repeatable `--env KEY=VALUE` attach a shell prefix and
environment that are applied to **every** `ssh_bash` and `ssh_process` run, so a
venv / conda / PYTHONPATH setup is not repeated on each call:

```
/ssh -i key root@host:/work --activate 'source .venv/bin/activate' --env PYTHONPATH=/work/src
```

## Remote commands (`ssh_bash`)

Runs a command on the remote over the shared connection. For **long-running**
work (downloads, training, dev servers) prefer `ssh_process`: an `ssh_bash`
`timeout` kills the local SSH client, which closes the channel and the remote
command is most likely terminated by `SIGHUP`. On timeout `ssh_bash` now returns
an explicit message saying it *was* a timeout, what it did to the remote, and to
re-run under `ssh_process` instead — no more ambiguous "Command aborted".

**Long output is consistent with the local `read`/`bash` tools.** `ssh_read`
takes `offset`/`limit` like local `read`. `ssh_bash` inherits the local bash
truncation: output over ~50KB / 2000 lines is tail-truncated and the **full**
output is spilled to a **local** temp file (`/tmp/pi-bash-*.log`), shown as a
`Full output: …` footer. Page that file with the local `read` tool's
`offset`/`limit` rather than re-running the command, and scope commands with
`head`/`tail`/`grep`/`sed` when you only need part. No remote-specific output
cap or extra params — the behavior mirrors the built-in tools exactly.

## Secrets (`ssh_secret_write`)

Write a secret to a remote file **without the value entering the tool-call
record**. The value is read locally from an env var or a local file and streamed
to the remote over stdin; only the name/path and destination are logged. The
remote file is created under `umask 077` (never briefly world-readable) and
`chmod`ed to `0600` by default.

```
ssh_secret_write remotePath=.env.key fromEnv=PIONEER_API_KEY
ssh_secret_write remotePath=/srv/app/token fromFile=./secret.txt mode=640
```

Use this for API keys / tokens / credentials instead of `ssh_write` or
`ssh_bash` (whose arguments are recorded).

## Background jobs (`ssh_process`)

`start | list | output | logs | kill | clear`. Jobs run under
`<remote-cwd>/.pi-ssh-processes/<id>/` (stdout.log, stderr.log, pid, exit_code).
`list`/`output` report the captured exit code once a job finishes (via an EXIT
trap, so it survives commands that call `exit`). `clear` prunes finished jobs.

### Completion & log notifications (no polling)

`start` accepts notification options. A session-scoped poller watches the job and
pushes a message that **re-engages the agent** when something happens, so you
never loop on `list`/`output`:

- `alertOnSuccess` (default `false`), `alertOnFailure` (default `true`),
  `alertOnKill` (default `false`) — fire on exit, classified by exit code
  (`0` success / `>=128` or missing `exit_code` killed / other failure).
- `logWatches: [{ pattern, stream?, repeat? }]` — fire when a remote log line
  matches a regex (`stream` default `both`, `repeat` default one-shot). This is
  sugar: each watch becomes a process-bound **monitor** (see `ssh_monitor`).

Completion notifications report the run duration and, when a **newer run of the
same `name`** was started after this one, tag the alert
`[superseded: a newer run of this name was started after it]` — so a late
failure alert from an obsolete run is not confused with the current one in
parallel multi-run workflows.

The remote job itself runs via `nohup setsid`, so it keeps running regardless of
the local machine. **Mac sleep is safe:** the poller is a local timer that simply
pauses while asleep and, on wake, sweeps the remote logs from byte offsets — no
completion or matching log line is lost, only the notification is delayed until
wake.

**Reconnect / pi-restart safe:** each job persists its notification config to
`notify.json` in the job dir. On every (re)connect — including `ssh_connect`
switching and a full pi restart — pollers are re-armed from disk
(`rehydratePollers`): a finished, un-notified job fires its missed completion
once (guarded by a `notified` marker), and still-running jobs resume watching
from the current log EOF. Reconnecting to the **same** host keeps the in-memory
pollers intact. Use `ssh_process attach <id>` to manually (re)arm notifications
for a job started earlier.

### Live follow

`ssh_process output` accepts `followSeconds: N` to stream new stdout/stderr lines
live for N seconds (remote `timeout … tail -F`) before returning the snapshot —
better than re-polling `output` to watch progress.

## Runtime log monitors (`ssh_monitor`)

`create | list | update | pause | resume | remove | attach`. A **monitor** is a
first-class, runtime-manageable log watch decoupled from `ssh_process`: unlike
`logWatches` (frozen at `start`), you can attach one to **any** running job by id
at **any** time, then change/pause/remove it without restarting the job.

```
ssh_monitor create  source=process:<procId>:stderr  pattern='Traceback'        # -> mon_…
ssh_monitor create  source=process:<procId>:both     pattern='loss=' repeat=true name=loss
ssh_monitor list
ssh_monitor pause   <mon_id>     # resume / remove likewise
ssh_monitor update  <mon_id>  pattern='NaN|inf'      # standalone monitors only
```

- **Source** is `process:<procId>[:stdout|stderr|both]` (stream defaults to
  `both`); get `<procId>` from `ssh_process list`. (Phase 1 is process-source
  only; `file:`/`probe:` sources land later.)
- Matching **seeks to the log's current end** at create, so a monitor attached to
  an already-running job fires only on **new** lines, never historical ones.
- **Standalone** monitors persist to `<remote-cwd>/.pi-ssh-monitors/<id>.json`
  and **re-arm on every (re)connect / pi restart** (seeking each source to EOF),
  exactly like process completion notifications. They auto-remove when their
  bound process is cleared or exits.
- A monitor created from `ssh_process logWatches` is shown in `list` tagged
  `(ssh_process)`; it lives in the job's `notify.json`. `update` only edits
  standalone monitors; `pause/resume/remove` on a sugar monitor are
  session-scoped (reset on the next reconnect, driven by `notify.json`).

## Transfer (`ssh_push` / `ssh_pull`)

rsync over the shared connection. `ssh_push` is `.gitignore`-filtered. Both
accept `dryRun: true` to preview a transfer (handy before pushing a large tree)
without writing.

By **default** the result is a single-line summary (files transferred, bytes,
elapsed) so a large push does not flood the agent context. Pass `verbose: true`
for the full per-file itemized list with live progress streaming. The progress
flag is chosen by local rsync version — `--info=progress2` on rsync ≥ 3.1,
`--progress` on the rsync 2.6.9 Apple ships by default (so push/pull work out of
the box on stock macOS). Missing rsync gives an actionable error
(`brew install rsync`).

## Auto-sync (`ssh_sync`)

`start | stop | status`. Does an initial full push, then a debounced local
`fs.watch` rsyncs the workspace to the remote on every change
(`.gitignore`-filtered) — removing the manual `ssh_push` step from the
edit-locally/run-remotely loop. One watcher at a time; stops on disconnect.
Recursive `fs.watch` requires macOS/Windows (on Linux use `ssh_push`).

## Port forwarding (`ssh_tunnel`)

`open | close | list`. Forwards a remote port to a local port via
`ssh -O forward/cancel -L` over the shared ControlMaster, so a remote dev server
/ TensorBoard / Jupyter / web UI is reachable at `http://localhost:<localPort>`.
Tunnels close automatically on disconnect.

```
ssh_tunnel open  remotePort=8080            # localhost:8080 -> remote 127.0.0.1:8080
ssh_tunnel open  localPort=9000 remotePort=6006   # TensorBoard
ssh_tunnel list
ssh_tunnel close localPort=8080
```

## Tool-call titles

Every `ssh_*` tool renders a rich one-line title that reads like the local tool:
the `[root@host]` badge first (in `muted`), then the **local** tool label
(`read`/`edit`/`ls`/`$`/… in `toolTitle` bold), the path/command argument in
`accent`, and the read line-range in `warning`:

```
[root@host] read src/agents/openclaw/eval/types.py:100-139
[root@host] edit src/agents/openclaw/eval/types.py (2 edits)   → result: +12 -3 colored diff
[root@host] $ pytest -q tests/ (timeout 60s)
[root@host] process start "train" $ python train.py
```

`ssh_read` reuses the local read result renderer (syntax highlight + truncation
note); `ssh_edit` shows a colored `+adds -dels` diff parsed from the remote patch.
The host tag and remote-relative paths come from the live connection at render
time (best-effort; never throws if disconnected).

## Reconnection

Foreground file/read tools (`ssh_read`/`ls`/`find`/`grep`/`write`/`edit`/
`secret_write`) auto-recover from transient transport drops: on a connection-level
failure the dead ControlMaster is torn down and the command is retried with
exponential backoff (1, 2, 4 … capped 30s) up to 10 attempts, showing a live
status `Reconnecting user@host — attempt 2/10, retry in 1s…` and an info/error
notice on recovery/give-up. It only re-runs on transport failures (where the
command never executed), so re-running is safe.

Gated by `AsyncLocalStorage` so background pollers (process/widget monitoring)
never block on a long backoff — they keep failing fast and retrying next tick.
`ssh_bash` and `ssh_process start` intentionally keep the single retry (re-running
a long/launching command up to 10× would be unsafe).

## Design notes

- OpenSSH ControlMaster multiplexing — one persistent master per connection.
- Real in-place remote `ssh_edit` via python3 (diff returned); read-rewrite fallback when python3 is absent.
- POSIX-safe quoting; payloads sent via stdin; remote-path escapes rejected.
- Retry-once on transport drops (ControlMaster reset); poller ticks swallow
  transient per-tick failures so a stale socket after wake can never kill it.
- Internal machine-parsed reads (status / log deltas) use a non-login shell so a
  remote profile banner cannot corrupt status parsing or byte offsets.
- `ssh_read` reports a useful reason (missing / is-a-directory / unreadable)
  instead of an opaque `SSH failed (1)`.
- `ssh_find` returns paths relative to the remote search root (not mangled
  `../../../` paths) and honors `.gitignore` via remote `git check-ignore`.
- `ssh_bash` accepts `tty: true` to allocate a remote pty (`ssh -tt`) for
  commands that need a terminal.

## Type-check (dev)

No local TS install is bundled. To type-check against pi's own types:

```bash
PIROOT=$(dirname "$(command -v pi)")/../lib/node_modules/@earendil-works/pi-coding-agent
# or locate it: npm root -g
TSC=$(find "$(npm root -g)" -path '*typescript/lib/tsc.js' | head -1)
node "$TSC" --noEmit --skipLibCheck --strict --target es2022 --module esnext \
  --moduleResolution bundler --types node \
  --typeRoots "$PIROOT/node_modules/@types" index.ts
```

Pre-existing SDK strictness mismatches (tool `execute` arity / required `details`)
are expected — pi loads extensions via jiti, which transpiles without type-checking.
</content>
