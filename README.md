# SSH Remote Execution Extension

Edit locally, test/run remotely. Adds `ssh_*` tools
(read/write/edit/grep/find/ls/bash/process/push/pull/sync/tunnel) that operate on
an active SSH remote while the built-in `read`/`write`/`edit`/`bash` stay local.

## Connect

```
/ssh -i /path/key.pem root@host:/abs/work        # connect, set remote cwd
/ssh root@host                                    # use remote pwd as cwd
/ssh cd subdir/or/abs/path                        # move remote cwd (no reconnect)
/ssh profiles                                     # list saved profiles
/ssh off                                          # disconnect
/ssh                                              # status
```

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
  matches a regex (`stream` default `both`, `repeat` default one-shot).

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

## Transfer (`ssh_push` / `ssh_pull`)

rsync over the shared connection. `ssh_push` is `.gitignore`-filtered. Both
stream live progress and accept `dryRun: true` to preview a transfer (handy
before pushing a large tree) without writing. The progress flag is chosen by
local rsync version — `--info=progress2` on rsync ≥ 3.1, `--progress` on the
rsync 2.6.9 Apple ships by default (so push/pull work out of the box on stock
macOS). Missing rsync gives an actionable error (`brew install rsync`).

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
