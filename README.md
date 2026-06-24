# SSH Remote Execution Extension

Edit locally, test/run remotely. Adds `ssh_*` tools (read/write/edit/grep/find/ls/bash/process/push/pull) that operate on an active SSH remote while the built-in `read`/`write`/`edit`/`bash` stay local.

## Connect

```
/ssh -i /path/key.pem root@host:/abs/work        # connect, set remote cwd
/ssh root@host                                    # use remote pwd as cwd
/ssh cd subdir/or/abs/path                        # move remote cwd (no reconnect)
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
wake. If pi exits entirely, the job still runs; reconnect and `ssh_process list`.

## Transfer (`ssh_push` / `ssh_pull`)

rsync over the shared connection. `ssh_push` is `.gitignore`-filtered. Both
stream live `--info=progress2` output and accept `dryRun: true` to preview a
transfer (handy before pushing a large tree) without writing.

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
