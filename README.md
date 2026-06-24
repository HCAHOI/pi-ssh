# SSH Remote Execution Extension

Edit locally, test/run remotely. Adds `ssh_*` tools (read/write/edit/grep/find/ls/bash/process/push/pull) that operate on an active SSH remote while the built-in `read`/`write`/`edit`/`bash` stay local.

## Connect

```
/ssh -i /path/key.pem root@host:/abs/work        # connect, set remote cwd
/ssh root@host                                    # use remote pwd as cwd
/ssh off                                          # disconnect
/ssh                                              # status
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

## Design notes

- OpenSSH ControlMaster multiplexing — one persistent master per connection.
- Real in-place remote `ssh_edit` via python3 (diff returned); read-rewrite fallback when python3 is absent.
- POSIX-safe quoting; payloads sent via stdin; remote-path escapes rejected.
- Retry-once on transport drops (ControlMaster reset).
- `ssh_push`/`ssh_pull` use rsync over the shared connection (`.gitignore`-filtered, itemized output).

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
