# Isolated PostgreSQL Compose environment

**Infrastructure only. This does not enable PostgreSQL in Codex.** The fork's
Rust adapters, storage selection, migration controller, cross-host history,
installation lifecycle and release qualification remain separate incomplete work.
These files must not be used to claim epic #1 or issue #4/#18 is complete.

## Legacy namespace policy

PostgreSQL bootstrap retains strict protected-table privilege checks (#185).
It does not repair or normalize the broad runtime grants left by pre-hardening
development fixtures at schema formats 1–4. Such a namespace is refused before
migration; its grants and data remain the administrator's responsibility.
There is no supported automatic ACL-normalization path for those fixtures.

Older-format upgrade tests explicitly exercise that refusal before applying the
known fixture grants and testing an upgrade from the hardened layout. The grants
are test preparation, not production migration behavior. The unmerged legacy
catalog implementation remains preserved in `storage/05e1-postgres-thread-sections`;
it is not enabled by this policy. Real Linux/PostgreSQL validation of this decision
remains required before #185 can be closed.

## What is provided

A PostgreSQL 17 development service with TLS-only TCP connections, SCRAM passwords,
separate runtime/migrator/read-only-backup roles, server-side health checks,
receipt-bound Docker engine and volume ownership, schema backup/empty-target
restore, and an opt-in two-deployment integration runner. It creates **no Codex
application tables** and never edits Codex configuration, homes or installations.

The default image tag is `postgres:17.11-bookworm`. `pin` pulls that explicit tag,
resolves its content digest, checks its PostgreSQL version in an isolated process,
and records the digest before `up` is permitted. This is content pinning, not a
claim of an independently verified release signature. Existing pins are never
silently updated. PostgreSQL 18+ and other image variants are refused by the helper.

**Execution status:** the nine-step real infrastructure journey passed on
2026-09-27 from native Windows with Docker Desktop's Linux engine 29.7.2,
Compose 5.4.0, and PostgreSQL 17.11. It verified TLS/authentication, role separation,
backup receipts and integrity, restarts, restored table/sequence permissions,
occupied-target refusal, and live certificate renewal with served-leaf verification.
The tested image digest was
`sha256:639ab7ceb90e13123085b741fb31ef493fba25463002f6da665352e7b534b652`.
Linux and native Windows offline checks cover the host helpers. This is infrastructure
evidence, not a supported Codex/PostgreSQL version matrix or Codex migration approval.

## Requirements and state

Use Python 3.10+, OpenSSL on PATH, and a Docker engine with the Compose v2 plugin
supporting `up --wait`. The containers are Linux containers, including when invoked
from Windows/macOS Docker Desktop. Windows containers are not supported. Native Windows Docker Desktop file sharing
was exercised by the infrastructure journey; macOS has not been qualified.

Choose an **absolute, new external state directory** whose parent already exists.
State inside this source tree, symlink paths, ambiguous names, and conflicting
receipts are rejected. POSIX directories/files use modes 0700/0600. On Windows,
initialization removes inherited directory access and grants the current SID and
SYSTEM before creating credentials. Reload validates native ACLs and rejects unsafe
owners, broad grants and NULL DACLs without changing them. Native Windows tests
exercise initialization, reload, unsafe copies and non-ASCII state paths.

The directory contains `receipt.json`, `secrets/`, `backups/`, and a generated
nonsecret `compose.env`. Keep it outside replaceable packages and source checkouts.
Protect and retain it. File-backed Compose secrets are not an encrypted vault.
Never upload this directory, put it in Git, or paste its contents into a chat.
The private development CA key is retained locally and is **not mounted** into any
container. The service copies only its mounted keys/passwords into a private tmpfs
with PostgreSQL ownership, rather than relying on ignored secret-file uid options.

## Start on Windows PowerShell

Run from this repository root (or the extracted package `source` directory). None of these
commands install or replace a Codex executable.

```powershell
$state = Join-Path $env:LOCALAPPDATA "codex-pg-lab"
python scripts/postgres/manage.py --state "$state" init --project codex-pg-lab
python scripts/postgres/manage.py --state "$state" pin
python scripts/postgres/manage.py --state "$state" config
python scripts/postgres/manage.py --state "$state" up
python scripts/postgres/manage.py --state "$state" smoke
python scripts/postgres/manage.py --state "$state" status
```

On Linux/macOS, substitute an absolute state path, for example
`--state "$HOME/codex-pg-lab"`, in the same commands. The initial `init` action only
generates local files; `pin` uses Docker/registry access; `up` explicitly creates a
new labeled volume and starts the database. Repeated matching initialization does
not rotate or overwrite credentials. `up` rejects certificates expiring within a
week. The generated development server certificate is valid for 90 days. Renew it
before expiry (or recover an expired leaf) without deleting the state or volume:

```powershell
python scripts/postgres/manage.py --state "$state" renew-certificate
python scripts/postgres/manage.py --state "$state" up
```

Renewal keeps the CA, server key, credentials, instance and data volume. It verifies
a new immutable certificate before atomically selecting it in the receipt; `up`
recreates the service to mount that certificate. Interrupted attempts keep the old
receipt active and retain incomplete candidates for inspection. Original files and
previous leaves are retained. Renewal needs no running Docker service and refuses
a CA with less than 90 days remaining. CA replacement requires a separate operator
procedure. The OpenSSL executable chosen by `init --openssl` is saved for later
checks and renewal; older receipts without that field use `openssl` on PATH.

A registry error or missing tools produces a nonzero exit, not a successful pin.
A Docker CLI timeout can leave work running in the Docker engine: reconcile status
rather than blindly starting another operation. Image pins are bound to the engine
ID; changing Docker contexts/engines requires deliberate separate setup.

## Connection and administration boundaries

| Field | Value |
|---|---|
| Published endpoint | `127.0.0.1:<receipt port>` on the **Docker engine host** (default port `55432`) |
| Container service endpoint | `postgres:5432` inside this deployment's network |
| Database / application schema | `codex` / `codex_storage` |
| Runtime identity | `codex_runtime` |
| Migration identity | `codex_migrator`, followed by `SET ROLE codex_owner` |
| Backup identity | `codex_backup` |
| Client TLS | `sslmode=verify-full`, CA from `secrets/ca.crt` |

Runtime may read/write application tables, but cannot create them, become the
owner, create databases/roles, or bypass row security. The migrator is not a
superuser and must explicitly select the owner role so default privileges apply.
The backup login receives SELECT only. Server administration uses a peer-authenticated
Unix socket inside the dedicated container; TCP superuser login is rejected.
Application-specific row security, writer fencing and migration protocols are not
implemented by these generic roles.

Passwords are in separate `secrets/*.password` files, not the Compose environment or
command line. The helper never prints them. Supply the runtime password through a
protected credential facility or mode-0600 libpq passfile; do not construct a
password-bearing shell command or connection URI. The current Codex fork cannot
consume this database until its remote-storage implementation exists.

Do not open a public database port for this development CA setup. For a second
workstation, use an explicitly authorized SSH tunnel to the Docker host's loopback
port and verify the CA plus `localhost` hostname. Provision the public CA and
necessary credential securely on that workstation, not the CA private key.
For a private routed deployment, use operator-managed certificates/firewall rules
and separately review any bind-address change. The helper intentionally provides
no "disable TLS" or public-bind flag. This is not a production HA/PITR deployment.

## Backup and empty-target restore

```powershell
python scripts/postgres/manage.py --state "$state" backup
```

The command prints a backup ID and checksum after both the custom-format archive
and receipt have been published. Find them under `backups/<id>.dump` and `.json`.
Only `codex_storage` is backed up: not all PostgreSQL databases/roles, not Codex
local files, and not installation/configuration state. A schema backup is **not**
the complete bidirectional migration required by #13–#15. Retained `.partial`
files or an archive without a receipt do not signify a completed backup.
Publication uses same-filesystem hard links to refuse overwrite; unsupported
filesystems fail rather than silently substituting unsafe overwrite behavior.

Create a **separate** deployment using another new state directory, project, and
port (for example `--project codex-pg-restore --port 55433`). Initialize, pin and
start that deployment. Then restore an independently trusted archive:

```powershell
python scripts/postgres/manage.py --state "$restoreState" restore `
  --archive "$state/backups/<id>.dump" --sha256 <trusted-sha256> `
  --confirm-empty-destination
```

Replace placeholders with the ID and independently checked digest. A digest proves
which bytes are consumed, not that SQL is safe: **never restore an untrusted dump**.
The helper consumes a private authenticated copy, decodes it fully before database
mutation, and restores with an empty-schema check in one transaction. There is no
`CASCADE`, `--clean`, or existing-data overwrite mode. Runtime/backup grants are
reestablished on the restored schema. A failed restore must be reconciled before
retrying; connection loss after commit is not treated as proof of rollback.

The development restore helper caps compressed input at 128 MiB and requires its
copy plus decoded SQL to fit a 512 MiB tmpfs. Exhaustion fails before mutation.
This is a declared tooling limit, not a claim of bounded large-history migration.
After restoration, verify public Codex behavior once that implementation exists;
a successful PostgreSQL restore does not establish upstream Codex compatibility.

## Persistence, shutdown and recovery

`stop` stops the receipt-owned database; `up` restarts it. `down` removes only the
receipt-owned Compose services/network and preserves the external data volume,
credentials and backups. No scheduled installer, updater or reinstall hook is
created. There is intentionally no purge or volume-deletion command.

```powershell
python scripts/postgres/manage.py --state "$state" down
```

Compose never owns the external volume's lifecycle, even with a raw `down -v`.
Manual Docker volume deletion remains destructive; no wrapper can prevent an
administrator from doing that. Do not delete the data volume to repair a failure.

A partial bootstrap (existing `PG_VERSION` without the matching durable completion
marker), wrong instance identity, foreign Docker volume/container/network,
corrupt/missing credentials or incompatible major version fails closed. Preserve
all affected files/volumes, inspect the specific receipt, and repair on an isolated
copy. There is no automatic reset, credential rotation or marker forgery path.
For missing secrets/receipts, these high-level commands may be blocked: a Docker
administrator must inspect exact ownership before stopping the affected container.

The operation lock serializes helper commands for one state directory. After a
crash, inspect `.operation.lock` and confirm its PID/host is no longer active before
manually removing that **one** stale lock. It is not a cross-host Codex writer fence.
On Windows, the separate `.operation.guard` file holds the operating-system lock
and is persistent; never remove it during marker recovery. POSIX operations use
a persistent per-user guard outside the state directory; never delete those
guard files during recovery. Moving or recreating the state directory does not
release an active guard.

Upgrades require a separately reviewed compatible image and a new qualified state
or explicit operator migration procedure. Re-running `pin` preserves the existing
digest. Arbitrary upstream/package updates, source rebases, shared remote Codex
clients, schema rollback and Codex uninstall are not handled by this environment.
Removing the database containers is distinct from uninstalling the Codex patch.

## Actual infrastructure qualification

The opt-in runner creates two isolated deployments, checks TLS/auth/role separation,
backs up newly written data, recreates/restarts services, restores current data to
an empty destination, rejects a corrupt digest and an occupied destination, verifies
restored permissions, and renews a running service's certificate while comparing
the served certificate with the new leaf. It then stops both projects while retaining
volumes and evidence.
It never interprets missing Docker as a passing or skipped database test.

```powershell
$run = Join-Path $env:LOCALAPPDATA "codex-pg-qualification-new"
python scripts/postgres/integration.py --root "$run" --port-base 55432
```

The root must not exist yet. Inspect `qualification.json` and retained deployment
receipts. This run is still **not** Codex feature qualification: it does not test
SQLx adapters, real conversations, stale-writer fences, migration cutover, the
storage wizard, package updates, upstream export compatibility or uninstall.

Offline developer checks (not a substitute for the command above):

```text
python -m unittest discover -s scripts/postgres/tests -v
python -m unittest discover -s scripts/storage_contract -t scripts -v
```

POSIX permission/FIFO tests are explicitly skipped on Windows. Separate native
Windows tests exercise ACL handling; platform skips are reported in test totals.

## Primary implementation references

- Docker secrets: https://docs.docker.com/compose/how-tos/use-secrets/
- File-backed secret permission caveat: https://docs.docker.com/reference/compose-file/services/#secrets
- External volumes: https://docs.docker.com/reference/compose-file/volumes/
- Official PostgreSQL image: https://github.com/docker-library/postgres
- PostgreSQL 17 TLS: https://www.postgresql.org/docs/17/ssl-tcp.html
- Client verification: https://www.postgresql.org/docs/17/libpq-ssl.html
- Default privilege ownership: https://www.postgresql.org/docs/17/sql-alterdefaultprivileges.html
- Restore/SQL trust: https://www.postgresql.org/docs/17/app-pgrestore.html

These references informed implementation. They do not provide evidence that this
particular Compose deployment or the fork's remote storage has been qualified.
