# Continuous integration

Woodpecker reads the seven workflows in `.woodpecker/`. They run on pushes to
`main` and pull requests targeting `main`, matching the previous GitHub CI:

| Workflow | Checks |
| --- | --- |
| `check` | Rust formatting, Clippy, and workspace tests (serial, with RabbitMQ and Bun) |
| `audit` | `cargo audit` |
| `ui` | Node 20 UI build and committed `rust/ui/dist` comparison |
| `pika-smoke` | Python pika publish/consume smoke against the Rust broker |
| `bun` | Bun broker, console, and conformance runner regression tests, with a Rust peer |
| `php` | PHP broker tests with the sockets extension |
| `conformance` | RabbitMQ, Rust, Bun, and PHP protocol conformance |

## Woodpecker setup

Activate the repository on your Woodpecker server. Use the default configuration
path (`.woodpecker/`) and a Linux Docker agent. Have a Woodpecker server admin
enable **Trusted → Security** in the repository settings (Woodpecker 3.x;
older versions use a single **Trusted** toggle). Both `check` and
`conformance` need this repository-wide permission: these steps start private
Docker daemons to run their fixtures. They do not mount the agent's Docker socket
or use host networking. Each workflow has its own workspace and fixture ports.

See Woodpecker's [project settings](https://woodpecker-ci.org/docs/usage/project-settings)
and [privileged mode](https://woodpecker-ci.org/docs/usage/workflow-syntax#privileged-mode)
documentation for the server-side setting.

If parsing fails with `Insufficient trust level to use privileged mode`, open
the repository in Woodpecker, go to **Settings → General → Trusted**, enable
**Security**, save, and restart the failed pipeline. Only Woodpecker server
admins can change this setting; repository admin access on GitHub is not enough.
The Network and Volumes trust options are not required by these workflows.

An authenticated Woodpecker 3.x server admin can also apply the setting with:

```sh
woodpecker-cli repo update --trusted-security danielbarion/queueforge
```

The local configuration linter checks YAML and workflow rules but does not
validate the repository's trust permissions on your Woodpecker server. Removing
`privileged: true` alone would prevent the private Docker daemons from starting.

The conformance workflow exposes its temporary LDAP and HTTPS fixtures to its
private Docker bridge with `QF_FIXTURE_BIND_HOST=0.0.0.0`; local runs continue
binding to loopback by default. On Linux, the RabbitMQ container maps
`host.docker.internal` to the Docker host gateway.

No repository secrets are required for the checks. Conformance summaries appear
in the step logs even after test failures, and the final step creates
`conformance-results.tar.gz` containing the JSON and JUnit results. Woodpecker
does not provide the previous GitHub artifact upload automatically: to retain
the archive outside the temporary workspace, add an upload plugin for your
artifact store and its credentials.

Validate the configuration locally with:

```sh
woodpecker-cli lint .woodpecker/*.yaml
```

If branch protection requires the old GitHub job statuses, replace those required
checks with the Woodpecker statuses after the first successful run.

## GitHub CI rollback

The old workflow is preserved as `.github/workflows/ci.yml.disabled`. GitHub
Actions only loads `.yml` and `.yaml` files, so it no longer runs automatically.
Rename it back to `ci.yml` to restore GitHub CI; deactivate the repository in
Woodpecker if you want to stop Woodpecker runs at the same time.

See GitHub's [workflow file requirements](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax).
