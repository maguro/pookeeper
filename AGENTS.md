# AGENTS.md

Directives for coding agents working in this repository.

## Project

Pookeeper is a pure Python ZooKeeper client. It has no runtime dependencies. It supports Python 3.10 through 3.12.

## Commands

The project uses `uv`. Run tools through `uv run`.

- `make install`: create the virtual environment and install the pre-commit hooks.
- `make test`: run the full test suite with coverage.
- `uv run pytest tests/test_path.py`: run one test file. Add `::test_name` to run one test.
- `make check`: run the lock file check, pre-commit, mypy, and deptry. CI runs the same checks.
- `uv run mypy`: type check the `pookeeper` package.
- `make docs-test`: build the MkDocs site in strict mode.
- `make build`: build the wheel and sdist into `dist/`.

## Tests

- `tests/test_client.py` and `tests/test_container.py` start a real ZooKeeper server in Docker through `testcontainers` (`tests/container.py`). Docker must be running. The container binds host ports 2181, 2888, and 3888 and uses the fixed name `zookeeper`, so these tests cannot run in parallel.
- `tests/test_archive.py` and `tests/test_path.py` need no server.
- `tests/harness.py` and `tests/common.py` are a legacy harness that runs a local ZooKeeper cluster from `ZOOKEEPER_PATH`. Current tests do not use it.
- Mocks use `mockito`, not `unittest.mock`.
- The pytest timeout is 300 seconds per test.

## Architecture

- `pookeeper/__init__.py`: public API. It holds the `allocate*` factory functions, `Watcher`, the connection `State` classes, `CreateCode` types, `Perms`, and the `ZookeeperError` hierarchy mapped from server error codes.
- `pookeeper/zookeeper.py`: the client classes. `Client33` implements the ZooKeeper 3.3 operations. `Client34` adds multi-op transactions through `_Transaction`.
- `pookeeper/impl.py`: the connection engine. `WriterThread` owns the socket, connects, sends requests, and handles reconnect and session timeout. `ReaderThread` reads replies and dispatches watcher events. Requests and replies match by xid.
- `pookeeper/archive.py`: Jute binary serialization (`OutputArchive`, `InputArchive`).
- `pookeeper/packets/`: one class per Jute record. `data/` holds shared records and `proto/` holds request and response records. Each class has `serialize` and `deserialize` methods that call the archive. Follow the existing pattern when you add a record.
- `pookeeper/hosts.py`: parses the connection string and iterates hosts in random order.
- `pookeeper/zkpath.py`: ZooKeeper path helpers.

## Code style

- Follow PEP 8, except that the line length is 88.
- Write Google-style docstrings with `Args:`, `Returns:`, and `Raises:` sections. mkdocstrings renders this format.
- Ruff settings are in `pyproject.toml`, but the ruff pre-commit hook is disabled. Do not reformat files you do not change.
- New and changed functions get type hints. mypy runs with `check_untyped_defs`.
- Source files start with the Apache 2.0 license header. Copy it into new files.
- Keep the package free of runtime dependencies. Add development tools to the `dev` dependency group in `pyproject.toml`, then run `uv lock`.

## Commits

- Write commit messages in the Chris Beams style: an imperative subject line of 50 characters or fewer, a blank line, and a wrapped body that explains why.
- Do not add co-author or attribution lines.
