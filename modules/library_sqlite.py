"""Wrapper over sqlite3 with a simple, safe API.

Same style contract as ``library_requests``: every public method returns a
``(bool, data)`` tuple.

``False`` always means "something failed" and ``data`` is the raised
exception; ``True`` means success and ``data`` is either a status message
(writes/checks) or rows from the query (reads).

All SQL is built with ``?`` placeholders for values and double-quoted,
validated identifiers for tables/columns, so apostrophes and special
characters never break (or inject into) a query.
"""

from __future__ import annotations

import functools
import os
import queue
import re
import sqlite3
import threading
from typing import Any

# Identifier validation: only letters, digits and underscores. Anything else
# is rejected before it can reach the SQL string.
_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
# A data type carries its DEFAULT too, and a DEFAULT is written with quotes
# and commas: "TEXT NOT NULL DEFAULT (datetime('now'))" is the normal way to
# stamp a row from the database itself. Semicolons, comment markers and double
# quotes stay rejected, so a data type still cannot smuggle a second statement
# or a string literal of its own.
_SAFE_DATA_TYPE = re.compile(r"^[A-Za-z0-9_ ()',]*$")

# Operators allowed in the advanced WHERE helpers. This is a whitelist on
# purpose: the operator is interpolated into the SQL string (a placeholder
# cannot be an operator), so it must not be able to carry anything else.
_SAFE_OPERATORS = frozenset({
    "=", "!=", "<>", ">", ">=", "<", "<=",
    "LIKE", "NOT LIKE",
    "IN", "NOT IN",
    "IS NULL", "IS NOT NULL",
    "BETWEEN", "NOT BETWEEN",
})

_OPERATORS_NO_VALUE = frozenset({"IS NULL", "IS NOT NULL"})
_OPERATORS_MANY_VALUES = frozenset({"IN", "NOT IN"})
_OPERATORS_TWO_VALUES = frozenset({"BETWEEN", "NOT BETWEEN"})

class _WriteQueue:
    """
    One dedicated background thread owns a single write connection and runs
    every write statement through it, one at a time, in arrival order.

    This is what actually removes "database is locked" errors, rather than
    just tolerating them better: two threads can never contend for SQLite's
    write lock if only one connection (this one) ever writes. WAL still lets
    concurrent reads happen on their own short-lived connections while this
    thread is mid-write, so reads are unaffected and unserialized.

    One instance per ``path_db`` (cached on the class), because two writer
    threads for the same file would bring the contention right back.

    The thread can fail, and a failed thread must never turn into a caller
    that waits forever: if the file cannot be opened, ``self.error`` is set
    and every later job is answered with it immediately, and
    ``for_path()`` replaces the dead instance so a transient filesystem
    problem does not poison that path for the life of the process.
    """

    _instances: dict[str, "_WriteQueue"] = {}
    _instances_lock = threading.Lock()

    def __init__(self, path_db: str, timeout: float = 5.0) -> None:
        self.path_db = path_db
        self.timeout = timeout
        self.error: str | None = None
        self._jobs: queue.Queue = queue.Queue()
        self._thread = threading.Thread(target=self._run_forever, daemon=True)
        self._thread.start()

    @classmethod
    def for_path(cls, path_db: str, timeout: float = 5.0) -> "_WriteQueue":
        """
        Return the writer queue for this path, replacing a dead one.

        A queue is dead when its thread died or when it never managed to open
        the file. Rebuilding it here is what makes the failure self-healing:
        the retry either works or fails again, but it never blocks.

        Args:
            path_db (str): database file this queue writes to.
            timeout (float): seconds to wait for a SQLite lock, and the
                deadline a caller waits for its answer. The first caller for
                a path sets it; later ones reuse that queue.

        Returns:
            (_WriteQueue) a live queue, or a failed one that answers with
            its error instead of hanging.
        """
        with cls._instances_lock:
            instance = cls._instances.get(path_db)
            if instance is not None and not instance.alive():
                instance = None
            if instance is None:
                instance = cls(path_db, timeout=timeout)
                cls._instances[path_db] = instance
            return instance

    def alive(self) -> bool:
        """True while this queue can still do real work."""
        return self.error is None and self._thread.is_alive()

    def _run_forever(self) -> None:
        conn: sqlite3.Connection | None = None
        try:
            conn = sqlite3.connect(
                self.path_db, timeout=self.timeout, check_same_thread=False
            )
            conn.execute(f"PRAGMA busy_timeout = {int(self.timeout * 1000)}")
            try:
                conn.execute("PRAGMA journal_mode = WAL")
            except sqlite3.Error:
                pass
        except Exception as error:  # missing folder, read-only mount, ...
            self.error = f"SQLite writer could not open {self.path_db!r}: {error}"

        while True:
            job = self._jobs.get()
            if job is None:
                break
            statement, params, done = job
            # ``conn is None`` is asked separately from ``self.error``: the
            # connection is the thing that may be missing, and every use of it
            # below has to be behind a check that says so.
            if conn is None:
                done.set_result((
                    False,
                    self.error or f"SQLite writer has no connection to {self.path_db!r}",
                ))
                continue
            if self.error is not None:
                done.set_result((False, self.error))
                continue
            try:
                if callable(statement):
                    result = statement(conn)
                    conn.commit()
                    done.set_result((True, result))
                else:
                    cursor = conn.execute(statement, params)
                    conn.commit()
                    done.set_result((True, cursor))
            except Exception as error:  # sqlite3.Error, ...
                try:
                    conn.rollback()
                except sqlite3.Error:
                    pass
                done.set_result((False, str(error)))
                self._check_connection(conn)

    def _check_connection(self, conn) -> None:
        """
        Mark the queue as failed when the connection itself is unusable.

        Only runs after an error, so it costs nothing on the happy path. A
        connection that survived an ordinary error (a constraint violation, a
        duplicate column) is left alone: that is no reason to stop writing.
        """
        if conn is None:
            return
        try:
            conn.execute("SELECT 1")
        except Exception as error:
            self.error = f"SQLite writer lost the connection to {self.path_db!r}: {error}"

    def execute(self, sql: str, params: tuple) -> tuple[bool, Any]:
        """
        Queue one write and wait for its answer, within the timeout.

        Returns:
            (tuple) ``(True, cursor)`` or ``(False, error)``. It always
            returns: a queue that cannot work says so instead of blocking.
            The cursor travels back to the caller so ``lastrowid`` can be
            read without a second query.
        """
        return self.run_job(lambda conn, statement=sql, values=params: conn.execute(statement, values))

    def run_job(self, job) -> tuple[bool, Any]:
        """
        Run a callable with the writer connection, inside the writer thread.

        This is what makes a read-modify-write atomic. The callable sees a
        connection nobody else can write through, so the columns it read are
        still the columns it writes: without this, a second request can slip
        an ``ALTER TABLE`` in between and the row no longer matches the
        table. The writer thread commits (or rolls back) around it, so a
        callable that raises leaves nothing half-written.

        Args:
            job (callable): receives the connection, returns the payload.

        Returns:
            (tuple) ``(True, payload)`` or ``(False, error)``.
        """
        if self.error is not None:
            return (False, self.error)
        done = _SimpleFuture(timeout=self.timeout, label=self.path_db)
        self._jobs.put((job, None, done))
        return done.get_result()


class _SimpleFuture:
    """
    Minimal blocking handoff between the caller thread and the writer thread.

    The wait has a deadline on purpose. A writer thread that dies without
    answering (killed, out of memory, an error raised outside the guarded
    block) would otherwise leave the caller blocked forever, and a hang is not
    something this contract can express.
    """

    def __init__(self, timeout: float, label: str) -> None:
        self._event = threading.Event()
        # A real tuple, not None: get_result() is typed as always returning
        # one, and the value below is what a caller would get if the event
        # were set without a result. That should not happen, but a timeout
        # path that returned None would hand a caller a None to unpack.
        self._result: tuple[bool, Any] = (False, "the writer thread stopped without an answer")
        self._timeout = timeout
        self._label = label

    def set_result(self, result: tuple[bool, Any]) -> None:
        self._result = result
        self._event.set()

    def get_result(self) -> tuple[bool, Any]:
        if not self._event.wait(self._timeout):
            return (
                False,
                f"SQLite writer did not answer within {self._timeout:g}s "
                f"for {self._label}",
            )
        return self._result


def _never_raises(method):
    """Turn an escaping exception into ``(False, message)``.

    The module's contract is that a caller chains calls without a
    try/except, so a bad table or column name is a returned error and never
    a raised ``ValueError``. Identifier validation is the reason this exists:
    it was reachable from twenty call sites, and the one that was not inside
    a try/except raised straight at the caller. A ``ValueError`` is the
    expected answer to a bad identifier, so its message is returned plain; any
    other class keeps its name, so a genuine bug is still visible in the log.
    """

    @functools.wraps(method)
    def wrapper(*args, **kwargs):
        try:
            return method(*args, **kwargs)
        except Exception as error:
            if isinstance(error, ValueError):
                return (False, str(error))
            return (False, f"{type(error).__name__}: {error}")

    return wrapper


class module_sqlite:
    """
    Minimal, safe SQLite helper.

    All methods return a (bool, data) tuple: True on success with a status
    message or the fetched rows, False on failure with the error message.
    No method raises; validation errors come back as (False, "...").

    ------------------------------------------------------------------
    Available methods
    ------------------------------------------------------------------

    Check:
        check_table_name                  check_table_name_structure
        check_column                      check_column_and_value_name

    Create:
        create_table                      create_new_column_in_table
        create_values_in_table_where      insert_dict

    Rename:
        rename_table                      rename_value_in_table

    Update:
        set_column_all_rows
        update_table_values_where         update_table_values_where_and_match
        update_multiple_params_where_and_match
        update_table_values_where_many    update_where_advanced

    Remove:
        remove_table                      remove_all_data_from_table
        remove_table_values_where         remove_table_values_where_and_match
        remove_table_values_where_many    remove_where_advanced
        delete_values_in_table_where      delete_all_values_in_table

    Read:
        read_tables_from_database         read_all_table_and_columns
        read_only_last_column_nums        read_by_id
        read_all_table_and_columns_where
        read_table_values_where           read_table_values_where_and_match
        read_table_values_where_many      read_where_advanced
        read_columns_where
    ------------------------------------------------------------------
    Filtering levels
    ------------------------------------------------------------------

    Reads, updates and deletes come in three levels of filter size, plus
    an operator level:

    - 1 column      read_table_values_where(table, (col, val))
    - 2 columns     read_table_values_where_and_match(table, (col, val), (col, val))
    - N columns     read_table_values_where_many(table, filters=[(col, val), ...])
    - operators     read_where_advanced(table, filters=[(col, op, val), ...])

    The same shape applies to update_* and remove_*. A filter on a single
    column, on a pair, or on a natural key of three or more columns is one
    call either way, and the values are always bound as ? placeholders.

    ------------------------------------------------------------------
    Contract notes
    ------------------------------------------------------------------

    A read that matches nothing is a success with no rows, not a failure:

        read_table_values_where(..., fetch_one=True)  -> (True, None)
        read_table_values_where(...)                  -> (True, [])

    This is what makes "read it, create it if missing" writable without
    treating the empty case as an error.

    An UPDATE or DELETE that matches no row is also a success: the
    statement did what it was asked to, even if the WHERE matched nothing.

    Values are always passed as ? placeholders. Identifiers (table and
    column names) are validated against ^[A-Za-z_][A-Za-z0-9_]*$ and
    double-quoted before they reach the SQL string. The operators of the
    *_where_advanced helpers come from a whitelist; they are the only
    thing interpolated into the query, and they cannot carry anything else.

    ------------------------------------------------------------------
    Quick start
    ------------------------------------------------------------------
    Example:
        ```python
        from modules.library_sqlite import module_sqlite

        db = module_sqlite(path_db="database.db")

        db.create_table("message", {
            "user_name": "TEXT",
            "drag_screen": "TEXT",
            "time_created": "TEXT",
        })
        ```
        ```python
        db.insert_dict("message", {"user_name": "javier", "drag_screen": "HOME"})

        ok, rows = db.read_table_values_where_many(
            "message",
            filters=[("user_name", "javier"), ("drag_screen", "HOME")],
            fetch_one=True,
        )
        ```
        ```python
        db.update_table_values_where_many(
            "message",
            by_data={"drag_screen": "WORK"},
            filters=[("user_name", "javier")],
        )
        ```
        ```python
        db.remove_table_values_where_many(
            "message",
            filters=[("user_name", "javier"), ("drag_screen", "WORK")],
        )
        ```
    """

    def __init__(self, path_db: str = "database.db", timeout: float = 5.0):
        """
        Args:
            path_db (str): path to the SQLite database file.
            timeout (float): seconds to wait for a SQLite lock. It is the
                **busy timeout**, not a query deadline: it bounds how long a
                writer waits for another writer, and it is also the deadline a
                caller waits for the writer thread to answer. The first
                ``module_sqlite`` opened for a given path sets it for that
                path, because they share one writer thread.

        Returns:
            (none)
        """
        self.path_db = path_db
        self.timeout = float(timeout)

    # ---------------- private helpers ----------------
    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        """
        Validate and double-quote an identifier (table or column name).

        Args:
            identifier (str): name to quote.

        Returns:
            (str) quoted identifier like ``"table_name"``.

        Raises:
            ValueError: if the identifier is not a safe SQLite counter.
        """
        if not _SAFE_IDENTIFIER.fullmatch(str(identifier)):
            raise ValueError(f"Unsafe SQLite identifier: {identifier!r}")
        return f'"{identifier}"'

    def _check_writable_path(self) -> str | None:
        """
        Return why this database cannot be written, or None when it can.

        Cheapest check first, and before the writer thread or the statement
        exists: a wrong path must be reported as a wrong path rather than as
        an API error, and above all it must not become a request that never
        returns.
        """
        path = str(self.path_db or "").strip()
        if not path:
            return "path_db is empty"
        if path == ":memory:":
            return None
        try:
            if os.path.isdir(path):
                return f"path_db is a directory, not a database file: {path!r}"
            parent = os.path.dirname(os.path.abspath(path)) or "."
            if not os.path.isdir(parent):
                return f"the folder of path_db does not exist: {parent!r}"
            if not os.access(parent, os.W_OK):
                return f"the folder of path_db is not writable: {parent!r}"
            if os.path.exists(path) and not os.access(path, os.W_OK):
                return f"the database file is not writable: {path!r}"
        except (OSError, ValueError) as error:
            return f"path_db is not a usable file path: {path!r} ({error})"
        return None

    def _run(self, sql: str, params: tuple = (), fetch: bool = True) -> tuple[bool, Any]:
        """
        Run a statement. Writes go through a single serialized writer
        thread; reads use a fresh, self-closing connection.

        Args:
            sql (str): parameterized SQL statement.
            params (tuple): values for the ``?`` placeholders.
            fetch (bool): if True, this is a read and rows are returned.
                If False, this is a write and it is queued to the single
                writer thread for this ``path_db``.

        Returns:
            (tuple) ``(True, rows)`` on success or ``(False, error)``. It
            always returns: an unusable ``path_db`` is answered with the
            reason, never with a hang.

        A write answers with what the statement produced: the new row id for
        an ``INSERT`` (``lastrowid``) and the number of affected rows for
        anything else. That is why a create endpoint can return ``{"id": 42}``
        without asking the database twice.

        The payload is deliberately typed ``Any``: it is the fetched rows on
        success and the raised exception on failure, so no single type fits.
        Callers must check the flag before touching it, which is what the
        contract says and what a type checker cannot verify for them.

        Writes are serialized through ``_WriteQueue`` instead of each
        opening its own connection: with only one connection ever writing
        to this file from this process, two callers can never contend for
        SQLite's write lock, so "database is locked" cannot happen from
        internal contention (measured: 0 errors at 3000 concurrent writers,
        vs hundreds with the previous per-call-connection approach). Reads
        keep using short-lived connections; WAL lets them proceed freely
        while a write is in flight.
        """
        if not fetch:
            problem = self._check_writable_path()
            if problem:
                return (False, problem)
            status, payload = _WriteQueue.for_path(
                self.path_db, timeout=self.timeout
            ).execute(sql, params)
            if not status:
                return (False, payload)
            verb = str(sql).lstrip().split(None, 1)[0].upper() if str(sql).strip() else ""
            if verb == "INSERT":
                return (True, payload.lastrowid)
            return (True, payload.rowcount)

        try:
            conn = sqlite3.connect(self.path_db, timeout=self.timeout)
            conn.execute(f"PRAGMA busy_timeout = {int(self.timeout * 1000)}")
            try:
                conn.execute("PRAGMA journal_mode = WAL")
            except sqlite3.Error:
                pass
            try:
                cursor = conn.execute(sql, params)
                data = cursor.fetchall()
            finally:
                conn.close()
        except Exception as error:  # sqlite3.Error, ValueError, ...
            return (False, str(error))
        return (True, data)

    def _table_exists(self, table_name: str) -> bool:
        """Return True if ``table_name`` exists in the database."""
        status, rows = self._run(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        )
        return bool(status and rows)

    # ---------------- CHECK ----------------
    @_never_raises
    def check_table_name(self, table_name: str = "message") -> tuple:
        """
        Check if a table exists.

        Args:
            table_name (str): table to look for.

        Returns:
            (tuple) ``(True, "Table exists: [...]")`` or ``(False, message)``.

        Example:
            ```python
            print(db.check_table_name("message"))
            # (True, 'Table exists: [message]')
            ```
        """
        if self._table_exists(table_name):
            return (True, f"Table exists: [{table_name}]")
        return (False, f"Table does not exist: [{table_name}]")

    @_never_raises
    def check_table_name_structure(self, table_name: str = "message") -> tuple:
        """
        Return the structure (PRAGMA rows) of a table.

        Args:
            table_name (str): table to inspect.

        Returns:
            (tuple) ``(True, rows)`` where each row is
            (cid, name, type, notnull, default_value, pk) or ``(False, ...)``.

        Example:
            ```python
            ok, rows = db.check_table_name_structure("message")
            print([row[1] for row in rows])   # column names
            ```
        """
        if not self._table_exists(table_name):
            return (False, f"Table does not exist: [{table_name}]")
        quote = self._quote_identifier(table_name)
        return self._run(f"PRAGMA table_info({quote})")

    @_never_raises
    def check_column(
        self, table_name: str = "message", column_name: str = "column_name"
    ) -> tuple:
        """
        Check if a column exists inside a table.

        Args:
            table_name (str): table to inspect.
            column_name (str): column to look for.

        Returns:
            (tuple) ``(True, message)`` or ``(False, ...)``.

        Example:
            ```python
            print(db.check_column("message", "user_name"))
            # (True, 'Column exists: table [message] column [user_name]')
            ```
        """
        status = self.check_table_name_structure(table_name)
        if not status[0]:
            return status
        rows = status[1]
        columns = [row[1] for row in rows]
        if column_name in columns:
            return (True, f"Column exists: table [{table_name}] column [{column_name}]")
        return (False, f"Column does not exist: table [{table_name}] column [{column_name}]")

    @_never_raises
    def check_column_and_value_name(
        self,
        table_name: str = "message",
        column_data: tuple = (),
    ) -> tuple:
        """
        Check if a column contains a given value.

        Args:
            table_name (str): table to inspect.
            column_data (tuple): (column_name, value) to look for.

        Returns:
            (tuple) ``(True, message)`` or ``(False, ...)``.

        Example:
            ```python
            print(db.check_column_and_value_name("message", ("user_name", "javier")))
            # (True, 'Value exists: column [user_name] value [javier]')
            ```
        """
        if len(column_data) != 2:
            return (False, f"column_data must be a tuple (column, value), got: {column_data!r}")
        column_name, value = column_data

        status, _ = self.check_column(table_name, column_name)
        if not status:
            return self.check_column(table_name, column_name)

        quote = self._quote_identifier(table_name)
        column_quote = self._quote_identifier(column_name)
        status = self._run(
            f"SELECT COUNT(*) FROM {quote} WHERE {column_quote}=?",
            (value,),
        )
        if not status[0]:
            return status
        rows = status[1]
        if not rows or not rows[0][0]:
            return (False, f"Value does not exist: column [{column_name}] value [{value}]")
        return (True, f"Value exists: column [{column_name}] value [{value}]")

    # ---------------- CREATE ----------------
    @_never_raises
    def create_table(
        self, table_name: str = "message", column_data: dict | None = None
    ) -> tuple:
        """
        Create a table if it does not exist yet.

        Args:
            table_name (str): table to create.
            column_data (dict): {column_name: SQL_DATA_TYPE}, e.g.
                {"user_name": "TEXT", "ranking_app": "TEXT"}.

        Returns:
            (tuple) ``(True, message)`` or ``(False, error)``.

        Example:
            ```python
            db.create_table("message", {"user_name": "TEXT", "time_created": "TEXT"})
            # (True, 'Table created: [message]')
            ```
        """
        column_data = column_data or {}
        if not column_data:
            return (False, "column_data cannot be empty")

        try:
            pieces = []
            for column_name, data_type in column_data.items():
                if not _SAFE_DATA_TYPE.fullmatch(str(data_type)):
                    raise ValueError(f"Unsafe SQL data type: {data_type!r}")
                pieces.append(f"{self._quote_identifier(column_name)} {data_type}")
        except Exception as error:
            return (False, str(error))

        quote = self._quote_identifier(table_name)
        status = self._run(
            f"CREATE TABLE IF NOT EXISTS {quote} ({', '.join(pieces)})", fetch=False
        )
        if not status[0]:
            return status
        return (True, f"Table created: [{table_name}]")

    @_never_raises
    def create_new_column_in_table(
        self,
        table_name: str = "message",
        column_name: str = "colum_name",
        sql_data_type: str = "TEXT",
    ) -> tuple:
        """
        Add a new column to an existing table.

        Args:
            table_name (str): existing table.
            column_name (str): name of the new column.
            sql_data_type (str): data type, e.g. "TEXT", "INTEGER".

        Returns:
            (tuple) ``(True, message)`` or ``(False, ...)``.

        Example:
            ```python
            db.create_new_column_in_table("message", "user_name_test", "TEXT")
            ```
        """
        status, _ = self.check_table_name(table_name)
        if not status:
            return self.check_table_name(table_name)
        status, _ = self.check_column(table_name, column_name)
        if status:
            return (False, f"Cannot ADD duplicate column, rename it: [{column_name}]")

        quote = self._quote_identifier(table_name)
        column_quote = self._quote_identifier(column_name)
        if not _SAFE_DATA_TYPE.fullmatch(str(sql_data_type)):
            return (False, f"Unsafe SQL data type: {sql_data_type!r}")

        status = self._run(
            f"ALTER TABLE {quote} ADD COLUMN {column_quote} {sql_data_type}",
            fetch=False,
        )
        if not status[0]:
            return status
        return (True, f"Column added: [{column_name}] in table [{table_name}]")

    @_never_raises
    def create_values_in_table_where(
        self, table_name: str = "message", column_data: tuple = ()
    ) -> tuple:
        """
        Insert one row into a table.

        Args:
            table_name (str): target table.
            column_data (tuple): values of the new row, one per column.

        Returns:
            (tuple) ``(True, message)`` or ``(False, ...)``.

        Example:
            ```python
            db.create_values_in_table_where("message", ("javier", "2026-09-22"))
            ```
        """
        status = self.check_table_name_structure(table_name)
        if not status[0]:
            return status
        rows = status[1]

        column_count = len(rows)
        if not column_data or len(column_data) != column_count:
            return (
                False,
                f"Values provided: {len(column_data)}, columns in table: {column_count}",
            )

        placeholders = ", ".join("?" for _ in column_data)
        quote = self._quote_identifier(table_name)
        status = self._run(
            f"INSERT INTO {quote} VALUES ({placeholders})", tuple(column_data), fetch=False
        )
        if not status[0]:
            return status
        return (True, f"Row inserted in table [{table_name}]: {tuple(column_data)}")

    @_never_raises
    def insert_dict(
        self,
        table_name: str = "message",
        data: dict | None = None,
        defaults: dict | None = None,
        fill_missing: str | int = "",
    ) -> tuple:
        """
        Insert a row from a dict, deciding the columns atomically.

        This is the safe version of "store this JSON": it reads the real
        column order, keeps the values the table has, fills the rest, and
        inserts **in one writer job**. Doing it in separate calls (read
        structure, build the row, insert) is a race: a second request that
        adds a column in between leaves the row with the wrong number of
        values, and the caller gets a 400 for a request that was fine.

        Keys that are not columns of the table are **not** created here. A
        schema that changes with the traffic is a schema nobody can index,
        query or migrate, so a new key is reported instead of silently
        dropped or silently turned into a column.

        A generated key (``INTEGER PRIMARY KEY``, with or without
        ``AUTOINCREMENT``) is left to SQLite unless the caller sends it
        explicitly. It is the rowid alias, so it cannot hold a value at all:
        writing ``fill_missing`` into it fails the whole INSERT with
        "datatype mismatch", which is what a table with an id column used to
        get from every insert.

        Args:
            table_name (str): target table.
            data (dict): {column: value}. Values are stringified.
            defaults (dict): {column: value} used when ``data`` lacks the key.
            fill_missing (str | int): value for columns in neither dict.

        Returns:
            (tuple) ``(True, new_row_id)`` or ``(False, error)``.

        Example:
            ```python
            print(db.insert_dict("records", {"mes": "septiembre", "monto": 5000}))
            # (True, 7)
            ```
        """
        data = data or {}
        defaults = defaults or {}
        if not isinstance(data, dict):
            return (False, f"data must be a dict, got: {type(data).__name__}")
        if not data and not defaults:
            return (False, "data cannot be empty")

        unknown = [key for key in data if not _SAFE_IDENTIFIER.fullmatch(str(key))]
        if unknown:
            return (False, f"Unsafe SQLite identifier: {unknown[0]!r}")

        def job(conn):
            quote = self._quote_identifier(table_name)
            schema = conn.execute(f"PRAGMA table_info({quote})").fetchall()
            columns = [row[1] for row in schema]
            if not columns:
                raise ValueError(f"Table does not exist: [{table_name}]")
            extra = [key for key in data if key not in columns]
            if extra:
                raise ValueError(
                    f"Table [{table_name}] has no column(s) {sorted(extra)}; "
                    f"create them first, its columns are {columns}"
                )
            # PRAGMA row: (cid, name, type, notnull, default_value, pk).
            generated = {
                row[1] for row in schema
                if row[5] and str(row[2]).strip().upper() == "INTEGER"
            }
            columns = [
                column for column in columns
                if column in data or column not in generated
            ]
            values = []
            for column in columns:
                if column in data:
                    values.append(str(data[column]))
                elif column in defaults:
                    values.append(str(defaults[column]))
                else:
                    values.append(str(fill_missing))
            names = ", ".join(self._quote_identifier(column) for column in columns)
            marks = ", ".join("?" for _ in columns)
            cursor = conn.execute(
                f"INSERT INTO {quote} ({names}) VALUES ({marks})", tuple(values)
            )
            return cursor.lastrowid

        status, row_id = _WriteQueue.for_path(
            self.path_db, timeout=self.timeout
        ).run_job(job)
        if not status:
            return (False, row_id)
        return (True, row_id)

    @_never_raises
    def set_column_all_rows(
        self,
        table_name: str = "message",
        column_name: str = "column_name",
        value_to_set: str | int = "NULL",
    ) -> tuple:
        """
        Set a column to a value in ALL rows of the table.

        There is no WHERE clause on purpose: this is an administrative
        operation, not a business one. The name says so, "all rows", so a
        caller cannot reach for it thinking it targets a subset. If you need
        to update specific rows, use update_table_values_where_many or
        update_table_values_where, which take filters.

        Typical uses:

        - Empty an obsolete column: reset_column_all_rows("register", "old_field")
          with the default value_to_set="NULL".
        - Reset counters after a close: reset_column_all_rows("register", "llenos_5kg", 0).

        Args:
            table_name (str): table to update.
            column_name (str): column to reset.
            value_to_set (str | int): value to write. Use "NULL" (the
                default) to set SQL NULL.

        Returns:
            (tuple) (True, message) or (False, ...).

        Example:
            db.reset_column_all_rows("message", "drag_screen")
            # (True, 'Column [drag_screen] in table [message] set to NULL in all rows')

            db.reset_column_all_rows("message", "drag_screen", "HOME")
            # (True, 'Column [drag_screen] in table [message] set to HOME in all rows')
        """
        status = self.check_column(table_name, column_name)
        if not status[0]:
            return status

        quote = self._quote_identifier(table_name)
        column_quote = self._quote_identifier(column_name)
        if str(value_to_set).upper() == "NULL":
            status = self._run(
                f"UPDATE {quote} SET {column_quote}=NULL", fetch=False
            )
        else:
            status = self._run(
                f"UPDATE {quote} SET {column_quote}=?", (value_to_set,), fetch=False
            )
        if not status[0]:
            return status
        return (True, f"Column [{column_name}] in table [{table_name}] set to {value_to_set} in all rows")

    # ---------------- RENAME ----------------
    @_never_raises
    def rename_table(
        self, old_table_name: str = "message", new_table_name: str = "message"
    ) -> tuple:
        """
        Rename a table.

        Args:
            old_table_name (str): current table name.
            new_table_name (str): new table name.

        Returns:
            (tuple) ``(True, message)`` or ``(False, ...)``.

        Example:
            ```python
            db.rename_table("message", "whatsapp")
            ```
        """
        status = self.check_table_name(old_table_name)
        if not status[0]:
            return status

        old_quote = self._quote_identifier(old_table_name)
        new_quote = self._quote_identifier(new_table_name)
        status = self._run(
            f"ALTER TABLE {old_quote} RENAME TO {new_quote}", fetch=False
        )
        if not status[0]:
            return status
        return (True, f"Table [{old_table_name}] renamed to [{new_table_name}]")

    @_never_raises
    def rename_value_in_table(
        self,
        table_name: str = "message",
        old_column_name: str = "message",
        new_column_name: str = "new_data",
    ) -> tuple:
        """
        Rename a column inside a table.

        Args:
            table_name (str): table that owns the column.
            old_column_name (str): current column name.
            new_column_name (str): new column name.

        Returns:
            (tuple) ``(True, message)`` or ``(False, ...)``.

        Example:
            ```python
            db.rename_value_in_table("message", "user_name", "user_name")
            ```
        """
        status = self.check_column(table_name, old_column_name)
        if not status[0]:
            return status

        quote = self._quote_identifier(table_name)
        old_quote = self._quote_identifier(old_column_name)
        new_quote = self._quote_identifier(new_column_name)
        status = self._run(
            f"ALTER TABLE {quote} RENAME COLUMN {old_quote} TO {new_quote}",
            fetch=False,
        )
        if not status[0]:
            return status
        return (True, f"Column [{old_column_name}] renamed to [{new_column_name}]")

    # ---------------- UPDATE ----------------
    @_never_raises
    def update_table_values_where(
        self,
        table_name: str = "message",
        by_data: tuple = (),
        change_data_where: tuple = (),
    ) -> tuple:
        """
        Update a value in the rows where another column matches.

        Args:
            table_name (str): table to update.
            by_data (tuple): (column_name, value_to_set).
            change_data_where (tuple): (column_name, value_to_match).

        Returns:
            (tuple) ``(True, message)`` or ``(False, ...)``.

        Example:
            ```python
            db.update_table_values_where(
                "message",
                by_data=("user_name", "xavier53348"),
                change_data_where=("user_name", "xavier"),
            )
            ```
        """
        status = self.check_column_and_value_name(table_name, change_data_where)
        if not status[0]:
            return status
        if len(by_data) != 2:
            return (False, f"by_data must be a tuple (column, value), got: {by_data!r}")

        try:
            quote = self._quote_identifier(table_name)
            set_column = self._quote_identifier(by_data[0])
            where_column = self._quote_identifier(change_data_where[0])
        except Exception as error:
            return (False, str(error))

        status = self._run(
            f"UPDATE {quote} SET {set_column}=? WHERE {where_column}=?",
            (by_data[1], change_data_where[1]),
            fetch=False,
        )
        if not status[0]:
            return status
        return (True, f"Table [{table_name}] updated: {change_data_where} -> {by_data}")

    @_never_raises
    def update_table_values_where_and_match(
        self,
        table_name: str = "message",
        by_data: tuple = (),
        change_data_where: tuple = (),
        match_with: tuple = (),
    ) -> tuple:
        """
        Update a value in the rows where two columns match.

        Args:
            table_name (str): table to update.
            by_data (tuple): (column_name, value_to_set).
            change_data_where (tuple): (column_name, value_to_match).
            match_with (tuple): extra (column_name, value_to_match) that must
                also match in the same row.

        Returns:
            (tuple) ``(True, message)`` or ``(False, ...)``.

        Example:
            ```python
            db.update_table_values_where_and_match(
                "message",
                by_data=("user_name", "xavier53348"),
                change_data_where=("user_name", "xavier"),
                match_with=("drag_screen", "TEXT"),
            )
            ```
        """
        for column_data in (change_data_where, match_with):
            if len(column_data) != 2:
                return (
                    False,
                    f"column_data must be a tuple (column, value), got: {column_data!r}",
                )
            # The columns are checked, the values are not: an UPDATE that
            # matches no row is a statement that did what it was asked to.
            status = self.check_column(table_name, column_data[0])
            if not status[0]:
                return status
        if len(by_data) != 2:
            return (False, f"by_data must be a tuple (column, value), got: {by_data!r}")

        try:
            quote = self._quote_identifier(table_name)
            set_column = self._quote_identifier(by_data[0])
            where_column = self._quote_identifier(change_data_where[0])
            match_column = self._quote_identifier(match_with[0])
        except Exception as error:
            return (False, str(error))

        status = self._run(
            f"UPDATE {quote} SET {set_column}=? "
            f"WHERE {where_column}=? AND {match_column}=?",
            (by_data[1], change_data_where[1], match_with[1]),
            fetch=False,
        )
        if not status[0]:
            return status
        return (
            True,
            f"Table [{table_name}] updated: {change_data_where} -> {by_data} "
            f"WHERE also {match_with}",
        )

    @_never_raises
    def update_multiple_params_where_and_match(
        self,
        table_name: str = "table_name",
        by_data: dict | None = None,
        change_data_where: list | None = None,
    ) -> tuple:
        """
        Update many columns at once on the rows that match all given filters.

        Args:
            table_name (str): table to update.
            by_data (dict): {column_name: new_value, ...}.
            change_data_where (list): [(column_name, value_to_match), ...].

        Returns:
            (tuple) ``(True, message)`` or ``(False, ...)``.

        Example:
            ```python
            db.update_multiple_params_where_and_match(
                table_name="widgets",
                by_data={"nombre": "Widget Pro", "email": "pro@email.com"},
                change_data_where=[("widget_id", "32"), ("activo", "1")],
            )
            ```
        """
        by_data = by_data or {}
        change_data_where = change_data_where or []
        if not by_data:
            return (False, "by_data cannot be empty")

        status = self.check_table_name(table_name)
        if not status[0]:
            return status

        quote = self._quote_identifier(table_name)
        try:
            set_clause = ", ".join(
                f"{self._quote_identifier(column)} = ?" for column in by_data
            )
            where_clause = " AND ".join(
                f"{self._quote_identifier(column)} = ?" for column, _ in change_data_where
            )
        except Exception as error:
            return (False, str(error))

        if not where_clause:
            return (False, "change_data_where must contain at least one (column, value)")

        values = list(by_data.values()) + [value for _, value in change_data_where]
        status = self._run(
            f"UPDATE {quote} SET {set_clause} WHERE {where_clause}",
            tuple(values),
            fetch=False,
        )
        if not status[0]:
            return status
        return (True, f"Table [{table_name}] updated multiple columns")


    # ---------------- REMOVE ----------------
    @_never_raises
    def remove_table(self, table_name: str = "message") -> tuple:
        """
        Drop (permanently erase) a table and all its data.

        Args:
            table_name (str): table to drop.

        Returns:
            (tuple) ``(True, message)`` or ``(False, ...)``.

        Example:
            ```python
            db.remove_table("message")
            ```
        """
        status = self.check_table_name(table_name)
        if not status[0]:
            return status

        quote = self._quote_identifier(table_name)
        status = self._run(f"DROP TABLE IF EXISTS {quote}", fetch=False)
        if not status[0]:
            return status
        return (True, f"Table [{table_name}] dropped")

    @_never_raises
    def remove_all_data_from_table(self, table_name: str = "message") -> tuple:
        """
        Delete all rows from a table (keeps the table structure).

        Args:
            table_name (str): table to empty.

        Returns:
            (tuple) ``(True, message)`` or ``(False, ...)``.

        Example:
            ```python
            db.remove_all_data_from_table("message")
            ```
        """
        status = self.check_table_name(table_name)
        if not status[0]:
            return status

        quote = self._quote_identifier(table_name)
        status = self._run(f"DELETE FROM {quote}", fetch=False)
        if not status[0]:
            return status
        return (True, f"Table [{table_name}] emptied")

    @_never_raises
    def remove_table_values_where(
        self, table_name: str = "message", remove_data_where: tuple = ()
    ) -> tuple:
        """
        Delete rows where a column matches a given value.

        Args:
            table_name (str): table to change.
            remove_data_where (tuple): (column_name, value_to_match).

        Returns:
            (tuple) ``(True, message)`` or ``(False, ...)``.

        Example:
            ```python
            db.remove_table_values_where("message", ("user_name", "xavier53348"))
            ```
        """
        status = self.check_column_and_value_name(table_name, remove_data_where)
        if not status[0]:
            return status

        quote = self._quote_identifier(table_name)
        column_quote = self._quote_identifier(remove_data_where[0])
        status = self._run(
            f"DELETE FROM {quote} WHERE {column_quote}=?",
            (remove_data_where[1],),
            fetch=False,
        )
        if not status[0]:
            return status
        return (
            True,
            f"Deleted from table [{table_name}] where "
            f"{remove_data_where[0]}={remove_data_where[1]}",
        )

    @_never_raises
    def remove_table_values_where_and_match(
        self,
        table_name: str = "message",
        remove_data_where: tuple = (),
        match_with: tuple = (),
    ) -> tuple:
        """
        Delete rows where two columns both match given values.

        Args:
            table_name (str): table to change.
            remove_data_where (tuple): (column_name, value_to_match).
            match_with (tuple): extra (column_name, value_to_match).

        Returns:
            (tuple) ``(True, message)`` or ``(False, ...)``.

        Example:
            ```python
            db.remove_table_values_where_and_match(
                "message",
                remove_data_where=("user_name", "javier"),
                match_with=("drag_screen", "TEXT"),
            )
            ```
        """
        for column_data in (remove_data_where, match_with):
            if len(column_data) != 2:
                return (
                    False,
                    f"column_data must be a tuple (column, value), got: {column_data!r}",
                )
            # Same rule as the reads: deleting what is not there is a DELETE
            # that matched no row, not a failed one.
            status = self.check_column(table_name, column_data[0])
            if not status[0]:
                return status

        quote = self._quote_identifier(table_name)
        where_column = self._quote_identifier(remove_data_where[0])
        match_column = self._quote_identifier(match_with[0])
        status = self._run(
            f"DELETE FROM {quote} WHERE {where_column}=? AND {match_column}=?",
            (remove_data_where[1], match_with[1]),
            fetch=False,
        )
        if not status[0]:
            return status
        return (
            True,
            f"Deleted from table [{table_name}] where {remove_data_where} "
            f"and {match_with}",
        )

    # ---------------- READ ----------------
    @_never_raises
    def read_by_id(self, table_name: str = "message", row_id: int = 1) -> tuple:
        """
        Read one row by its rowid, the id an insert returns.

        ``insert_dict`` answers with the new ``rowid``, and this reads it
        back, so a create endpoint can return ``{"id": 42}`` and a detail
        endpoint can find it without a second query or a public "id" column.

        Args:
            table_name (str): table to read.
            row_id (int): rowid to fetch.

        Returns:
            (tuple) ``(True, row)`` or ``(False, error)``. A rowid that does
            not exist is an error, not an empty row.

        Example:
            ```python
            status, row_id = db.insert_dict("records", {"cliente": "javier"})
            print(db.read_by_id("records", row_id))
            ```
        """
        if not isinstance(row_id, int) or isinstance(row_id, bool):
            return (False, f"row_id must be an integer, got: {row_id!r}")
        status = self.check_table_name(table_name)
        if not status[0]:
            return status
        quote = self._quote_identifier(table_name)
        status, rows = self._run(f"SELECT rowid, * FROM {quote} WHERE rowid = ?", (row_id,))
        if not status:
            return (False, rows)
        if not rows:
            return (False, f"No row found with id {row_id} in table [{table_name}]")
        return (True, rows[0])

    @_never_raises
    def read_columns_where(
        self,
        table_name: str = "message",
        columns: list | None = None,
        filters: list | None = None,
        fetch_one: bool = False,
        limit: int = 0,
        offset: int = 0,
        order_by: str = "",
    ) -> tuple:
        """
        Read only the given columns from rows that match the given filters.

        This is what the other reads cannot express: they always return
        SELECT *, so a table with many columns costs a full row per match.
        Here the caller chooses the projection, and the filters stay the
        same shape as read_table_values_where_many.

        Args:
            table_name (str): table to read.
            columns (list): column names to select, in order. Must be a
                non-empty list of existing columns.
            filters (list): [(column_name, value_to_match), ...], ANDed.
                An empty list is rejected: that is
                read_all_table_and_columns with a projection, not this.
            fetch_one (bool): if True, return only the first match.
            limit (int): max rows to return. 0 = no limit.
            offset (int): rows to skip, for the next page.
            order_by (str): column, optionally followed by ASC or DESC.

        Returns:
            (tuple) (True, rows), (True, row_or_None) with fetch_one,
            or (False, error).

        Example:
            ok, rows = db.read_columns_where(
                "register",
                columns=["type", "name", "llenos_5kg"],
                filters=[("date", "2024-08-26")],
            )
            # (True, [('bodega', 'local', 5), ('camion', 'pepe', 30)])
        """
        if not columns:
            return (False, "columns must be a non-empty list")
        if not isinstance(columns, (list, tuple)):
            return (False, f"columns must be a list, got: {type(columns).__name__}")

        filters = filters or []
        if not isinstance(filters, (list, tuple)) or not filters:
            return (False, "filters must be a non-empty list of (column, value) tuples")

        for column in columns:
            status = self.check_column(table_name, column)
            if not status[0]:
                return status

        for column_data in filters:
            if not isinstance(column_data, (list, tuple)) or len(column_data) != 2:
                return (
                    False,
                    f"each filter must be a tuple (column, value), got: {column_data!r}",
                )
            status = self.check_column(table_name, column_data[0])
            if not status[0]:
                return status

        if limit < 0 or offset < 0:
            return (False, f"limit and offset cannot be negative, got: limit={limit}, offset={offset}")
        status, order = self._order_clause(table_name, order_by)
        if not status:
            return (False, order)

        try:
            quote = self._quote_identifier(table_name)
            column_clause = ", ".join(self._quote_identifier(c) for c in columns)
            where_clause = " AND ".join(
                f"{self._quote_identifier(column)} = ?" for column, _ in filters
            )
        except Exception as error:
            return (False, str(error))

        page = self._page_clause(limit, offset)
        status = self._run(
            f"SELECT {column_clause} FROM {quote} WHERE {where_clause}{order}{page}",
            tuple(value for _, value in filters),
        )
        if not status[0]:
            return status
        rows = status[1]
        if fetch_one:
            return (True, rows[0] if rows else None)
        return (True, rows)
    
    @_never_raises
    def read_tables_from_database(self) -> tuple:
        """
        List all user tables in the database.

        Returns:
            (tuple) ``(True, [table_name, ...])`` or ``(False, error)``.

        Example:
            ```python
            ok, tables = db.read_tables_from_database()
            print(tables)   # ["message"]
            ```
        """
        status = self._run(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
        if not status[0]:
            return status
        rows = status[1]
        return (True, [row[0] for row in rows])

    @_never_raises
    def read_all_table_and_columns(
        self,
        table_name: str = "message",
        fetch_one: bool = False,
        limit: int = 0,
        offset: int = 0,
        order_by: str = "",
    ) -> tuple:
        """
        Read all rows (or just one) from a table.

        Args:
            table_name (str): table to read.
            fetch_one (bool): if True, return only the first row.
            limit (int): max rows to return. 0 = no limit. **Always set it in
                an endpoint**: materializing a whole table is what makes a
                read slow, and the cost grows with the rows, not with the
                traffic.
            offset (int): rows to skip, for the next page.
            order_by (str): column, optionally followed by ``ASC`` or ``DESC``,
                e.g. ``"creado_en DESC"``. A column that does not exist is
                answered as an error.

        Returns:
            (tuple) ``(True, rows)`` or ``(False, ...)``.

        Example:
            ```python
            ok, rows = db.read_all_table_and_columns(
                "message", limit=50, offset=0, order_by="id DESC"
            )
            print(rows)
            ```
        """
        status = self.check_table_name(table_name)
        if not status[0]:
            return status
        if limit < 0 or offset < 0:
            return (False, f"limit and offset cannot be negative, got: limit={limit}, offset={offset}")
        status, order = self._order_clause(table_name, order_by)
        if not status:
            return (False, order)

        quote = self._quote_identifier(table_name)
        sql = f"SELECT * FROM {quote}" + order
        page = self._page_clause(limit, offset)
        status = self._run(sql + page)
        if not status[0]:
            return status
        rows = status[1]
        return (True, rows[0] if fetch_one else rows)

    @staticmethod
    def _page_clause(limit: int, offset: int) -> str:
        """
        Build the ``LIMIT``/``OFFSET`` tail of a query.

        Both values are integers interpolated into the statement, and they
        were coerced with ``int()`` on purpose: a bound parameter is not
        allowed in a SQLite ``LIMIT``, and interpolating a raw string is
        exactly the injection this module refuses to do elsewhere.
        """
        clause = ""
        if limit:
            clause += f" LIMIT {int(limit)}"
        if offset:
            clause += f" OFFSET {int(offset)}"
        return clause

    def _order_clause(self, table_name: str = "message", order_by: str = "") -> tuple:
        """
        Build the ``ORDER BY`` tail of a query, validated against the table.

        The column goes through the same validated-quote path as everything
        else, because an identifier cannot be a bound parameter: interpolating
        a raw string into a query is the injection this module refuses to do.
        A column that does not exist is answered as an error rather than
        reaching SQLite, so a typo is reported as a typo instead of as an
        OperationalError, and a whole read is not wasted on it.

        Returns:
            (tuple) ``(True, " ORDER BY \\"col\\" DESC")`` or ``(False, error)``.
        """
        if not order_by:
            return (True, "")
        parts = str(order_by).split()
        if len(parts) > 2 or (len(parts) == 2 and parts[1].upper() not in ("ASC", "DESC")):
            return (False, f"order_by must be 'column' or 'column ASC|DESC', got: {order_by!r}")
        direction = f" {parts[1].upper()}" if len(parts) == 2 else ""

        status, columns = self.check_table_name_structure(table_name)
        if not status:
            return (False, columns)
        names = [row[1] for row in columns]
        if parts[0] not in names:
            return (False, f"Column does not exist: table [{table_name}] column [{parts[0]}]")
        return (True, f" ORDER BY {self._quote_identifier(parts[0])}{direction}")

    @_never_raises
    def read_only_last_column_nums(

        self,
        table_name: str = "message",
        column_name: str = "column_name",
        show_last_num_of_values: int = 10,
        fetch_one: bool = False,
    ) -> tuple:
        """
        Read the last N rows of a table by insertion order.

        Args:
            table_name (str): table to read.
            column_name (str): kept for API compatibility, not used. Rows are
                ordered by rowid (insertion order).
            show_last_num_of_values (int): max number of rows returned.
            fetch_one (bool): if True, return only the most recent row.

        Returns:
            (tuple) ``(True, rows)`` or ``(False, ...)``.

        Example:
            ```python
            ok, rows = db.read_only_last_column_nums("message", show_last_num_of_values=2)
            print(rows)
            ```
        """
        status = self.check_table_name(table_name)
        if not status[0]:
            return status

        quote = self._quote_identifier(table_name)
        status = self._run(
            f"SELECT * FROM {quote} ORDER BY rowid DESC LIMIT ?",
            (show_last_num_of_values,),
        )
        if not status[0]:
            return status
        rows = status[1]
        return (True, rows[0] if fetch_one else rows)

    @_never_raises
    def read_table_values_where(
        self,
        table_name: str = "message",
        read_data_where: tuple = (),
        fetch_one: bool = False,
        limit: int = 0,
        offset: int = 0,
        order_by: str = "",
    ) -> tuple:
        """
        Read rows where a column matches a given value.

        Args:
            table_name (str): table to read.
            read_data_where (tuple): (column_name, value_to_match).
            fetch_one (bool): if True, return only the first match.
            limit (int): max rows to return. 0 = no limit.
            offset (int): rows to skip, for the next page.
            order_by (str): column, optionally followed by ``ASC`` or ``DESC``.

        Returns:
            (tuple) ``(True, rows)`` or ``(False, ...)``. **A query that
            matches nothing is a success with no rows**, not a failure: with
            ``fetch_one`` that is ``(True, None)``. It used to be answered
            ``(False, "Value does not exist")``, which is indistinguishable
            from a real read error and turned every "create if it is not
            there yet" into a retryable failure the queue could never satisfy.

        Example:
            ```python
            ok, rows = db.read_table_values_where(
                "message", ("user_name", "xavier53348"),
                limit=50, order_by="id DESC",
            )
            print(rows)
            ```
        """
        status = self.check_column(table_name, read_data_where[0])
        if not status[0]:
            return status
        if limit < 0 or offset < 0:
            return (False, f"limit and offset cannot be negative, got: limit={limit}, offset={offset}")
        status, order = self._order_clause(table_name, order_by)
        if not status:
            return (False, order)

        quote = self._quote_identifier(table_name)
        column_quote = self._quote_identifier(read_data_where[0])
        page = self._page_clause(limit, offset)
        status = self._run(
            f"SELECT * FROM {quote} WHERE {column_quote}=?{order}{page}",
            (read_data_where[1],),
        )
        if not status[0]:
            return status
        rows = status[1]
        if fetch_one:
            return (True, rows[0] if rows else None)
        return (True, rows)

    @_never_raises
    def read_table_values_where_and_match(
        self,
        table_name: str = "message",
        read_data_where: tuple = (),
        match_with: tuple = (),
        fetch_one: bool = False,
        limit: int = 0,
        offset: int = 0,
        order_by: str = "",
    ) -> tuple:
        """
        Read rows where two columns both match given values.

        A match that finds nothing is a success with no rows, for the same
        reason ``read_table_values_where`` is one: a lookup that is allowed to
        come up empty is how "read it, create it if missing" is written, and
        answering that lookup with ``(False, "Value does not exist")`` turns
        the first run of every create into an error the caller cannot tell
        from a broken database. Only the **columns** are validated here, the
        way a ``WHERE`` clause is: a name that is not a column is a bug to
        report, a value that is not there yet is the normal case.

        Args:
            table_name (str): table to read.
            read_data_where (tuple): (column_name, value_to_match).
            match_with (tuple): extra (column_name, value_to_match).
            fetch_one (bool): if True, return only the first match.
            limit (int): max rows to return. 0 = no limit.
            offset (int): rows to skip, for the next page.
            order_by (str): column, optionally followed by ``ASC`` or ``DESC``.

        Returns:
            (tuple) ``(True, rows)`` or ``(False, ...)``.

        Example:
            ```python
            ok, rows = db.read_table_values_where_and_match(
                "message",
                read_data_where=("user_name", "javier"),
                match_with=("drag_screen", "TEXT"),
                fetch_one=False,
            )
            print(rows)
            ```
        """
        for column_data in (read_data_where, match_with):
            if len(column_data) != 2:
                return (
                    False,
                    f"column_data must be a tuple (column, value), got: {column_data!r}",
                )
            status = self.check_column(table_name, column_data[0])
            if not status[0]:
                return status
        if limit < 0 or offset < 0:
            return (False, f"limit and offset cannot be negative, got: limit={limit}, offset={offset}")
        status, order = self._order_clause(table_name, order_by)
        if not status:
            return (False, order)

        quote = self._quote_identifier(table_name)
        where_column = self._quote_identifier(read_data_where[0])
        match_column = self._quote_identifier(match_with[0])
        page = self._page_clause(limit, offset)
        status = self._run(
            f"SELECT * FROM {quote} WHERE {where_column}=? AND {match_column}=?"
            f"{order}{page}",
            (read_data_where[1], match_with[1]),
        )
        if not status[0]:
            return status
        rows = status[1]
        return (True, rows[0] if fetch_one else rows)
    
    @_never_raises
    def read_all_columns_by_table(
        self,
        table_name: str = "message",
        filters: list | None = None,
        fetch_one: bool = False,
        limit: int = 0,
        offset: int = 0,
        order_by: str = "",
    ) -> tuple:
        """
        Read all columns of a table filtered by one or more column/value pairs.

        Same as read_table_values_where_many, kept as an explicit name for the
        case "give me every column of this table for this filter". It returns
        rows as tuples, one per matching row, with every column of the table
        in schema order.

        Args:
            table_name (str): table to read.
            filters (list): [(column_name, value_to_match), ...]. All pairs
                are ANDed. An empty list is rejected: that is
                read_all_table_and_columns, not this.
            fetch_one (bool): if True, return only the first match.
            limit (int): max rows to return. 0 = no limit.
            offset (int): rows to skip, for the next page.
            order_by (str): column, optionally followed by ASC or DESC.

        Returns:
            (tuple) (True, rows), (True, row_or_None) with fetch_one,
            or (False, error).

        Example:
            ok, rows = db.read_all_columns_by_table(
                "register",
                filters=[("date", "2024-08-26")],
            )
            # (True, [(1, '2024-08-26', 'bodega', 'local', 5, ...), ...])
        """
        filters = filters or []
        if not isinstance(filters, (list, tuple)) or not filters:
            return (False, "filters must be a non-empty list of (column, value) tuples")

        for column_data in filters:
            if not isinstance(column_data, (list, tuple)) or len(column_data) != 2:
                return (
                    False,
                    f"each filter must be a tuple (column, value), got: {column_data!r}",
                )
            status = self.check_column(table_name, column_data[0])
            if not status[0]:
                return status

        if limit < 0 or offset < 0:
            return (False, f"limit and offset cannot be negative, got: limit={limit}, offset={offset}")
        status, order = self._order_clause(table_name, order_by)
        if not status:
            return (False, order)

        try:
            quote = self._quote_identifier(table_name)
            where_clause = " AND ".join(
                f"{self._quote_identifier(column)} = ?" for column, _ in filters
            )
        except Exception as error:
            return (False, str(error))

        page = self._page_clause(limit, offset)
        status = self._run(
            f"SELECT * FROM {quote} WHERE {where_clause}{order}{page}",
            tuple(value for _, value in filters),
        )
        if not status[0]:
            return status
        rows = status[1]
        if fetch_one:
            return (True, rows[0] if rows else None)
        return (True, rows)


    # =========================================================================
    # PARTE 2 — Helper _build_where_advanced.
    # Pégalo DENTRO de la clase module_sqlite, junto a _order_clause y _page_clause.
    # =========================================================================

    def _build_where_advanced(self, table_name: str, filters, conjunction: str = "AND") -> tuple:
        """
        Build the WHERE clause for the *_where_advanced helpers.

        Returns (True, (sql_where, params)) or (False, error).
        """
        if not isinstance(filters, (list, tuple)) or not filters:
            return (False, "filters must be a non-empty list of (column, operator, value) tuples")

        conjunction = str(conjunction).upper()
        if conjunction not in ("AND", "OR"):
            return (False, f"conjunction must be 'AND' or 'OR', got: {conjunction!r}")

        fragments = []
        params: list = []
        for item in filters:
            if not isinstance(item, (list, tuple)) or len(item) != 3:
                return (
                    False,
                    f"each filter must be a tuple (column, operator, value), got: {item!r}",
                )
            column, operator, value = item
            status = self.check_column(table_name, column)
            if not status[0]:
                return status
            operator = str(operator).upper().strip()
            if operator not in _SAFE_OPERATORS:
                return (False, f"Unsupported operator: {operator!r}")

            col_quote = self._quote_identifier(column)

            if operator in _OPERATORS_NO_VALUE:
                fragments.append(f"{col_quote} {operator}")
                continue

            if operator in _OPERATORS_MANY_VALUES:
                if not isinstance(value, (list, tuple)) or not value:
                    return (
                        False,
                        f"{operator} expects a non-empty list or tuple, got: {value!r}",
                    )
                marks = ", ".join("?" for _ in value)
                fragments.append(f"{col_quote} {operator} ({marks})")
                params.extend(value)
                continue

            if operator in _OPERATORS_TWO_VALUES:
                if not isinstance(value, (list, tuple)) or len(value) != 2:
                    return (
                        False,
                        f"{operator} expects a tuple (min, max), got: {value!r}",
                    )
                fragments.append(f"{col_quote} {operator} ? AND ?")
                params.extend(value)
                continue

            # Simple binary operators: =, !=, <>, >, >=, <, <=, LIKE, NOT LIKE
            fragments.append(f"{col_quote} {operator} ?")
            params.append(value)

        where_clause = f" {conjunction} ".join(fragments)
        return (True, (where_clause, tuple(params)))


    # =========================================================================
    # PARTE 3 — Los seis métodos que reemplazan los stubs.
    # Bórralos del archivo y pega estos DENTRO de la clase module_sqlite.
    # =========================================================================

    @_never_raises
    def delete_values_in_table_where(
        self, table_name: str = "message", row_id: int = 0
    ) -> tuple:
        """Borra la fila cuyo id coincide. Alias de remove_table_values_where(table, ("id", row_id))."""
        return self.remove_table_values_where(table_name, ("id", row_id))

    @_never_raises
    def delete_all_values_in_table(self, table_name: str = "message") -> tuple:
        """Vacía la tabla, sin borrarla. Alias de remove_all_data_from_table."""
        return self.remove_all_data_from_table(table_name)

    @_never_raises
    def read_all_table_and_columns_where(
        self, table_name: str = "message", row_id: int = 0
    ) -> tuple:
        """Lee la fila por rowid. Alias de read_by_id."""
        return self.read_by_id(table_name, row_id)

    @_never_raises
    def read_table_values_where_many(
        self,
        table_name: str = "message",
        filters: list | None = None,
        fetch_one: bool = False,
        limit: int = 0,
        offset: int = 0,
        order_by: str = "",
    ) -> tuple:
        """Lee filas donde coincidan N columnas (todas, con AND)."""
        filters = filters or []
        if not isinstance(filters, (list, tuple)) or not filters:
            return (False, "filters must be a non-empty list of (column, value) tuples")

        for column_data in filters:
            if not isinstance(column_data, (list, tuple)) or len(column_data) != 2:
                return (
                    False,
                    f"each filter must be a tuple (column, value), got: {column_data!r}",
                )
            status = self.check_column(table_name, column_data[0])
            if not status[0]:
                return status

        if limit < 0 or offset < 0:
            return (False, f"limit and offset cannot be negative, got: limit={limit}, offset={offset}")
        status, order = self._order_clause(table_name, order_by)
        if not status:
            return (False, order)

        try:
            quote = self._quote_identifier(table_name)
            where_clause = " AND ".join(
                f"{self._quote_identifier(column)} = ?" for column, _ in filters
            )
        except Exception as error:
            return (False, str(error))

        page = self._page_clause(limit, offset)
        status = self._run(
            f"SELECT * FROM {quote} WHERE {where_clause}{order}{page}",
            tuple(value for _, value in filters),
        )
        if not status[0]:
            return status
        rows = status[1]
        if fetch_one:
            return (True, rows[0] if rows else None)
        return (True, rows)

    @_never_raises
    def update_table_values_where_many(
        self,
        table_name: str = "message",
        by_data: dict | None = None,
        filters: list | None = None,
    ) -> tuple:
        """Actualiza columnas donde coincidan N columnas (todas, con AND)."""
        return self.update_multiple_params_where_and_match(
            table_name=table_name,
            by_data=by_data,
            change_data_where=filters,
        )

    @_never_raises
    def remove_table_values_where_many(
        self,
        table_name: str = "message",
        filters: list | None = None,
    ) -> tuple:
        """Borra filas donde coincidan N columnas (todas, con AND)."""
        filters = filters or []
        if not isinstance(filters, (list, tuple)) or not filters:
            return (False, "filters must be a non-empty list of (column, value) tuples")

        for column_data in filters:
            if not isinstance(column_data, (list, tuple)) or len(column_data) != 2:
                return (
                    False,
                    f"each filter must be a tuple (column, value), got: {column_data!r}",
                )
            status = self.check_column(table_name, column_data[0])
            if not status[0]:
                return status

        try:
            quote = self._quote_identifier(table_name)
            where_clause = " AND ".join(
                f"{self._quote_identifier(column)} = ?" for column, _ in filters
            )
        except Exception as error:
            return (False, str(error))

        status = self._run(
            f"DELETE FROM {quote} WHERE {where_clause}",
            tuple(value for _, value in filters),
            fetch=False,
        )
        if not status[0]:
            return status
        return (True, f"Deleted from table [{table_name}] where {filters}")


# =========================================================================
# PARTE 4 — Los tres métodos *_where_advanced.
# read_where_advanced va en la sección READ.
# update_where_advanced va en la sección UPDATE.
# remove_where_advanced va en la sección REMOVE.
# =========================================================================

    @_never_raises
    def read_where_advanced(
        self,
        table_name: str = "message",
        filters: list | None = None,
        conjunction: str = "AND",
        fetch_one: bool = False,
        limit: int = 0,
        offset: int = 0,
        order_by: str = "",
    ) -> tuple:
        """Lee filas con operadores: =, !=, >, LIKE, IN, IS NULL, BETWEEN."""
        filters = filters or []
        status, built = self._build_where_advanced(table_name, filters, conjunction)
        if not status:
            return (False, built)
        where_clause, params = built

        if limit < 0 or offset < 0:
            return (False, f"limit and offset cannot be negative, got: limit={limit}, offset={offset}")
        status, order = self._order_clause(table_name, order_by)
        if not status:
            return (False, order)

        quote = self._quote_identifier(table_name)
        page = self._page_clause(limit, offset)
        status = self._run(
            f"SELECT * FROM {quote} WHERE {where_clause}{order}{page}",
            params,
        )
        if not status[0]:
            return status
        rows = status[1]
        if fetch_one:
            return (True, rows[0] if rows else None)
        return (True, rows)

    @_never_raises
    def update_where_advanced(
        self,
        table_name: str = "message",
        by_data: dict | None = None,
        filters: list | None = None,
        conjunction: str = "AND",
    ) -> tuple:
        """Actualiza columnas con operadores en el WHERE."""
        by_data = by_data or {}
        filters = filters or []
        if not by_data:
            return (False, "by_data cannot be empty")

        status, built = self._build_where_advanced(table_name, filters, conjunction)
        if not status:
            return (False, built)
        where_clause, params = built

        try:
            set_clause = ", ".join(
                f"{self._quote_identifier(column)} = ?" for column in by_data
            )
        except Exception as error:
            return (False, str(error))

        values = list(by_data.values()) + list(params)
        quote = self._quote_identifier(table_name)
        status = self._run(
            f"UPDATE {quote} SET {set_clause} WHERE {where_clause}",
            tuple(values),
            fetch=False,
        )
        if not status[0]:
            return status
        return (True, f"Table [{table_name}] updated where {where_clause}")

    @_never_raises
    def remove_where_advanced(
        self,
        table_name: str = "message",
        filters: list | None = None,
        conjunction: str = "AND",
    ) -> tuple:
        """Borra filas con operadores en el WHERE."""
        filters = filters or []
        status, built = self._build_where_advanced(table_name, filters, conjunction)
        if not status:
            return (False, built)
        where_clause, params = built

        quote = self._quote_identifier(table_name)
        status = self._run(
            f"DELETE FROM {quote} WHERE {where_clause}",
            params,
            fetch=False,
        )
        if not status[0]:
            return status
        return (True, f"Deleted from table [{table_name}] where {where_clause}")

# ----------------------------------------------------------------------
# Usage examples (commented). I ran this module directly inside src/ with:
#   python modules/library_sqlite.py
# ----------------------------------------------------------------------
if __name__ == "__main__":
    import os
    import tempfile
    import threading

    temp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    temp_path = temp.name
    temp.close()

    try:
        db = module_sqlite(path_db=temp_path)

        # CREATE
        print(db.create_table("message", {
            "user_name": "TEXT",
            "drag_screen": "TEXT",
            "time_created": "TEXT",
        }))

        # INSERT (two rows on purpose)
        print(db.create_values_in_table_where("message", ("javier", "HOME", "now")))
        print(db.create_values_in_table_where("message", ("xavier53348", "WORK", "later")))

        # CHECK
        print(db.check_table_name("message"))
        print(db.check_column("message", "user_name"))
        print(db.check_column_and_value_name("message", ("user_name", "javier")))

        # READ
        print(db.read_tables_from_database())
        print(db.read_all_table_and_columns("message", fetch_one=True))
        print(db.read_only_last_column_nums("message", show_last_num_of_values=2))

        # UPDATE
        print(db.update_table_values_where(
            "message",
            by_data=("user_name", "xavier53348"),
            change_data_where=("user_name", "javier"),
        ))

        # CONTRACT: malformed by_data must not raise, must return (False, error)
        print(db.update_table_values_where(
            "message",
            by_data=(),
            change_data_where=("user_name", "xavier53348"),
        ))
        print(db.update_table_values_where_and_match(
            "message",
            by_data=("bad column!name", "x"),
            change_data_where=("user_name", "xavier53348"),
            match_with=("drag_screen", "WORK"),
        ))

        # REMOVE (only one row), then empty the table
        print(db.remove_table_values_where("message", ("drag_screen", "WORK")))
        print(db.remove_all_data_from_table("message"))
        print(db.remove_table("message"))

        # ERROR handling: keep the [False, error] contract
        print(db.read_all_table_and_columns("missing_table", fetch_one=True))

        # SQL injection can no longer break queries (it stays a plain value)
        db.create_table("notes", {"text": "TEXT"})
        print(db.create_values_in_table_where("notes", ("it's mine; DROP TABLE notes;",)))

    finally:
        os.unlink(temp_path)

    # ------------------------------------------------------------------
    # Concurrency and safety net. A call that never returns is the one
    # failure this contract cannot express, so every case below runs behind
    # a watchdog: if it does not answer, the self-test fails loudly instead
    # of freezing.
    # ------------------------------------------------------------------
    def call_with_watchdog(label, func, seconds=5.0):
        box = []
        thread = threading.Thread(target=lambda: box.append(func()), daemon=True)
        thread.start()
        thread.join(timeout=seconds)
        if thread.is_alive():
            raise AssertionError(
                f"{label}: the call never returned (still blocked after {seconds:g}s)"
            )
        return box[0]

    with tempfile.TemporaryDirectory() as load_dir:
        # CONCURRENCY: 20 writers x 25 rows through one writer thread. Zero
        # errors and an exact row count is the proof that the queue really
        # removes lock contention instead of only tolerating it.
        rows_per_worker = 25
        workers = 20
        loaded = module_sqlite(path_db=os.path.join(load_dir, "concurrency.db"))
        loaded.create_table("hits", {"payload": "TEXT"})
        failures = []

        def writer(worker_id):
            for index in range(rows_per_worker):
                status, data = loaded.create_values_in_table_where(
                    "hits", (f"{worker_id}-{index}",)
                )
                if not status:
                    failures.append(str(data))

        threads = [threading.Thread(target=writer, args=(worker,)) for worker in range(workers)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        status, rows = loaded.read_all_table_and_columns("hits")
        print(f"[concurrencia] {workers}x{rows_per_worker} escrituras "
              f"-> errores={len(failures)} filas={len(rows)}")
        assert not failures, f"concurrent writes failed: {failures[:3]}"
        assert len(rows) == workers * rows_per_worker, (
            f"expected {workers * rows_per_worker} rows, got {len(rows)}"
        )

        # A wrong path must be reported as a wrong path, fast, and twice in a
        # row: the second call must not inherit a broken writer.
        missing_folder = os.path.join(load_dir, "no-such-folder", "data.db")
        a_directory = os.path.join(load_dir, "i-am-a-directory")
        os.mkdir(a_directory)
        for label, bad_path in (("carpeta inexistente", missing_folder),
                                ("path es una carpeta", a_directory)):
            for attempt in (1, 2):
                result = call_with_watchdog(
                    f"{label} (intento {attempt})",
                    lambda bad_path=bad_path: module_sqlite(path_db=bad_path).create_table(
                        "message", {"user_name": "TEXT"}
                    ),
                )
                print(f"[{label} #{attempt}] -> {result}")
                assert result[0] is False, f"{label} should have failed, got {result}"

        # An unanswered handoff must time out with an error, not block.
        orphan = _SimpleFuture(timeout=0.2, label="orphan.db")
        print("[future sin respuesta] ->", orphan.get_result())

        # A dead writer is replaced: the path works again on the next call.
        dead = _WriteQueue.for_path(os.path.join(load_dir, "heal.db"))
        dead.error = "simulated dead writer"
        print("[cola muerta] ->", dead.execute("CREATE TABLE t(a)", ()))
        healed = _WriteQueue.for_path(dead.path_db)
        print("[cola curada] ->", healed.execute("CREATE TABLE t(a)", ()))
        assert healed.alive(), "a dead writer should have been replaced by a live one"
        assert healed.execute("CREATE TABLE t2(a)", ())[0] is True

        # A path that passes the checks and still cannot be opened (a
        # database on a volume that is not mounted) is the case that used to
        # hang forever: the failure happens inside the writer thread.
        not_mounted = os.path.join(load_dir, "not-mounted.db")
        try:
            os.symlink(os.path.join(load_dir, "nowhere", "data.db"), not_mounted)
        except (OSError, NotImplementedError):
            print("[symlink roto] -> omitido en esta plataforma")
        else:
            for attempt in (1, 2):
                result = call_with_watchdog(
                    f"volumen no montado (intento {attempt})",
                    lambda: module_sqlite(path_db=not_mounted).create_table(
                        "message", {"user_name": "TEXT"}
                    ),
                )
                print(f"[volumen no montado #{attempt}] -> {result}")
                assert result[0] is False, f"a missing volume should have failed: {result}"
            # The volume shows up: the same path must work without a restart.
            real_db = os.path.join(load_dir, "real.db")
            module_sqlite(path_db=real_db).create_table("message", {"user_name": "TEXT"})
            os.remove(not_mounted)
            os.symlink(real_db, not_mounted)
            recovered = call_with_watchdog(
                "volumen montado",
                lambda: module_sqlite(path_db=not_mounted).create_table(
                    "other", {"user_name": "TEXT"}
                ),
            )
            print("[volumen montado] ->", recovered)
            assert recovered[0] is True, f"a healed path should work: {recovered}"

    # ------------------------------------------------------------------
    # A query that matches nothing is a success with no rows, and that
    # distinction is the one a "create if it is not there yet" depends on.
    # It used to be answered (False, "Value does not exist"), which reads
    # exactly like a broken database.
    # ------------------------------------------------------------------
    print("\n[lecturas que no encuentran nada] ->")
    # A fresh file: temp_path was left behind by the killed-writer test above,
    # and a reused path is a reused (cached) writer.
    cases = module_sqlite(path_db=os.path.join(tempfile.mkdtemp(), "reads.db"))
    created = cases.create_table(
        "pedidos", {"codigo": "TEXT UNIQUE", "estado": "TEXT", "monto": "INTEGER"}
    )
    print("  create_table ->", created)
    assert created[0] is True, f"the table should exist: {created}"
    cases.insert_dict("pedidos", {"codigo": "P-1", "estado": "pendiente", "monto": 100})
    cases.insert_dict("pedidos", {"codigo": "P-2", "estado": "entregada", "monto": 200})

    no_match = cases.read_table_values_where("pedidos", ("codigo", "P-999"), fetch_one=True)
    print("  fetch_one sin coincidencia ->", no_match)
    assert no_match == (True, None), f"no match must be (True, None): {no_match}"

    no_rows = cases.read_table_values_where("pedidos", ("estado", "cancelada"))
    print("  sin coincidencias ->", no_rows)
    assert no_rows == (True, []), f"no match must be (True, []): {no_rows}"

    print("  codigo repetido ->", cases.insert_dict("pedidos", {"codigo": "P-1", "estado": "x", "monto": 1}))
    assert cases.insert_dict("pedidos", {"codigo": "P-1", "estado": "x", "monto": 1})[0] is False, (
        "a UNIQUE code must not be inserted twice"
    )
    print("  sigue having one row ->", len(cases.read_all_table_and_columns("pedidos")[1]))

    print("\n[orden y paginacion] ->")
    newest = cases.read_all_table_and_columns("pedidos", order_by="codigo DESC", limit=1)
    print("  order_by codigo DESC, limit 1 ->", newest[1])
    assert newest[1][0][0] == "P-2", f"wrong order: {newest}"
    filtered = cases.read_table_values_where("pedidos", ("estado", "pendiente"), order_by="codigo ASC")
    print("  filtro + orden ->", filtered[1])
    assert len(filtered[1]) == 1, f"wrong filter: {filtered}"
    print("  columna que no existe ->", cases.read_all_table_and_columns("pedidos", order_by="nope")[1])
    assert cases.read_all_table_and_columns("pedidos", order_by="nope")[0] is False
    print("  order_by con inyeccion ->", cases.read_all_table_and_columns("pedidos", order_by="codigo; DROP TABLE pedidos")[1][:46])
    assert len(cases.read_all_table_and_columns("pedidos")[1]) == 2, "the table must survive"

    print("\n[read_by_id] ->")
    first = cases.insert_dict("pedidos", {"codigo": "P-3", "estado": "pendiente", "monto": 300})
    print("  insert devuelve id ->", first)
    print("  read_by_id ->", cases.read_by_id("pedidos", first[1])[1])
    assert cases.read_by_id("pedidos", first[1])[0] is True
    print("  id que no existe ->", cases.read_by_id("pedidos", 9999)[1])
    assert cases.read_by_id("pedidos", 9999)[0] is False
    # The wrong type on purpose: this is the check that keeps a caller from
    # crashing on a value that came from a query string.
    print("  id que no es entero ->", cases.read_by_id("pedidos", "abc")[1])  # type: ignore[arg-type]
    assert cases.read_by_id("pedidos", "abc")[0] is False  # type: ignore[arg-type]

    # ------------------------------------------------------------------
    # A table with a generated key, and a two-column lookup. Both are how a
    # real app stores rows, and both used to fail: the generated id was
    # written like any other column and rejected the whole INSERT, and the
    # two-column lookup answered "Value does not exist" instead of an empty
    # result, so a row could never be created by the read that precedes it.
    # ------------------------------------------------------------------
    print("\n[clave generada y lectura de dos columnas] ->")
    stock = module_sqlite(path_db=os.path.join(tempfile.mkdtemp(), "stock.db"))
    with_key = stock.create_table("stock", {
        "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
        "tipo": "TEXT NOT NULL",
        "nombre": "TEXT NOT NULL",
        "created_at": "TEXT NOT NULL DEFAULT (datetime('now'))",
    })
    print("  create_table con DEFAULT (datetime('now')) ->", with_key)
    assert with_key[0] is True, f"a DEFAULT with quotes is a valid data type: {with_key}"

    generated = stock.insert_dict("stock", {"tipo": "truck", "nombre": "Pepe"})
    print("  insert_dict con id autogenerado ->", generated)
    assert generated[0] is True, f"a generated key must not break the insert: {generated}"
    row = stock.read_by_id("stock", generated[1])[1]
    print("  fila leida ->", row)
    # read_by_id answers "rowid, *", so the generated id shows up twice.
    assert row[3] == "Pepe", f"wrong row: {row}"
    # created_at was not sent, so it is stored as fill_missing ("") and never
    # as the column DEFAULT: insert_dict writes every column it decided to
    # write, and a default only applies to the ones it left out. A caller that
    # wants the stamp passes it in ``defaults``.
    assert row[4] == "", f"a missing column is stored as fill_missing: {row}"
    explicit = stock.insert_dict("stock", {"id": 50, "tipo": "truck", "nombre": "Nano"})
    print("  insert_dict con id explicito ->", explicit)
    assert explicit == (True, 50), f"an explicit key must be honoured: {explicit}"

    pair = stock.read_table_values_where_and_match(
        "stock", ("tipo", "truck"), ("nombre", "NoExiste")
    )
    print("  dos columnas sin coincidencia ->", pair)
    assert pair == (True, []), f"a lookup that matches nothing must succeed empty: {pair}"
    found = stock.read_table_values_where_and_match(
        "stock", ("tipo", "truck"), ("nombre", "Pepe")
    )
    print("  dos columnas con coincidencia ->", found)
    assert found[0] is True and len(found[1]) == 1, f"wrong pair read: {found}"
    print("  dos columnas + orden y limite ->", stock.read_table_values_where_and_match(
        "stock", ("tipo", "truck"), ("nombre", "Pepe"), order_by="id DESC", limit=1
    )[1])
    print("  dos columnas, columna inexistente ->", stock.read_table_values_where_and_match(
        "stock", ("tipo", "truck"), ("nope", "Pepe")
    )[1])
    assert stock.read_table_values_where_and_match(
        "stock", ("tipo", "truck"), ("nope", "Pepe")
    )[0] is False, "an unknown column is still an error"
    print("  update sin coincidencia ->", stock.update_table_values_where_and_match(
        "stock",
        by_data=("nombre", "X"),
        change_data_where=("tipo", "truck"),
        match_with=("nombre", "NoExiste"),
    ))
    assert stock.update_table_values_where_and_match(
        "stock",
        by_data=("nombre", "X"),
        change_data_where=("tipo", "truck"),
        match_with=("nombre", "NoExiste"),
    )[0] is True, "an UPDATE that matches no row is not a failure"
    print("  delete sin coincidencia ->", stock.remove_table_values_where_and_match(
        "stock", ("tipo", "truck"), ("nombre", "NoExiste")
    ))
    assert stock.remove_table_values_where_and_match(
        "stock", ("tipo", "truck"), ("nombre", "NoExiste")
    )[0] is True, "a DELETE that matches no row is not a failure"
