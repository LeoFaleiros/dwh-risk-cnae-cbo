import hashlib
from datetime import datetime, timezone

from sqlalchemy import Engine, text


def compute_checksum(value: str) -> str:
    return hashlib.md5(value.encode()).hexdigest()


def is_already_loaded(engine: Engine, table_name: str, checksum: str) -> bool:
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT 1 FROM ops.load_history "
                "WHERE table_name = :t AND checksum = :c AND status = 'success'"
            ),
            {"t": table_name, "c": checksum},
        ).fetchone()
    return row is not None


def register_start(
    engine: Engine,
    table_name: str,
    file_name: str,
    checksum: str,
    partition_dt: str | None = None,
) -> int:
    with engine.begin() as conn:
        row = conn.execute(
            text(
                "INSERT INTO ops.load_history "
                "(table_name, partition_dt, file_name, checksum, started_at, status) "
                "VALUES (:t, :p, :f, :c, :ts, 'running') RETURNING id"
            ),
            {
                "t": table_name,
                "p": partition_dt,
                "f": file_name,
                "c": checksum,
                "ts": datetime.now(timezone.utc),
            },
        ).fetchone()
    return row[0]


def register_finish(engine: Engine, load_id: int, rows: int) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE ops.load_history "
                "SET finished_at = :ts, status = 'success', rows_loaded = :r "
                "WHERE id = :id"
            ),
            {"ts": datetime.now(timezone.utc), "r": rows, "id": load_id},
        )


def register_error(engine: Engine, load_id: int, error_msg: str) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE ops.load_history "
                "SET finished_at = :ts, status = 'error', error_msg = :e "
                "WHERE id = :id"
            ),
            {"ts": datetime.now(timezone.utc), "e": str(error_msg)[:2000], "id": load_id},
        )
