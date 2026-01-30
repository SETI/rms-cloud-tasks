"""
SQLite database for task tracking and event logging.
"""

import json
import logging
import sqlite3
from pathlib import Path
from types import TracebackType
from typing import Any, Dict, List, Optional

from .time_utils import parse_utc, utc_now_iso


logger = logging.getLogger(__name__)


class TaskDatabase:
    """SQLite database for tracking task status and events."""

    def __init__(self, db_file: str) -> None:
        """
        Initialize the task database.

        Parameters:
            db_file: Path to the SQLite database file
        """
        self.db_file = Path(db_file)
        self.conn: Optional[sqlite3.Connection] = None
        self._initialize_database()

    def _initialize_database(self) -> None:
        """Create the database tables if they don't exist."""
        self.conn = sqlite3.connect(self.db_file, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row  # Allow access by column name

        cursor = self.conn.cursor()

        # Create tasks table
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS tasks (
                task_id TEXT PRIMARY KEY,
                task_data TEXT,
                status TEXT,
                retry BOOLEAN,
                enqueued_at TIMESTAMP,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                elapsed_time REAL,
                hostname TEXT,
                result TEXT,
                exception TEXT,
                exit_code INTEGER
            )
        """
        )

        # Create events table
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TIMESTAMP,
                hostname TEXT,
                event_type TEXT,
                task_id TEXT,
                raw_event TEXT
            )
        """
        )

        # Create index on task status for faster queries
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_task_status ON tasks(status)
        """
        )

        # Create index on event task_id for faster queries
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_event_task_id ON events(task_id)
        """
        )

        self.conn.commit()
        logger.debug(f"Initialized task database at {self.db_file}")

    def insert_task(self, task_id: str, task_data: Dict[str, Any], status: str = "pending") -> None:
        """
        Insert a new task into the database.

        Parameters:
            task_id: Unique task identifier
            task_data: Task data dictionary
            status: Initial task status (default: pending)
        """
        cursor = self.conn.cursor()
        cursor.execute(
            """
            INSERT INTO tasks (task_id, task_data, status, enqueued_at)
            VALUES (?, ?, ?, ?)
            """,
            (task_id, json.dumps(task_data), status, utc_now_iso()),
        )
        self.conn.commit()

    def update_task_enqueued(self, task_id: str) -> None:
        """
        Update task status to in_queue_original when first enqueued to cloud queue.

        Parameters:
            task_id: Task identifier
        """
        cursor = self.conn.cursor()
        cursor.execute(
            """
            UPDATE tasks
            SET status = 'in_queue_original', started_at = ?
            WHERE task_id = ?
            """,
            (utc_now_iso(), task_id),
        )
        self.conn.commit()

    def update_task_from_event(self, event: Dict[str, Any]) -> None:
        """
        Update task status based on an event.

        Parameters:
            event: Event dictionary from worker
        """
        event_type = event.get("event_type")
        task_id = event.get("task_id")
        retry = event.get("retry", False)

        if not task_id:
            # Non-task event (e.g., spot_termination, non_fatal_exception)
            return

        cursor = self.conn.cursor()

        # Determine new status based on event type and retry flag
        if event_type == "in_queue_original":
            status = "in_queue_original"
        elif event_type == "task_completed":
            status = "completed"
        elif event_type == "task_exception":
            if retry:
                status = "exception_with_retry"
            else:
                status = "exception"
        elif event_type == "task_timed_out":
            if retry:
                status = "timed_out_with_retry"
            else:
                status = "timed_out"
        elif event_type == "task_exited":
            if retry:
                status = "exited_without_status_with_retry"
            else:
                status = "exited_without_status"
        else:
            logger.warning(f"Unknown event type: {event_type}")
            return

        # Update task record (normalize timestamp to UTC for storage)
        completed_at = event.get("timestamp")
        if completed_at is not None:
            dt = parse_utc(completed_at if isinstance(completed_at, str) else str(completed_at))
            completed_at = dt.isoformat() if dt is not None else completed_at
        update_fields = {
            "status": status,
            "retry": retry,
            "completed_at": completed_at,
            "elapsed_time": event.get("elapsed_time"),
            "hostname": event.get("hostname"),
        }

        if event_type == "task_completed":
            update_fields["result"] = json.dumps(event.get("result"))
        elif event_type in ("task_exception", "task_exited"):
            if event_type == "task_exception":
                update_fields["exception"] = event.get("exception")
            if event_type == "task_exited":
                update_fields["exit_code"] = event.get("exit_code")

        # Build UPDATE query dynamically
        set_clause = ", ".join([f"{k} = ?" for k in update_fields.keys()])
        values = list(update_fields.values()) + [task_id]

        cursor.execute(
            f"""
            UPDATE tasks
            SET {set_clause}
            WHERE task_id = ?
            """,
            values,
        )
        self.conn.commit()

    def insert_event(self, event: Dict[str, Any]) -> None:
        """
        Insert an event into the events table.
        Event timestamp is normalized to UTC for storage.

        Parameters:
            event: Event dictionary
        """
        ts = event.get("timestamp")
        if ts is not None:
            dt = parse_utc(ts if isinstance(ts, str) else str(ts))
            ts = dt.isoformat() if dt is not None else ts
        cursor = self.conn.cursor()
        cursor.execute(
            """
            INSERT INTO events (timestamp, hostname, event_type, task_id, raw_event)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                ts,
                event.get("hostname"),
                event.get("event_type"),
                event.get("task_id"),
                json.dumps(event),
            ),
        )
        self.conn.commit()

    def get_task_counts(self) -> Dict[str, int]:
        """
        Get counts of tasks by status.

        Returns:
            Dictionary mapping status to count
        """
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT status, COUNT(*) as count
            FROM tasks
            GROUP BY status
            """
        )
        results = cursor.fetchall()
        return {row["status"]: row["count"] for row in results}

    def get_total_tasks(self) -> int:
        """
        Get total number of tasks.

        Returns:
            Total task count
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) as count FROM tasks")
        return cursor.fetchone()["count"]

    def is_all_tasks_complete(self) -> bool:
        """
        Check if all tasks are in a terminal state (reported back, no retry).

        Terminal statuses: completed, exception, timed_out, exited_without_status,
        failed. Tasks with retry=True (exception_with_retry, timed_out_with_retry,
        exited_without_status_with_retry) are not complete.

        Returns:
            True if all tasks are in a terminal state
        """
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT COUNT(*) as count
            FROM tasks
            WHERE status NOT IN (
                'completed',
                'exception',
                'timed_out',
                'exited_without_status',
                'failed'
            )
            """
        )
        incomplete_count = cursor.fetchone()["count"]
        return incomplete_count == 0

    def get_tasks_by_status(self, status: str) -> List[Dict[str, Any]]:
        """
        Get all tasks with a specific status.

        Parameters:
            status: Task status to filter by

        Returns:
            List of task dictionaries
        """
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT * FROM tasks WHERE status = ?
            """,
            (status,),
        )
        return [dict(row) for row in cursor.fetchall()]

    def get_task_statistics(self) -> Dict[str, Any]:
        """
        Get comprehensive task statistics.

        Returns:
            Dictionary with statistics
        """
        cursor = self.conn.cursor()

        # Get elapsed time statistics for completed tasks
        cursor.execute(
            """
            SELECT
                MIN(elapsed_time) as min_time,
                MAX(elapsed_time) as max_time,
                AVG(elapsed_time) as avg_time
            FROM tasks
            WHERE elapsed_time IS NOT NULL
            """
        )
        time_stats = dict(cursor.fetchone())

        # Get elapsed times for percentile calculation
        cursor.execute(
            """
            SELECT elapsed_time
            FROM tasks
            WHERE elapsed_time IS NOT NULL
            ORDER BY elapsed_time
            """
        )
        elapsed_times = [row["elapsed_time"] for row in cursor.fetchall()]

        # Calculate percentiles
        percentiles = {}
        if elapsed_times:
            import numpy as np

            elapsed_array = np.array(elapsed_times)
            percentiles = {
                "median": float(np.median(elapsed_array)),
                "p90": float(np.percentile(elapsed_array, 90)),
                "p95": float(np.percentile(elapsed_array, 95)),
                "std": float(np.std(elapsed_array)),
            }

        # Get exception counts
        cursor.execute(
            """
            SELECT exception, COUNT(*) as count
            FROM tasks
            WHERE exception IS NOT NULL
            GROUP BY exception
            ORDER BY count DESC
            """
        )
        exception_counts = {row["exception"]: row["count"] for row in cursor.fetchall()}

        # Get spot termination events
        cursor.execute(
            """
            SELECT DISTINCT hostname
            FROM events
            WHERE event_type = 'spot_termination'
            """
        )
        spot_terminations = [row["hostname"] for row in cursor.fetchall()]

        # Get time range
        cursor.execute(
            """
            SELECT
                MIN(enqueued_at) as start_time,
                MAX(completed_at) as end_time
            FROM tasks
            """
        )
        time_range = dict(cursor.fetchone())

        return {
            "time_stats": time_stats,
            "percentiles": percentiles,
            "elapsed_times": elapsed_times,
            "exception_counts": exception_counts,
            "spot_terminations": spot_terminations,
            "time_range": time_range,
        }

    def get_remaining_task_ids(self) -> List[str]:
        """
        Get task IDs of tasks that are not yet complete.

        A task is "remaining" if it has not reached a terminal state. Terminal
        states (reported back, no retry) are: completed, exception, timed_out,
        exited_without_status. Tasks with retry=True (exception_with_retry,
        timed_out_with_retry, exited_without_status_with_retry) are counted
        as remaining because they will be retried.

        Returns:
            List of task IDs
        """
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT task_id
            FROM tasks
            WHERE status NOT IN (
                'completed',
                'exception',
                'timed_out',
                'exited_without_status',
                'failed'
            )
            """
        )
        return [row["task_id"] for row in cursor.fetchall()]

    def close(self) -> None:
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None

    def __enter__(self) -> "TaskDatabase":
        """Context manager entry."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool | None:
        """Context manager exit."""
        self.close()
        return None
