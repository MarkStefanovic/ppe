from psycopg2.pool import ThreadedConnectionPool

from src.adapter.db import open_db


def test_cancel_running_jobs(pool_fixture: ThreadedConnectionPool):
    with pool_fixture.getconn() as con:
        with con.cursor() as cur:
            cur.execute("""
                INSERT INTO ppe.batch (batch_id) OVERRIDING SYSTEM VALUE VALUES (-1);
                INSERT INTO ppe.task (task_id, task_name, task_sql, retries, timeout_seconds) OVERRIDING SYSTEM VALUE VALUES (-1, 'test_task', 'SELECT 1', 1, 60);
                INSERT INTO ppe.job (job_id, batch_id, task_id) OVERRIDING SYSTEM VALUE VALUES (-1, -1, -1);
                INSERT INTO ppe.job (job_id, batch_id, task_id) OVERRIDING SYSTEM VALUE VALUES (-2, -1, -1);
            """)
            cur.execute("SELECT COUNT(*) FROM ppe.job;")
            ppe_jobs = cur.fetchone()[0]
            assert ppe_jobs == 2, f"Expected 2 jobs in ppe.job, but there were {ppe_jobs}."

    db = open_db(pool=pool_fixture)
    db.cancel_running_jobs(reason="Testing")
    with pool_fixture.getconn() as con:
        with con.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM ppe.job_cancel;")
            cancelled_jobs = cur.fetchone()[0]
            assert cancelled_jobs == 2, f"Expected 2 job in ppe.job_cancel after cancel_running_jobs, but there were {cancelled_jobs}."


