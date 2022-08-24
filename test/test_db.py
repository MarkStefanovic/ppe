from psycopg2.pool import ThreadedConnectionPool

from src import adapter


def test_cancel_running_jobs(pool_fixture: ThreadedConnectionPool):
    with pool_fixture.getconn() as con:
        with con.cursor() as cur:
            cur.execute("""
                INSERT INTO ppe.batch (batch_id) OVERRIDING SYSTEM VALUE VALUES (1);
                INSERT INTO ppe.task (task_id, task_name, task_sql, retries, timeout_seconds) OVERRIDING SYSTEM VALUE VALUES (1, 'test_task', 'SELECT 1', 1, 60);
                INSERT INTO ppe.job (job_id, batch_id, task_id) OVERRIDING SYSTEM VALUE VALUES (1, 1, 1);
                INSERT INTO ppe.job (job_id, batch_id, task_id) OVERRIDING SYSTEM VALUE VALUES (2, 1, 1);
            """)
            cur.execute("SELECT COUNT(*) FROM ppe.job;")
            ppe_jobs = cur.fetchone()[0]
            assert ppe_jobs == 2, f"Expected 2 jobs in ppe.job, but there were {ppe_jobs}."

    db = adapter.db.open_db(batch_id=1, pool=pool_fixture, days_logs_to_keep=3)
    db.cancel_running_jobs(reason="Testing")
    with pool_fixture.getconn() as con:
        with con.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM ppe.job_cancel;")
            cancelled_jobs = cur.fetchone()[0]
            assert cancelled_jobs == 2, f"Expected 2 job in ppe.job_cancel after cancel_running_jobs, but there were {cancelled_jobs}."


def test_get_ready_job(pool_fixture: ThreadedConnectionPool):
    with pool_fixture.getconn() as con:
        with con.cursor() as cur:
            cur.execute("""
                INSERT INTO ppe.batch (batch_id) OVERRIDING SYSTEM VALUE VALUES (1);
                INSERT INTO ppe.task (task_id, task_name, task_sql, retries, timeout_seconds) OVERRIDING SYSTEM VALUE VALUES (1, 'test_task', 'SELECT 1', 1, 60);
                INSERT INTO ppe.task_queue (task_id, task_name, cmd, task_sql, retries, timeout_seconds, latest_attempt_ts)
                VALUES (1, 'test_task', NULL, 'SELECT 1', 1, 60, '2010-01-02 03:04 +0');
            """)
            cur.execute("SELECT COUNT(*) FROM ppe.task_queue;")
            queued_tasks = cur.fetchone()[0]
            assert queued_tasks == 1, f"Expected 2 tasks in ppe.task_queue, but there were {queued_tasks}."

    db = adapter.db.open_db(batch_id=1, pool=pool_fixture, days_logs_to_keep=3)
    ready_job = db.get_ready_job()
    assert ready_job.task.name == "test_task"


def test_update_queue(pool_fixture: ThreadedConnectionPool):
    with pool_fixture.getconn() as con:
        with con.cursor() as cur:
            cur.execute("""
                INSERT INTO ppe.batch (batch_id) OVERRIDING SYSTEM VALUE VALUES (1);
                INSERT INTO ppe.task (task_id, task_name, task_sql, retries, timeout_seconds) OVERRIDING SYSTEM VALUE VALUES (1, 'test_task', 'SELECT 1', 1, 60);
                INSERT INTO ppe.schedule (schedule_id, schedule_name, min_seconds_between_attempts) OVERRIDING SYSTEM VALUE VALUES (1, 'every 10 seconds', 10);
                INSERT INTO ppe.task_schedule (task_id, schedule_id) VALUES (1, 1);
            """)
            cur.execute("SELECT COUNT(*) FROM ppe.task;")
            tasks = cur.fetchone()[0]
            assert tasks == 1, f"Expected 1 task in ppe.task, but there were {tasks}."

    db = adapter.db.open_db(batch_id=1, pool=pool_fixture, days_logs_to_keep=3)
    db.update_queue()
    with pool_fixture.getconn() as con:
        with con.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM ppe.task_queue;")
            queued_tasks = cur.fetchone()[0]
            assert queued_tasks == 1, f"Expected 1 job in ppe.task_queue, but there were {queued_tasks}."
