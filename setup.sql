/*
DROP SCHEMA IF EXISTS ppe CASCADE;
*/
CREATE SCHEMA ppe;

CREATE TABLE ppe.task (
    task_id SERIAL PRIMARY KEY
,   task_name TEXT NOT NULL
,   cmd TEXT[] NULL
,   task_sql TEXT NULL
,   retries INT NOT NULL CHECK (retries >= 0)
,   timeout_seconds INT NULL CHECK (timeout_seconds IS NULL OR timeout_seconds > 0)
,   enabled BOOL NOT NULL DEFAULT TRUE
,   UNIQUE (task_name)
);

CREATE FUNCTION ppe.create_task(
    p_task_name TEXT
,   p_cmd TEXT[] = NULL
,   p_task_sql TEXT = NULL
,   p_retries INT = 0
,   p_enabled BOOL = TRUE
,   p_timeout_seconds INT = NULL
)
RETURNS INT
AS $$
DECLARE
    v_result INT;
BEGIN
    ASSERT length(p_task_name) > 0, 'p_task_name cannot be blank.';
    ASSERT p_cmd IS NULL OR cardinality(p_cmd) > 0, 'If p_cmd is used, then it must have at least 1 item.';
    ASSERT p_task_sql IS NULL OR length(p_task_sql) > 0, 'If p_task_sql is used, then it cannot be blank.';
    ASSERT p_cmd IS NOT NULL OR p_task_sql IS NOT NULL, 'Either p_cmd or p_task_sql must be provided';
    ASSERT p_timeout_seconds IS NULL OR p_timeout_seconds > 0, 'If p_timeout_seconds is provided, then it must be > 0.';

    WITH new_row_id AS (
        INSERT INTO ppe.task (
            task_name
        ,   cmd
        ,   task_sql
        ,   retries
        ,   timeout_seconds
        ,   enabled
        ) VALUES (
            p_task_name
        ,   p_cmd
        ,   p_task_sql
        ,   COALESCE(p_retries, 0)
        ,   p_timeout_seconds
        ,   COALESCE(p_enabled, TRUE)
        )
        RETURNING task_id
    )
    SELECT *
    INTO v_result
    FROM new_row_id;

    RETURN v_result;
END;
$$
LANGUAGE plpgsql;

CREATE TABLE ppe.task_dependency (
    task_id INT NOT NULL REFERENCES ppe.task (task_id)
,   dependency_task_id INT NOT NULL REFERENCES ppe.task(task_id)
,   PRIMARY KEY (task_id, dependency_task_id)
);

CREATE TABLE ppe.schedule (
    schedule_id SERIAL PRIMARY KEY
,   schedule_name TEXT NOT NULL
,   start_ts TIMESTAMPTZ(0) NOT NULL DEFAULT '1900-01-01 +0'
,   end_ts TIMESTAMPTZ(0) NOT NULL DEFAULT '9999-12-31 +0'
,   start_month INT NOT NULL DEFAULT 1 CHECK (start_month BETWEEN 1 AND 12)
,   end_month INT NOT NULL DEFAULT 12 CHECK (end_month BETWEEN 1 AND 12)
,   start_month_day INT NOT NULL DEFAULT 1 CHECK (start_month_day BETWEEN 1 AND 31)
,   end_month_day INT NOT NULL DEFAULT 31 CHECK (end_month_day BETWEEN 1 AND 31)
,   start_week_day INT NOT NULL DEFAULT 1 CHECK (start_week_day BETWEEN 1 AND 7) -- 1 = Monday
,   end_week_day INT NOT NULL DEFAULT 7 CHECK (end_week_day BETWEEN 1 AND 7) -- 7 = Sunday
,   start_hour INT NOT NULL DEFAULT 1 CHECK (start_hour BETWEEN 1 AND 23)
,   end_hour INT NOT NULL DEFAULT 23 CHECK (end_hour BETWEEN 1 AND 23)
,   start_minute INT NOT NULL DEFAULT 1 CHECK (start_minute BETWEEN 1 AND 59)
,   end_minute INT NOT NULL DEFAULT 59 CHECK (end_minute BETWEEN 1 AND 59)
,   min_seconds_between_attempts INT NOT NULL CHECK (min_seconds_between_attempts > 0)
,   UNIQUE (schedule_name)
);

CREATE FUNCTION ppe.create_schedule (
    p_schedule_name TEXT
,   p_min_seconds_between_attempts INT
,   p_start_ts TIMESTAMPTZ(0) = '1900-01-01 +0'
,   p_end_ts TIMESTAMPTZ(0) = '9999-12-31 +0'
,   p_start_month INT = 1
,   p_end_month INT = 12
,   p_start_month_day  INT = 1
,   p_end_month_day INT = 31
,   p_start_week_day INT = 1
,   p_end_week_day INT = 7
,   p_start_hour INT = 1
,   p_end_hour INT = 23
,   p_start_minute INT = 1
,   p_end_minute INT = 59
)
RETURNS INT
AS $$
DECLARE
    v_new_id INT;
BEGIN
    ASSERT p_start_month BETWEEN 1 AND 12, 'p_start_month must be between 1 and 12.';
    ASSERT p_end_month BETWEEN 1 AND 12, 'p_end_month must be between 1 and 12.';
    ASSERT p_end_month >= p_start_month, 'p_end_month must be >= p_start_month.';
    ASSERT p_start_month_day BETWEEN 1 AND 31, 'p_start_month_day must be between 1 and 31.';
    ASSERT p_end_month_day BETWEEN 1 AND 31, 'p_end_month_day must be between 1 and 31.';
    ASSERT p_end_month_day >= p_start_month_day, 'p_end_month_day must be >= p_start_month_day.';
    ASSERT p_start_week_day BETWEEN 1 AND 7, 'p_start_week_day must be between 1 and 7.';
    ASSERT p_end_week_day BETWEEN 1 AND 7, 'p_end_week_day must be between 1 and 7.';
    ASSERT p_end_week_day >= p_start_week_day, 'p_end_week_day must be >= p_start_week_day.';
    ASSERT p_start_hour BETWEEN 1 AND 23, 'p_start_hour must be between 1 and 23.';
    ASSERT p_end_hour BETWEEN 1 AND 23, 'p_end_hour must be between 1 and 23.';
    ASSERT p_end_hour >= p_start_hour, 'p_end_hour must be >= p_start_hour.';
    ASSERT p_start_minute BETWEEN 1 AND 59, 'p_start_minute must be between 1 and 59.';
    ASSERT p_end_minute BETWEEN 1 AND 59, 'p_end_minute must be between 1 and 59.';
    ASSERT p_end_minute >= p_start_minute, 'p_end_minute must be >= p_start_minute.';

    WITH new_row AS (
        INSERT INTO ppe.schedule (
            schedule_name
        ,   start_ts
        ,   end_ts
        ,   start_month
        ,   end_month
        ,   start_month_day
        ,   end_month_day
        ,   start_week_day
        ,   end_week_day
        ,   start_hour
        ,   end_hour
        ,   start_minute
        ,   end_minute
        ,   min_seconds_between_attempts
        ) VALUES (
            p_schedule_name
        ,   p_start_ts
        ,   p_end_ts
        ,   p_start_month
        ,   p_end_month
        ,   p_start_month_day
        ,   p_end_month_day
        ,   p_start_week_day
        ,   p_end_week_day
        ,   p_start_hour
        ,   p_end_hour
        ,   p_start_minute
        ,   p_end_minute
        ,   p_min_seconds_between_attempts
        )
        RETURNING schedule_id
    )
    SELECT *
    INTO v_new_id
    FROM new_row;

    RETURN v_new_id;
END;
$$
LANGUAGE plpgsql;

CREATE TABLE ppe.task_schedule (
    task_id INT NOT NULL REFERENCES ppe.task (task_id)
,   schedule_id INT NOT NULL REFERENCES ppe.schedule (schedule_id)
,   PRIMARY KEY (task_id, schedule_id)
);

CREATE PROCEDURE ppe.schedule_task (
    p_task_id INT
,   p_schedule_id INT
)
AS $$
BEGIN
    INSERT INTO ppe.task_schedule (task_id, schedule_id)
    VALUES (p_task_id, p_schedule_id);
END;
$$
LANGUAGE plpgsql;

CREATE TABLE ppe.batch (
    batch_id SERIAL PRIMARY KEY
,   ts TIMESTAMPTZ(0) NOT NULL DEFAULT now()
);

CREATE FUNCTION ppe.create_batch ()
RETURNS INT
AS $$
    INSERT INTO ppe.batch (ts)
    VALUES (DEFAULT)
    RETURNING batch_id;
$$
LANGUAGE sql;

CREATE TABLE ppe.job (
    job_id SERIAL PRIMARY KEY
,   batch_id INT NOT NULL REFERENCES ppe.batch (batch_id)
,   task_id INT NOT NULL REFERENCES ppe.task (task_id)
,   ts TIMESTAMPTZ(0) NOT NULL DEFAULT now()
);
CREATE INDEX ix_job_task_id_ts ON ppe.job (task_id, ts DESC);
CREATE INDEX ix_job_batch_id ON ppe.job (batch_id);

CREATE FUNCTION ppe.create_job (
    p_batch_id INT
,   p_task_id INT
)
    RETURNS INT
AS $$
    INSERT INTO ppe.job (batch_id, task_id)
    VALUES (p_batch_id, p_task_id)
    RETURNING job_id;
$$
LANGUAGE sql;

CREATE TABLE ppe.batch_error (
    id SERIAL PRIMARY KEY
,   batch_id INT NOT NULL REFERENCES ppe.batch (batch_id)
,   message TEXT NOT NULL CHECK (length(trim(message)) > 0)
,   ts TIMESTAMPTZ(0) NOT NULL DEFAULT now()
);

CREATE TABLE ppe.batch_info (
    id SERIAL PRIMARY KEY
,   batch_id INT NOT NULL REFERENCES ppe.batch (batch_id)
,   message TEXT NOT NULL CHECK (length(trim(message)) > 0)
,   ts TIMESTAMPTZ(0) NOT NULL DEFAULT now()
);

CREATE TABLE ppe.job_cancel (
    job_id INT PRIMARY KEY REFERENCES ppe.job (job_id)
,   reason TEXT NOT NULL CHECK (length(trim(reason)) > 0)
,   ts TIMESTAMPTZ(0) NOT NULL DEFAULT now()
);

CREATE TABLE ppe.job_failure (
    id SERIAL PRIMARY KEY
,   job_id INT NOT NULL REFERENCES ppe.job (job_id)
,   message TEXT NOT NULL CHECK (length(trim(message)) > 0)
,   ts TIMESTAMPTZ(0) NOT NULL DEFAULT now()
);

CREATE TABLE ppe.job_info (
    id SERIAL PRIMARY KEY
,   job_id INT NOT NULL REFERENCES ppe.job (job_id)
,   message TEXT NOT NULL CHECK (length(trim(message)) > 0)
,   ts TIMESTAMPTZ(0) NOT NULL DEFAULT now()
);

CREATE TABLE ppe.job_skip (
    job_id INT PRIMARY KEY REFERENCES ppe.job (job_id)
,   reason TEXT NOT NULL CHECK (length(trim(reason)) > 0)
,   ts TIMESTAMPTZ(0) NOT NULL DEFAULT now()
);

CREATE TABLE ppe.job_success (
    id SERIAL PRIMARY KEY
,   job_id INT NOT NULL REFERENCES ppe.job (job_id)
,   execution_millis BIGINT NOT NULL CHECK (execution_millis >= 0)
,   ts TIMESTAMPTZ(0) NOT NULL DEFAULT now()
);

CREATE PROCEDURE ppe.cancel_running_jobs(
    p_reason TEXT
) AS
$$
    INSERT INTO ppe.job_cancel (
        job_id
    ,   reason
    )
    SELECT
        j.job_id
    ,   p_reason
    FROM ppe.job AS j
    WHERE
        NOT EXISTS (
            SELECT 1
            FROM ppe.job_cancel AS c
            WHERE
                j.job_id = c.job_id
        )
        AND NOT EXISTS (
            SELECT 1
            FROM ppe.job_failure AS f
            WHERE
                j.job_id = f.job_id
        )
        AND NOT EXISTS (
            SELECT 1
            FROM ppe.job_skip AS s
            WHERE
            j.job_id = s.job_id
        )
        AND NOT EXISTS (
            SELECT 1
            FROM ppe.job_success AS e
            WHERE
            j.job_id = e.job_id
        )
    ;
$$
LANGUAGE sql;

CREATE PROCEDURE ppe.job_completed_successfully (
    p_job_id INT
,   p_execution_millis BIGINT
)
AS $$
    INSERT INTO ppe.job_success (job_id, execution_millis)
    VALUES (p_job_id, p_execution_millis);
$$
LANGUAGE sql;

CREATE PROCEDURE ppe.log_batch_error(
    p_batch_id INT
,   p_message TEXT
) AS $$
BEGIN
    ASSERT length(trim(p_message)) > 0, 'p_message cannot be blank.';

    INSERT INTO ppe.batch_error (batch_id, message)
    VALUES (p_batch_id, p_message);
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE ppe.log_batch_info(
    p_batch_id INT
,   p_message TEXT
) AS $$
BEGIN
    ASSERT length(trim(p_message)) > 0, 'p_message cannot be blank.';

    INSERT INTO ppe.batch_info (batch_id, message)
    VALUES (p_batch_id, p_message);
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE ppe.job_cancelled(
    p_job_id INT
,   p_reason TEXT
) AS $$
BEGIN
    ASSERT length(trim(p_reason)) > 0, 'p_reason cannot be blank.';

    INSERT INTO ppe.job_cancel (job_id, reason)
    VALUES (p_job_id, p_reason);
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE ppe.job_failed(
    p_job_id INT
,   p_message TEXT
) AS $$
BEGIN
    ASSERT length(trim(p_message)) > 0, 'p_message cannot be blank.';

    INSERT INTO ppe.job_failure (job_id, message)
    VALUES (p_job_id, p_message);
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE ppe.log_job_info(
    p_job_id INT
,   p_message TEXT
) AS $$
BEGIN
    ASSERT length(trim(p_message)) > 0, 'p_message cannot be blank.';

    INSERT INTO ppe.job_info (job_id, message)
    VALUES (p_job_id, p_message);
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ppe.get_ready_tasks (
    p_max_jobs INT
)
RETURNS TABLE (
    task_id INT
,   task_name TEXT
,   cmd TEXT[]
,   task_sql TEXT
,   retries INT
,   timeout_seconds INT
)
AS $$
    WITH latest_attempts AS (
        SELECT DISTINCT ON (s.task_id)
            s.task_id
        ,   s.job_id
        ,   s.ts
        ,   t.timeout_seconds
        FROM ppe.job AS s
        JOIN ppe.task AS t
            ON s.task_id = t.task_id
        ORDER BY
            s.task_id
        ,   s.ts DESC
    )
    , latest_completions AS (
        SELECT
            t.job_id
        ,   t.ts
        FROM (
            SELECT
                f.job_id
            ,   f.ts
            FROM ppe.job_cancel AS f
            JOIN latest_attempts AS la
                ON f.job_id = la.job_id

            UNION ALL

            SELECT
                f.job_id
            ,   f.ts
            FROM ppe.job_failure AS f
            JOIN latest_attempts AS la
                ON f.job_id = la.job_id

            UNION ALL

            SELECT
                s.job_id
            ,   s.ts
            FROM ppe.job_success AS s
            JOIN latest_attempts AS la
                ON s.job_id = la.job_id

            UNION ALL

            SELECT
                s.job_id
            ,   s.ts
            FROM ppe.job_skip AS s
            JOIN latest_attempts AS la
                ON s.job_id = la.job_id
        ) AS t
        ORDER BY
            t.job_id
        ,   t.ts DESC
    )
    , running_jobs AS (
        SELECT
            j.task_id
        ,   j.job_id
        FROM latest_attempts AS j
        WHERE
            EXTRACT(EPOCH FROM NOW() - j.ts) < (j.timeout_seconds + 10)
            AND NOT EXISTS (
                SELECT 1
                FROM latest_completions AS lc
                WHERE j.job_id = lc.job_id
            )
    )
    , ready_jobs AS (
        SELECT DISTINCT ON (t.task_id)
            t.task_id
        ,   t.task_name
        ,   t.cmd
        ,   t.task_sql
        ,   t.retries
        ,   la.ts AS latest_attempt
        ,   EXTRACT(EPOCH FROM now() - la.ts) AS seconds_since_latest_attempt
        ,   s.min_seconds_between_attempts
        ,   t.timeout_seconds
        FROM ppe.task AS t
        JOIN ppe.task_schedule AS ts -- 1..m
            ON t.task_id = ts.task_id
        JOIN ppe.schedule AS s
             ON ts.schedule_id = s.schedule_id
        LEFT JOIN latest_attempts AS la
            ON t.task_id = la.task_id
        LEFT JOIN latest_completions AS lc
            ON la.job_id = lc.job_id
        WHERE
            t.enabled
            AND now() BETWEEN s.start_ts AND s.end_ts
            AND EXTRACT(MONTH FROM now()) BETWEEN s.start_month AND s.end_month
            AND EXTRACT(ISODOW FROM now()) BETWEEN s.start_week_day AND s.end_week_day
            AND EXTRACT(DAY FROM now()) BETWEEN s.start_month_day AND s.end_month_day
            AND EXTRACT(HOUR FROM now()) BETWEEN s.start_hour AND s.end_hour
            AND EXTRACT(MINUTE FROM now()) BETWEEN s.start_minute AND s.end_minute
            AND (
                NOT EXISTS (
                    SELECT 1
                    FROM running_jobs AS rj
                    WHERE la.task_id = rj.task_id
                )
                OR EXTRACT(EPOCH FROM now() - la.ts) > t.timeout_seconds + 10
            )
            AND (
                EXTRACT(EPOCH FROM now() - lc.ts) > s.min_seconds_between_attempts
                OR lc.job_id IS NULL
            )
        ORDER BY
            t.task_id
        ,   la.ts DESC
    )
    SELECT
        rj.task_id
    ,   rj.task_name
    ,   rj.cmd
    ,   rj.task_sql
    ,   rj.retries
    ,   rj.timeout_seconds
    FROM ready_jobs AS rj
    , LATERAL (
        VALUES (
            rj.seconds_since_latest_attempt/3600
        ,   rj.seconds_since_latest_attempt::float/rj.min_seconds_between_attempts
        )
    ) AS c (
        hours_since_last_attempt
    ,   factor
    )
    ORDER BY
        c.hours_since_last_attempt DESC
    ,   c.factor DESC
    ,   rj.latest_attempt
    LIMIT GREATEST((p_max_jobs - (SELECT COUNT(*) FROM running_jobs)), 0);
$$
LANGUAGE sql;

CREATE OR REPLACE FUNCTION ppe.delete_old_log_entries(
    p_days_to_keep INT = 3
)
RETURNS BIGINT
AS $$
    DECLARE
        v_cutoff TIMESTAMPTZ(0) = now() - make_interval(days := COALESCE(p_days_to_keep, 3));
        v_rows_deleted BIGINT := 0;
    BEGIN
        RAISE NOTICE 'v_cutoff: %', v_cutoff;

        WITH batches AS (
            DELETE FROM ppe.batch AS b
            WHERE
                b.ts < v_cutoff
                -- don't delete the most recent batch
                AND EXISTS (
                    SELECT 1
                    FROM ppe.batch AS b
                    WHERE b.ts > b.ts
                )
            RETURNING batch_id
        )
        , batch_error AS (
            DELETE FROM ppe.batch_error AS b
            WHERE
                EXISTS (
                    SELECT 1
                    FROM batches AS d
                    WHERE
                        b.batch_id = d.batch_id
                )
                OR b.ts < v_cutoff
            RETURNING batch_id
        )
        , batch_info AS (
            DELETE FROM ppe.batch_info AS b
            WHERE
                EXISTS (
                    SELECT 1
                    FROM batches AS d
                    WHERE
                        b.batch_id = d.batch_id
                )
                OR b.ts < v_cutoff
            RETURNING batch_id
        )
        , jobs AS (
            DELETE FROM ppe.job AS j
            WHERE
                j.ts < v_cutoff
                OR EXISTS (
                    SELECT 1
                    FROM batches AS b
                    WHERE j.batch_id = b.batch_id
                )
            RETURNING job_id
        )
        , job_failures AS (
            DELETE FROM ppe.job_failure AS f
            WHERE
                EXISTS (
                    SELECT 1
                    FROM jobs AS j
                    WHERE f.job_id = j.job_id
                )
            RETURNING 1
        )
        , job_skips AS (
            DELETE FROM ppe.job_skip AS s
            WHERE
                EXISTS (
                    SELECT 1
                    FROM jobs AS j
                    WHERE s.job_id = j.job_id
                )
            RETURNING 1
        )
        , job_successes AS (
            DELETE FROM ppe.job_success AS s
            WHERE
                EXISTS (
                    SELECT 1
                    FROM jobs AS j
                    WHERE s.job_id = j.job_id
                )
            RETURNING 1
        )
        , job_infos AS (
            DELETE FROM ppe.job_info AS i
            WHERE
                EXISTS (
                    SELECT 1
                    FROM jobs AS j
                    WHERE i.job_id = j.job_id
                )
            RETURNING 1
        )
        SELECT
            (SELECT COUNT(*) FROM batches)
        +   (SELECT COUNT(*) FROM batch_error)
        +   (SELECT COUNT(*) FROM batch_info)
        +   (SELECT COUNT(*) FROM jobs)
        +   (SELECT COUNT(*) FROM job_failures)
        +   (SELECT COUNT(*) FROM job_skips)
        +   (SELECT COUNT(*) FROM job_successes)
        +   (SELECT COUNT(*) FROM job_infos)
        INTO v_rows_deleted;

        RETURN v_rows_deleted;
    END;
$$
LANGUAGE plpgsql;
