/*
DROP SCHEMA IF EXISTS ppe CASCADE;
*/
CREATE SCHEMA ppe;

CREATE TABLE ppe.task (
    task_id SERIAL PRIMARY KEY
,   task_name TEXT NOT NULL CHECK (length(trim(task_name)) > 0)
,   tool TEXT NULL CHECK (tool IS NULL OR length(trim(tool)) > 0)
,   tool_args TEXT[] NULL
,   task_sql TEXT NULL CHECK (task_sql IS NULL OR length(trim(task_sql)) > 0)
,   retries INT NOT NULL CHECK (retries >= 0)
,   timeout_seconds INT NULL CHECK (timeout_seconds IS NULL OR timeout_seconds > 0)
,   enabled BOOL NOT NULL DEFAULT TRUE
,   UNIQUE (task_name)
);

CREATE FUNCTION ppe.create_task(
    p_task_name TEXT
,   p_tool TEXT = NULL
,   p_tool_args TEXT[] = NULL
,   p_task_sql TEXT = NULL
,   p_retries INT = 0
,   p_enabled BOOL = TRUE
,   p_timeout_seconds INT = NULL
)
RETURNS INT
AS $$
DECLARE
    v_result INT;
    v_tool_args TEXT[];
BEGIN
    ASSERT p_tool IS NOT NULL OR p_task_sql IS NOT NULL, 'Either p_tool or p_task_sql must be provided';
    ASSERT p_timeout_seconds IS NULL OR p_timeout_seconds > 0, 'If p_timeout_seconds is provided, then it must be > 0.';

    IF p_tool IS NULL THEN
        v_tool_args = NULL;
    ELSE
        v_tool_args =
            CASE
                WHEN p_tool_args IS NULL THEN NULL
                WHEN cardinality(p_tool_args) = 0 THEN NULL
                ELSE p_tool_args
            END;
    END IF;

    WITH new_row_id AS (
        INSERT INTO ppe.task (
            task_name
        ,   tool
        ,   tool_args
        ,   task_sql
        ,   retries
        ,   timeout_seconds
        ,   enabled
        ) VALUES (
            p_task_name
        ,   p_tool
        ,   v_tool_args
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

CREATE TABLE ppe.resource (
    resource_id SERIAL PRIMARY KEY
,   resource_name TEXT NOT NULL
,   capacity INT NOT NULL CHECK (capacity > 0)
,   enable_flag BOOL NOT NULL DEFAULT TRUE
,   UNIQUE (resource_name)
);

CREATE FUNCTION ppe.create_resource(
    p_name TEXT
,   p_capacity INT
)
RETURNS INT
LANGUAGE plpgsql
AS $$
DECLARE v_resource_id INT;
BEGIN
    ASSERT length(p_name) > 0, 'p_name cannot be blank.';
    ASSERT p_capacity > 0, 'p_max_allowed must be > 0.';

    WITH ins AS (
        INSERT INTO ppe.resource (resource_name, capacity)
        VALUES (p_name, p_capacity)
        RETURNING resource_id
    )
    SELECT resource_id
    INTO v_resource_id
    FROM ins;

    RETURN v_resource_id;
END;
$$;

CREATE TABLE ppe.task_resource (
    task_id INT NOT NULL REFERENCES ppe.task (task_id)
,   resource_id INT NOT NULL REFERENCES ppe.resource (resource_id)
,   units INT CHECK (units > 0)
,   PRIMARY KEY (task_id, resource_id)
);

CREATE PROCEDURE ppe.assign_resource_to_task (
    p_task_id INT
,   p_resource_id INT
,   p_units INT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_capacity INT;
BEGIN
    ASSERT p_units > 0, 'p_units must be > 0.';

    v_capacity = (SELECT r.capacity FROM ppe.resource AS r WHERE r.resource_id = p_resource_id);
    ASSERT p_units < v_capacity, FORMAT('p_units (%s) is more than the resource''s capacity (%s).', p_units, v_capacity);

    INSERT INTO ppe.task_resource (task_id, resource_id, units)
    VALUES (p_task_id, p_resource_id, p_units);
END;
$$;

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

CREATE OR REPLACE FUNCTION ppe.create_job (
    p_batch_id INT
,   p_task_id INT
)
RETURNS INT
LANGUAGE plpgsql
AS $$
DECLARE
    v_job_id INT;
BEGIN
    ASSERT p_batch_id IS NOT NULL, 'p_batch_id cannot be null.';
    ASSERT p_task_id > 0, 'p_task_id must be >= 0.';

    DELETE FROM ppe.task_queue AS q WHERE q.task_id = p_task_id;

    WITH ins AS (
        INSERT INTO ppe.job (batch_id, task_id)
        VALUES (p_batch_id, p_task_id)
        RETURNING job_id
    )
    SELECT job_id
    INTO v_job_id
    FROM ins;

    ASSERT v_job_id IS NOT NULL;

    RETURN v_job_id;
END;
$$;

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

CREATE TABLE ppe.latest_task_attempt (
    task_id INT PRIMARY KEY REFERENCES ppe.task (task_id)
,   job_id INT NOT NULL REFERENCES ppe.job (job_id)
,   start_ts TIMESTAMPTZ(0) NOT NULL DEFAULT now()
,   UNIQUE (job_id)
);

CREATE TABLE ppe.job_complete (
    job_id INT PRIMARY KEY REFERENCES ppe.job (job_id)
,   ts TIMESTAMPTZ(0) NOT NULL DEFAULT now()
);

CREATE TABLE ppe.task_running (
    task_id INT PRIMARY KEY REFERENCES ppe.task (task_id)
,   job_id INT NOT NULL REFERENCES ppe.job (job_id)
,   start_ts TIMESTAMPTZ(0) NOT NULL
);

CREATE TABLE ppe.task_queue (
    task_id INT PRIMARY KEY REFERENCES ppe.task (task_id)
,   task_name TEXT NOT NULL
,   tool TEXT NULL
,   tool_args TEXT[] NULL
,   task_sql TEXT NULL
,   retries INT NOT NULL
,   timeout_seconds INT NOT NULL
,   latest_attempt_ts TIMESTAMPTZ(0) NULL
,   ts TIMESTAMPTZ(0) NOT NULL DEFAULT now()
,   UNIQUE (task_name)
);

CREATE TABLE ppe.resource_status (
    resource_id INT PRIMARY KEY REFERENCES ppe.resource (resource_id)
,   capacity INT NOT NULL
,   reserved INT NOT NULL
,   available INT NOT NULL
);

CREATE OR REPLACE PROCEDURE ppe.update_queue ()
LANGUAGE plpgsql
AS $$
BEGIN
    SET TIME ZONE 'UTC';

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
    INSERT INTO ppe.latest_task_attempt (
        task_id
    ,   job_id
    ,   start_ts
    )
    SELECT
        s.task_id
    ,   s.job_id
    ,   s.ts AS start_ts
    FROM latest_attempts AS s
    ON CONFLICT (task_id)
    DO UPDATE SET
        job_id = EXCLUDED.job_id
    ,   start_ts = EXCLUDED.start_ts
    WHERE
        (ppe.latest_task_attempt.job_id, ppe.latest_task_attempt.start_ts) <> (EXCLUDED.job_id, EXCLUDED.start_ts)
    ;

    WITH latest_job_completions AS (
        SELECT
            t.job_id
        ,   t.ts
        FROM (
            SELECT
                f.job_id
            ,   f.ts
            FROM ppe.job_cancel AS f
            JOIN ppe.latest_task_attempt AS lta
                ON f.job_id = lta.job_id

            UNION ALL

            SELECT
                f.job_id
            ,   f.ts
            FROM ppe.job_failure AS f
            JOIN ppe.latest_task_attempt AS lta
                ON f.job_id = lta.job_id

            UNION ALL

            SELECT
                s.job_id
            ,   s.ts
            FROM ppe.job_success AS s
            JOIN ppe.latest_task_attempt AS lta
                ON s.job_id = lta.job_id

            UNION ALL

            SELECT
                s.job_id
            ,   s.ts
            FROM ppe.job_skip AS s
            JOIN ppe.latest_task_attempt AS lta
                ON s.job_id = lta.job_id
        ) AS t
        ORDER BY
            t.job_id
        ,   t.ts DESC
    )
    INSERT INTO ppe.job_complete (
        job_id
    ,   ts
    )
    SELECT
        ljc.job_id
    ,   ljc.ts
    FROM latest_job_completions AS ljc
    ON CONFLICT (job_id)
    DO UPDATE SET
        ts = EXCLUDED.ts
    WHERE
        ppe.job_complete.ts <> EXCLUDED.ts
    ;

    TRUNCATE ppe.task_running;
    INSERT INTO ppe.task_running (
        task_id
    ,   job_id
    ,   start_ts
    )
    SELECT
        lta.task_id
    ,   lta.job_id
    ,   lta.start_ts
    FROM ppe.latest_task_attempt AS lta
    JOIN ppe.task AS t
        ON lta.task_id = t.task_id
    WHERE
        NOT EXISTS (
            SELECT 1
            FROM ppe.job_complete AS jc
            WHERE
                lta.job_id = jc.job_id
        )
        AND EXTRACT(EPOCH FROM now() - lta.start_ts) <= t.timeout_seconds
    ;

    TRUNCATE ppe.resource_status;
    WITH running_job_resources AS (
        SELECT
            tr.resource_id
        ,   SUM(tr.units) AS units_in_use
        FROM ppe.task_running AS rj
        JOIN ppe.task_resource AS tr
            ON rj.task_id = tr.task_id
        GROUP BY
            tr.resource_id
    )
    INSERT INTO ppe.resource_status (
        resource_id
    ,   capacity
    ,   reserved
    ,   available
    )
    SELECT
        r.resource_id
    ,   r.capacity
    ,   COALESCE(rjr.units_in_use, 0) AS reserved
    ,   r.capacity - COALESCE(rjr.units_in_use, 0) AS available
    FROM ppe.resource AS r
    LEFT JOIN running_job_resources AS rjr
        ON r.resource_id = rjr.resource_id
    ;

    TRUNCATE ppe.task_queue;
    INSERT INTO ppe.task_queue (
        task_id
    ,   task_name
    ,   tool
    ,   tool_args
    ,   task_sql
    ,   retries
    ,   timeout_seconds
    ,   latest_attempt_ts
    )
    SELECT DISTINCT ON (t.task_id)
        t.task_id
    ,   t.task_name
    ,   t.tool
    ,   t.tool_args
    ,   t.task_sql
    ,   t.retries
    ,   t.timeout_seconds
    ,   lta.start_ts AS latest_attempt_ts
    FROM ppe.task AS t
    JOIN ppe.task_schedule AS ts -- 1..m
        ON t.task_id = ts.task_id
    JOIN ppe.schedule AS s
         ON ts.schedule_id = s.schedule_id
    LEFT JOIN ppe.latest_task_attempt AS lta
        ON t.task_id = lta.task_id
    LEFT JOIN ppe.job_complete AS ltc
        ON lta.job_id = ltc.job_id
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
                FROM ppe.task_running AS tr
                WHERE lta.task_id = tr.task_id
            )
            OR EXTRACT(EPOCH FROM now() - lta.start_ts) > t.timeout_seconds + 60
        )
        AND (
            EXTRACT(EPOCH FROM now() - ltc.ts) > s.min_seconds_between_attempts
            OR ltc.job_id IS NULL
        )
        AND NOT EXISTS (
            SELECT 1
            FROM ppe.resource_status AS rs
            JOIN ppe.task_resource AS tr
                ON t.task_id = tr.task_id
            WHERE
                rs.resource_id = tr.resource_id
                AND rs.available <= 0
        )
    ORDER BY
        t.task_id
    ,   lta.start_ts DESC
    ;
END;
$$;

CREATE OR REPLACE FUNCTION ppe.get_ready_task ()
RETURNS TABLE (
    task_id INT
,   task_name TEXT
,   tool TEXT
,   tool_args TEXT[]
,   task_sql TEXT
,   retries INT
,   timeout_seconds INT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_task_id INT;
BEGIN
    v_task_id = (
        SELECT
            q.task_id
        FROM ppe.task_queue AS q
        ORDER BY
            q.latest_attempt_ts
        ,   q.ts
        ,   random()
        LIMIT 1
    );

    IF v_task_id IS NOT NULL THEN
        DELETE FROM ppe.task_queue AS q
        WHERE q.task_id = v_task_id;

        RETURN QUERY
        SELECT
            t.task_id
        ,   t.task_name
        ,   t.tool
        ,   t.tool_args
        ,   t.task_sql
        ,   t.retries
        ,   t.timeout_seconds
        FROM ppe.task AS t
        WHERE
            t.task_id = v_task_id;
    END IF;
END;
$$;

CREATE OR REPLACE PROCEDURE ppe.delete_old_log_entries(
    p_current_batch_id INT
,   p_days_to_keep INT = 3
)
AS $$
DECLARE
    v_cutoff TIMESTAMPTZ(0) = now() - make_interval(days := COALESCE(p_days_to_keep, 3));
BEGIN
    RAISE NOTICE 'v_cutoff: %', v_cutoff;

    DROP TABLE IF EXISTS tmp_ppe_jobs_to_delete;
    CREATE TEMPORARY TABLE tmp_ppe_jobs_to_delete (
        job_id INT PRIMARY KEY
    );

    INSERT INTO tmp_ppe_jobs_to_delete (job_id)
    SELECT j.job_id
    FROM ppe.job AS j
    WHERE j.ts < v_cutoff;

    DELETE FROM ppe.latest_task_attempt AS lta
    WHERE EXISTS (
        SELECT 1
        FROM tmp_ppe_jobs_to_delete AS tmp
        WHERE lta.job_id = tmp.job_id
    );

    DELETE FROM ppe.job_complete AS ji
    WHERE EXISTS (
        SELECT 1
        FROM tmp_ppe_jobs_to_delete AS tmp
        WHERE ji.job_id = tmp.job_id
    );

    DELETE FROM ppe.job_info AS ji
    WHERE EXISTS (
        SELECT 1
        FROM tmp_ppe_jobs_to_delete AS tmp
        WHERE ji.job_id = tmp.job_id
    );

    DELETE FROM ppe.job_failure AS jf
    WHERE EXISTS (
        SELECT 1
        FROM tmp_ppe_jobs_to_delete AS tmp
        WHERE jf.job_id = tmp.job_id
    );

    DELETE FROM ppe.job_skip AS js
    WHERE EXISTS (
        SELECT 1
        FROM tmp_ppe_jobs_to_delete AS tmp
        WHERE js.job_id = tmp.job_id
    );

    DELETE FROM ppe.job_success AS js
    WHERE EXISTS (
        SELECT 1
        FROM tmp_ppe_jobs_to_delete AS tmp
        WHERE js.job_id = tmp.job_id
    );

    DELETE FROM ppe.task_running AS tr
    WHERE EXISTS (
        SELECT 1
        FROM tmp_ppe_jobs_to_delete AS tmp
        WHERE tr.job_id = tmp.job_id
    );

    DELETE FROM ppe.job AS j
    WHERE EXISTS (
        SELECT 1
        FROM tmp_ppe_jobs_to_delete AS tmp
        WHERE j.job_id = tmp.job_id
    );

    DROP TABLE IF EXISTS tmp_ppe_batches_to_delete;
    CREATE TEMPORARY TABLE tmp_ppe_batches_to_delete (
        batch_id INT PRIMARY KEY
    );

    INSERT INTO tmp_ppe_batches_to_delete (batch_id)
    SELECT DISTINCT
        b.batch_id
    FROM ppe.batch AS b
    WHERE
        b.batch_id <> p_current_batch_id
        AND NOT EXISTS (
            SELECT 1
            FROM ppe.job AS j
            WHERE b.batch_id = j.batch_id
        )
    ;

    DELETE FROM ppe.batch_error AS be
    WHERE EXISTS (
        SELECT 1
        FROM tmp_ppe_batches_to_delete tmp
        WHERE be.batch_id = tmp.batch_id
    );

    DELETE FROM ppe.batch_info AS bi
    WHERE EXISTS (
        SELECT 1
        FROM tmp_ppe_batches_to_delete tmp
        WHERE bi.batch_id = tmp.batch_id
    );

    DELETE FROM ppe.batch AS b
    WHERE EXISTS (
        SELECT 1
        FROM tmp_ppe_batches_to_delete tmp
        WHERE b.batch_id = tmp.batch_id
    );

END;
$$
LANGUAGE plpgsql;

CREATE TYPE ppe.task_issue_severity_option AS ENUM ('HIGH', 'MED', 'LOW');
CREATE TABLE ppe.task_issue_type (
    task_issue_type_id INT PRIMARY KEY
,   description TEXT NOT NULL
,   severity ppe.task_issue_severity_option NOT NULL
,   enabled BOOL NOT NULL DEFAULT TRUE
);
INSERT INTO ppe.task_issue_type (task_issue_type_id, description, severity)
VALUES
    (1, 'The task has no schedule associated with it.', 'HIGH')
,   (2, 'The task has repeatedly timed out.', 'HIGH')
,   (3, 'The task has repeatedly failed.', 'MED')
,   (4, 'Task Tool is not unique.', 'MED')
,   (5, 'Task SQL is not unique.', 'MED')
,   (6, 'The task is slow.', 'MED')
,   (7, 'The task has no resources associated with it.', 'LOW')
;

CREATE TABLE ppe.task_issue (
    task_issue_id SERIAL PRIMARY KEY
,   task_id INT NOT NULL REFERENCES ppe.task (task_id)
,   task_issue_type_id INT NOT NULL REFERENCES ppe.task_issue_type (task_issue_type_id)
,   supporting_info JSONB NOT NULL DEFAULT jsonb_build_object()
,   ts TIMESTAMPTZ(0) NOT NULL DEFAULT now()
,   UNIQUE (task_id, task_issue_type_id)
);

CREATE OR REPLACE PROCEDURE ppe.update_task_issues()
LANGUAGE plpgsql
AS $$
DECLARE
BEGIN
    TRUNCATE ppe.task_issue;

-- (1) task has no schedule associated with it
    INSERT INTO ppe.task_issue (task_id, task_issue_type_id)
    SELECT t.task_id, 1 AS task_issue_type_id
    FROM ppe.task AS t
    WHERE NOT EXISTS (
        SELECT 1
        FROM ppe.task_schedule AS ts
        WHERE t.task_id = ts.task_id
    );

-- (2) task has repeatedly timed out

-- (3) task has repeatedly errored out

-- (4) task tool not unique
    INSERT INTO ppe.task_issue (
        task_id
    ,   task_issue_type_id
    ,   supporting_info
    )
    SELECT
        min(t.task_id) AS task_id
    ,   4 AS task_issue_type_id
    ,   jsonb_build_object(
            'task_name', t.task_name
        ,   'task_ids', string_agg(t.task_id::TEXT, ', ')
        ,   'tool_args', string_agg(concat('[', array_to_string(t.tool_args, ', '), ']'), ', ')
        ) AS supporting_info
    FROM ppe.task AS t
    GROUP BY
        t.task_name
    HAVING
        COUNT(*) > 1
    ;

-- (5) task sql not unique
    INSERT INTO ppe.task_issue (
        task_id
    ,   task_issue_type_id
    ,   supporting_info
    )
    SELECT
        min(t.task_id) AS task_id
    ,   5 AS task_issue_type_id
    ,   jsonb_build_object(
            'task_sql', t.task_sql
        ,   'task_ids', string_agg(t.task_id::TEXT, ', ')
        ) AS supporting_info
    FROM ppe.task AS t
    GROUP BY
        t.task_sql
    HAVING
        COUNT(*) > 1
    ;

-- (6) task is slow

-- (7) task has no resources associated with it
    INSERT INTO ppe.task_issue (task_id, task_issue_type_id)
    SELECT t.task_id, 7 AS task_issue_type_id
    FROM ppe.task AS t
    WHERE NOT EXISTS (
        SELECT 1
        FROM ppe.task_resource AS tr
        WHERE t.task_id = tr.task_id
    );
END;
$$;

CREATE OR REPLACE FUNCTION ppe.get_task_issues()
RETURNS TABLE (
    task_id INT
,   task_name TEXT
,   issue TEXT
,   severity TEXT
)
LANGUAGE sql
AS $$
    SELECT
        ti.task_id
    ,   t.task_name
    ,   tit.description AS issue
    ,   tit.severity::TEXT AS severity
    FROM ppe.task_issue AS ti
    JOIN ppe.task AS t
        ON ti.task_id = t.task_id
    JOIN ppe.task_issue_type AS tit
        ON ti.task_issue_type_id = tit.task_issue_type_id
    ORDER BY
        tit.severity::TEXT
    ,   t.task_name
$$;
