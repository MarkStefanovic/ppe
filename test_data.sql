TRUNCATE TABLE ppe.batch CASCADE;
TRUNCATE TABLE ppe.batch_error CASCADE;
TRUNCATE TABLE ppe.batch_info CASCADE;
TRUNCATE TABLE ppe.job CASCADE;
TRUNCATE TABLE ppe.job_cancel CASCADE;
TRUNCATE TABLE ppe.job_failure CASCADE;
TRUNCATE TABLE ppe.job_info CASCADE;
TRUNCATE TABLE ppe.job_skip CASCADE;
TRUNCATE TABLE ppe.job_success CASCADE;
TRUNCATE TABLE ppe.resource CASCADE;
TRUNCATE TABLE ppe.task_resource CASCADE;
TRUNCATE TABLE ppe.schedule CASCADE;
TRUNCATE TABLE ppe.task CASCADE;
TRUNCATE TABLE ppe.task_schedule CASCADE;
TRUNCATE TABLE ppe.task_running CASCADE;
TRUNCATE TABLE ppe.task_queue CASCADE;
TRUNCATE TABLE ppe.job_complete CASCADE;
TRUNCATE TABLE ppe.latest_task_attempt CASCADE;

DO $$
DECLARE
    v_batch_id INT;
    v_job_1_id INT;
    v_job_2_id INT;
    v_job_3_id INT;
    v_schedule_1_id INT;
    v_schedule_2_id INT;
    v_hourly_during_work_hours_id INT;
    v_resource_1_id INT;
    v_task_1_id INT;
    v_task_2_id INT;
    v_task_3_id INT;
BEGIN
    v_task_1_id = (
        SELECT *
        FROM ppe.create_task(
            p_task_name := 'Task 1: Never Fails'
        ,   p_cmd := '{echo, Hello Task 1}'
--         ,   p_cmd := ARRAY['echo', '"Hello Task 1"']::TEXT[]
        ,   p_retries := 1
        ,   p_timeout_seconds := 20
        )
    );

    v_task_2_id = (
        SELECT *
        FROM ppe.create_task(
            p_task_name := 'Task 2: Sometimes Fails'
        ,   p_task_sql := 'SELECT pg_sleep(random() * 10 + 1);'
        ,   p_timeout_seconds := 30
        )
    );

    v_task_3_id = (
        SELECT *
        FROM ppe.create_task(
            p_task_name := 'Task 3: Always fails'
        ,   p_task_sql := 'SELECT 1/0'
        ,   p_timeout_seconds := 15
        )
    );

    v_resource_1_id = (SELECT * FROM ppe.create_resource(p_name := 'dw', p_capacity := 3));
    CALL ppe.assign_resource_to_task(p_task_id := v_task_1_id, p_resource_id := v_resource_1_id, p_units := 1);
    CALL ppe.assign_resource_to_task(p_task_id := v_task_2_id, p_resource_id := v_resource_1_id, p_units := 1);

    v_schedule_1_id = (SELECT * FROM ppe.create_schedule(p_schedule_name := 'Every 10s' , p_min_seconds_between_attempts := 10));
    v_schedule_2_id = (SELECT * FROM ppe.create_schedule(p_schedule_name := 'Every 7s' , p_min_seconds_between_attempts := 7));
    v_hourly_during_work_hours_id = (
        SELECT * FROM ppe.create_schedule(
            p_schedule_name := 'Hourly During Work Hours'
        ,   p_min_seconds_between_attempts := 3600
        ,   p_start_week_day := 1::SMALLINT -- start on Monday
        ,   p_end_week_day := 5::SMALLINT -- end on Friday
        ,   p_start_hour := 13::SMALLINT -- start at 6 AM LA time
        ,   p_end_hour := 23::SMALLINT -- end at 5 PM LA time
        )
     );

    CALL ppe.schedule_task(p_task_id := v_task_1_id, p_schedule_id := v_schedule_1_id);
    CALL ppe.schedule_task(p_task_id := v_task_2_id, p_schedule_id := v_schedule_2_id);
    CALL ppe.schedule_task(p_task_id := v_task_3_id, p_schedule_id := v_schedule_2_id);

    v_batch_id = (SELECT * FROM ppe.create_batch());

    v_job_1_id = (SELECT * FROM ppe.create_job(p_batch_id := v_batch_id , p_task_id := v_task_1_id));
    v_job_2_id = (SELECT * FROM ppe.create_job(p_batch_id := v_batch_id , p_task_id := v_task_2_id));
    v_job_3_id = (SELECT * FROM ppe.create_job(p_batch_id := v_batch_id , p_task_id := v_task_3_id));

    CALL ppe.cancel_running_jobs(p_reason := 'User cancelled');

    CALL ppe.log_batch_info(p_batch_id := v_batch_id, p_message := 'Test Batch Informative Message');
    CALL ppe.log_batch_error(p_batch_id := v_batch_id, p_message := 'Test Batch Error Message');

    RAISE NOTICE 'v_task_1_id: %', v_task_1_id;
    RAISE NOTICE 'v_task_2_id: %', v_task_2_id;
    RAISE NOTICE 'v_task_3_id: %', v_task_3_id;
    RAISE NOTICE 'v_resource_1_id: %', v_resource_1_id;
    RAISE NOTICE 'v_schedule_1_id: %', v_schedule_1_id;
    RAISE NOTICE 'v_schedule_2_id: %', v_schedule_2_id;
    RAISE NOTICE 'v_batch_id: %', v_batch_id;
    RAISE NOTICE 'v_job_1_id: %', v_job_1_id;
    RAISE NOTICE 'v_job_2_id: %', v_job_2_id;
    RAISE NOTICE 'v_job_3_id: %', v_job_2_id;
END;
$$;

CALL ppe.update_queue();

SELECT * FROM ppe.task_queue;
