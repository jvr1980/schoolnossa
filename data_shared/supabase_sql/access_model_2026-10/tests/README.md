Local verification of the access-model SQL (no production writes).

    docker run -d --rm --name sn-access-test -e POSTGRES_PASSWORD=test postgres:15-alpine
    run() { docker exec -i sn-access-test psql -q -U postgres -v ON_ERROR_STOP=1 -X; }
    run < tests/00_stub_live_schema.sql      # stand-in for the live schema
    run < 01_access_model_migration.sql
    run < tests/10_tests.sql                 # trial grant, re-signup, deletion paths, access matrix
    run < 03_go_live_backfill.sql
    run < 02_lock_school_tables.sql
    run < tests/20_after_backfill_and_lock.sql   # ends with "ALL ASSERTIONS PASSED"
    docker stop sn-access-test

Passed 2026-10-24 on postgres:15-alpine.
