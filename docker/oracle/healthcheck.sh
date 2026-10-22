#!/usr/bin/env bash
set -o pipefail
sqlplus -L -s 'search_it/LocalOnly_IT_2026@//127.0.0.1:1521/FREEPDB1' <<'SQL' | grep -Eq '^[[:space:]]*1[[:space:]]*$'
WHENEVER SQLERROR EXIT FAILURE
SELECT COUNT(*) FROM user_users
WHERE default_tablespace = 'SEARCH_DATA'
AND EXISTS (SELECT 1 FROM user_ts_quotas WHERE tablespace_name = 'SEARCH_DATA' AND max_bytes > 0);
EXIT;
SQL
