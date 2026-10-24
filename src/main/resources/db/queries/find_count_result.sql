SELECT request.status,
       criteria.criteria_json,
       CASE WHEN request.status = 'PROCESSED' THEN
           (SELECT COUNT(*)
            FROM processed_search matching
            WHERE matching.group_id = processed.group_id)
       END AS search_count,
       CASE WHEN request.status = 'PROCESSED' THEN
           DBMS_LOB.COMPARE(request.criteria_json, criteria.criteria_json)
       END AS criteria_comparison
FROM search_request request
LEFT JOIN processed_search processed ON processed.search_id = request.search_id
LEFT JOIN criteria_group criteria ON criteria.group_id = processed.group_id
WHERE request.search_id = HEXTORAW(:searchId)
