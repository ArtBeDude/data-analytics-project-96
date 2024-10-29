WITH cte1 AS (
    SELECT DISTINCT ON (le.visitor_id)
        le.visitor_id,
        ses.visit_date,
        ses.source AS utm_source,
        ses.medium AS utm_medium,
        ses.campaign AS utm_campaign,
        le.lead_id,
        le.created_at,
        le.amount,
        le.closing_reason,
        le.status_id
    FROM leads AS le
    LEFT JOIN sessions AS ses
        ON le.visitor_id = ses.visitor_id
    WHERE
        ses.source <> 'organic'
        AND ses.medium <> 'organic'
        AND le.amount > 0
    ORDER BY le.visitor_id ASC, ses.visit_date DESC
)

SELECT *
FROM cte1
ORDER BY
    amount DESC,
    visit_date ASC,
    utm_source ASC,
    utm_medium ASC,
    utm_campaign ASC
LIMIT 10;