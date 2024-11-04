WITH cte1 AS (
    SELECT
        le.visitor_id,
        ses.visit_date,
        ses.source AS utm_source,
        ses.medium AS utm_medium,
        ses.campaign AS utm_campaign,
        le.lead_id,
        le.created_at,
        le.amount,
        le.closing_reason,
        le.status_id,
        ROW_NUMBER()
            OVER (PARTITION BY ses.visitor_id ORDER BY visit_date DESC)
        AS rn
    FROM sessions AS ses
    LEFT JOIN leads AS le
        ON
            ses.visitor_id = le.visitor_id
            AND ses.visit_date <= le.created_at
    WHERE ses.medium <> 'organic'
),

cte2 AS (
    SELECT
        visitor_id,
        visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        lead_id,
        created_at,
        amount,
        closing_reason,
        status_id
    FROM cte1
    WHERE rn = 1
    ORDER BY
        amount DESC NULLS LAST,
        visit_date ASC,
        utm_source ASC,
        utm_medium ASC,
        utm_campaign ASC
),

ya_vk_spent AS (
    SELECT
        TO_CHAR(campaign_date, 'YYYY-MM-DD') AS date,
        utm_source,
        utm_medium,
        utm_campaign,
        SUM(daily_spent) AS total_cost
    FROM ya_ads
    GROUP BY 1, 2, 3, 4
    UNION ALL
    SELECT
        TO_CHAR(campaign_date, 'YYYY-MM-DD') AS date,
        utm_source,
        utm_medium,
        utm_campaign,
        SUM(daily_spent) AS total_cost
    FROM vk_ads
    GROUP BY 1, 2, 3, 4
    ORDER BY 1
)

SELECT
    TO_CHAR(cte2.visit_date, 'YYYY-MM-DD') AS visit_date,
    cte2.utm_source,
    cte2.utm_medium,
    cte2.utm_campaign,
    yv.total_cost,
    COUNT(*) AS visitors_count,
    COUNT(*) FILTER (WHERE lead_id IS NOT NULL) AS leads_count,
    COUNT(*) FILTER (WHERE status_id = 142) AS purchases_count,
    SUM(amount) AS revenue
FROM cte2
LEFT JOIN ya_vk_spent AS yv -- 
    ON
        TO_CHAR(cte2.visit_date, 'YYYY-MM-DD') = yv.date
        AND cte2.utm_source = yv.utm_source
        AND cte2.utm_medium = yv.utm_medium
        AND cte2.utm_campaign = yv.utm_campaign
GROUP BY 1, 2, 3, 4, 5
ORDER BY
    revenue DESC NULLS LAST,
    visit_date ASC,
    visitors_count DESC,
    utm_source ASC,
    utm_medium ASC,
    utm_campaign ASC
LIMIT 15;


