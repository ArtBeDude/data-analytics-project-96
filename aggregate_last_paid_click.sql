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
            OVER (PARTITION BY ses.visitor_id ORDER BY ses.visit_date DESC)
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
        utm_source,
        utm_medium,
        utm_campaign,
        TO_CHAR(campaign_date, 'YYYY-MM-DD') AS date_ad,
        SUM(daily_spent) AS total_cost
    FROM ya_ads
    GROUP BY
        utm_source,
        utm_medium,
        utm_campaign,
        TO_CHAR(campaign_date, 'YYYY-MM-DD')
    UNION ALL
    SELECT
        utm_source,
        utm_medium,
        utm_campaign,
        TO_CHAR(campaign_date, 'YYYY-MM-DD') AS date_ad,
        SUM(daily_spent) AS total_cost
    FROM vk_ads
    GROUP BY
        utm_source,
        utm_medium,
        utm_campaign,
        TO_CHAR(campaign_date, 'YYYY-MM-DD')
    ORDER BY date_ad
)

SELECT
    cte2.utm_source,
    cte2.utm_medium,
    cte2.utm_campaign,
    yv.total_cost,
    TO_CHAR(cte2.visit_date, 'YYYY-MM-DD') AS visit_date,
    COUNT(*) AS visitors_count,
    COUNT(*) FILTER (WHERE cte2.lead_id IS NOT NULL) AS leads_count,
    COUNT(*) FILTER (WHERE cte2.status_id = 142) AS purchases_count,
    SUM(cte2.amount) AS revenue
FROM cte2
LEFT JOIN ya_vk_spent AS yv
    ON
        TO_CHAR(cte2.visit_date, 'YYYY-MM-DD') = yv.date_ad
        AND cte2.utm_source = yv.utm_source
        AND cte2.utm_medium = yv.utm_medium
        AND cte2.utm_campaign = yv.utm_campaign
GROUP BY
    cte2.utm_source,
    cte2.utm_medium,
    cte2.utm_campaign,
    yv.total_cost,
    TO_CHAR(cte2.visit_date, 'YYYY-MM-DD')
ORDER BY
    revenue DESC NULLS LAST,
    visit_date ASC,
    visitors_count DESC,
    cte2.utm_source ASC,
    cte2.utm_medium ASC,
    cte2.utm_campaign ASC
LIMIT 15;
