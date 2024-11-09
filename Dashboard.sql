SELECT 
    date_trunc('day',visit_date),
    visitor_id,
    source
FROM sessions
WHERE medium != 'organic'; -- Выбор уникальных пользователей по дням без органики

WITH tab AS (
    SELECT 
        date_trunc('day',visit_date) AS visit_date,
        visitor_id,
        SOURCE,
        COUNT("source") OVER (PARTITION BY date_trunc('day',visit_date), SOURCE) AS cnt_source
    FROM sessions
    WHERE medium != 'organic'
)
SELECT 
    visit_date,
    visitor_id,
    SOURCE
FROM tab
WHERE cnt_source >= 5; -- датасет для подсчета пользователей по дням/каналам
-- где кол-во переходов в день по каналу >= 5 без органики

WITH cte1 AS (
    SELECT DISTINCT visitor_id
    FROM sessions
    WHERE medium != 'organic'
)
SELECT
     le.visitor_id,
    date_trunc('day',le.created_at)
FROM leads AS le
INNER JOIN cte1
    using(visitor_id) -- количество новых лидов в день без органики

SELECT
    ya.utm_source,
    ya.campaign_date,
    SUM(SUM(ya.daily_spent)) OVER (PARTITION BY ya.utm_source  ORDER BY ya.campaign_date   ROWS UNBOUNDED PRECEDING) as total_spent
FROM ya_ads ya
GROUP BY 1,2
UNION ALL
SELECT
    va.utm_source,
    va.campaign_date,
    SUM(SUM(va.daily_spent)) OVER (PARTITION BY va.utm_source  ORDER BY va.campaign_date   ROWS UNBOUNDED PRECEDING) AS total_spent
FROM vk_ads va
GROUP BY 1,2; -- подсчет стоимости рекламы по каналам ya и vk в динамике

WITH cte1 AS (
    SELECT DISTINCT visitor_id
    FROM sessions
    WHERE medium != 'organic'
),
cte2 AS (
    SELECT
        date_trunc('day',visit_date) AS visit_date,
        COUNT(DISTINCT visitor_id) AS count_visitors
    FROM sessions
    WHERE medium != 'organic'
    GROUP BY 1
    
)
SELECT
    date_trunc('day',le.created_at) AS cv_date,
    count_visitors,
    COUNT(le.visitor_id) AS leads_count,
    COUNT(le.visitor_id) FILTER (WHERE status_id = 142) as purchase_count
FROM leads AS le
INNER JOIN cte1
    using(visitor_id)
LEFT JOIN cte2
    ON date_trunc('day',le.created_at) = cte2.visit_date
GROUP BY 1,2; -- датасет с подсчетом уникальных пользователей, лидов и покупателей
-- для расчета процента конверсии

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
        ROW_NUMBER() OVER (PARTITION BY ses.visitor_id ORDER BY visit_date DESC) AS rn
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
    date_trunc('day', campaign_date) AS date,
    utm_source,
    utm_medium,
    utm_campaign,
    SUM(daily_spent) AS total_cost
FROM ya_ads
GROUP BY 1, 2, 3, 4
UNION ALL
SELECT
    date_trunc('day', campaign_date) AS date,
    utm_source,
    utm_medium,
    utm_campaign,
    SUM(daily_spent) AS total_cost
FROM vk_ads
GROUP BY 1, 2, 3, 4
ORDER BY 1
)
SELECT
    date_trunc('day', cte2.visit_date) AS visit_date,
    cte2.utm_source,
    cte2.utm_medium,
    cte2.utm_campaign,
    yv.total_cost,
    count(*) AS visitor_count,
    count(*) FILTER (WHERE lead_id IS NOT NULL) as leads_count,
    count(*) FILTER (WHERE status_id = 142) AS purchase_count,
    sum(amount) AS revenue
FROM cte2
LEFT JOIN ya_vk_spent AS yv
    ON  date_trunc('day', cte2.visit_date) = yv.date
    AND cte2.utm_source = yv.utm_source
    AND cte2.utm_medium = yv.utm_medium
    AND cte2.utm_campaign = yv.utm_campaign
GROUP BY 1, 2, 3, 4, 5
ORDER BY
    revenue DESC NULLS LAST,
    visit_date ASC,
    visitor_count  DESC,
    utm_source ASC,
    utm_medium ASC,
    utm_campaign ASC
LIMIT 15; -- модель last paid click для расчета метрик
-- cpc, cpa, cppu, roi по топ 15 прибыльным каналам.
