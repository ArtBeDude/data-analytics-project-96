SELECT
    visitor_id,
    source,
    date_trunc('day', visit_date) AS visit_date
FROM sessions
WHERE medium != 'organic';
--Выбор уникальных пользователей по дням без органики

WITH tab AS (
    SELECT
        visitor_id,
        source,
        date_trunc('day', visit_date) AS visit_date,
        count(source)
        OVER (PARTITION BY date_trunc('day', visit_date), source)
        AS cnt_source
    FROM sessions
    WHERE medium != 'organic'
)

SELECT
    visit_date,
    visitor_id,
    source
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
    date_trunc('day', le.created_at) AS created_at
FROM leads AS le
INNER JOIN cte1 AS cte1
    ON le.visitor_id = cte1.visitor_id;
-- количество новых лидов в день без органики

SELECT
    ya.utm_source,
    ya.campaign_date,
    sum(sum(ya.daily_spent))
    OVER (
        PARTITION BY ya.utm_source
        ORDER BY ya.campaign_date ROWS UNBOUNDED PRECEDING
    ) AS total_spent
FROM ya_ads AS ya
GROUP BY 1, 2
UNION ALL
SELECT
    va.utm_source,
    va.campaign_date,
    sum(sum(va.daily_spent))
    OVER (
        PARTITION BY va.utm_source
        ORDER BY va.campaign_date ROWS UNBOUNDED PRECEDING
    ) AS total_spent
FROM vk_ads AS va
GROUP BY 1, 2; -- подсчет стоимости рекламы по каналам ya и vk в динамике

WITH cte1 AS (
    SELECT DISTINCT visitor_id
    FROM sessions
    WHERE medium != 'organic'
),

cte2 AS (
    SELECT
        date_trunc('day', visit_date) AS visit_date,
        count(DISTINCT visitor_id) AS count_visitors
    FROM sessions
    WHERE medium != 'organic'
    GROUP BY 1

)

SELECT
    cte2.count_visitors,
    date_trunc('day', le.created_at) AS cv_date,
    count(le.visitor_id) AS leads_count,
    count(le.visitor_id) FILTER (WHERE le.status_id = 142) AS purchase_count
FROM leads AS le
INNER JOIN cte1
    ON le.visitor_id = cte1.visitor_id
LEFT JOIN cte2
    ON date_trunc('day', le.created_at) = cte2.visit_date
-- датасет с подсчетом уникальных пользователей, лидов и покупателей
GROUP BY 1, 2;
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
        Extract(DAY FROM le.created_at) - Extract(DAY FROM ses.visit_date)
        AS diff_day,
        Row_number()
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
        lead_id,
        created_at,
        diff_day,
        Cume_dist() OVER (ORDER BY diff_day ASC) AS cume_res
    FROM cte1
    WHERE
        rn = 1
        AND status_id = 142
    ORDER BY
        diff_day DESC
)

SELECT First_value(diff_day) OVER (ORDER BY diff_day ASC) AS lead_close
FROM cte2
WHERE cume_res >= 0.9
LIMIT 1;
