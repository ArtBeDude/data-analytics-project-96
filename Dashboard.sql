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
        )
    AS total_spent
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
        )
    AS total_spent
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
