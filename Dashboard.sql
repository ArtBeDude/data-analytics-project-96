WITH lpc AS (
    SELECT
        le.visitor_id,
        ses.source AS utm_source,
        le.lead_id,
        (le.created_at - ses.visit_date) AS date_diff,
        row_number()
            OVER (PARTITION BY ses.visitor_id ORDER BY ses.visit_date DESC)
        AS rn
    -- нумерация всех записей дней визитов
    FROM sessions AS ses
    LEFT JOIN leads AS le
        ON
            ses.visitor_id = le.visitor_id
            AND ses.visit_date <= le.created_at
    WHERE
        ses.medium != 'organic'
        AND le.status_id = 142
)

SELECT
    percentile_disc(0.9)
    WITHIN GROUP (ORDER BY date_diff ASC) AS day_to_close
-- выбираем первое значение интервала до закрытия выше процентиля (0.9)
FROM lpc
WHERE
    rn = 1 -- выбор первой записи посетителя по модели lpc

