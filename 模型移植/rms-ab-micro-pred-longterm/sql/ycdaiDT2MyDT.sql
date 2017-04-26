SELECT
    description,
    `date`,
    tag,
    YEAR(`date`) AS `year`,
    CASE description
        WHEN '元旦节' THEN 100
        WHEN '劳动节' THEN 101
        WHEN '国庆节' THEN 102
        WHEN '清明' THEN 103
        WHEN '端午节' THEN 104
        WHEN '中秋节' THEN 105
        WHEN '春节' THEN 106
        WHEN '七夕节' THEN 107
        WHEN '平安夜' THEN 110
        WHEN '情人节' THEN 109
        WHEN '圣诞节' THEN 108
    END AS holiday_key
FROM
    rms.ref_holidays order by `year`,holiday_key,`date`;


    SELECT
        description,
        `date`,
        100 + (holiday_key % 100) * 10 + `offset` AS tag,
        year(`date`) `year`,
       holiday_key,
        `offset`
    FROM
        rms.ref_holidays_tmp order by description, date;