CASE WHEN DATEPART(WEEKDAY, k.DateTimeEST) IN (1,7)
     THEN 'Weekend' ELSE 'Weekday' END          AS [type],

CASE
    WHEN DATEPART(WEEKDAY, k.DateTimeEST) IN (1,7)
         THEN 'Weekend'                          -- ← check day FIRST
    WHEN DATEPART(HOUR, k.DateTimeEST) BETWEEN 7 AND 18
         THEN 'BusinessHours'
    ELSE 'NonBusinessHours'
END                                             AS businessHour,