SELECT 'schools' AS t, count(*) AS n, count(DISTINCT schulnummer) AS ids,
       count(*) FILTER (WHERE schulnummer NOT LIKE 'NL-%') AS foreign_ids,
       count(DISTINCT city) AS cities, count(embedding) AS vectors,
       min(vector_dims(embedding)) AS dmin, max(vector_dims(embedding)) AS dmax
FROM public.nl_stage_schools
UNION ALL
SELECT 'primary_schools', count(*), count(DISTINCT schulnummer),
       count(*) FILTER (WHERE schulnummer NOT LIKE 'NL-%'),
       count(DISTINCT city), count(embedding),
       min(vector_dims(embedding)), max(vector_dims(embedding))
FROM public.nl_stage_primary_schools;
-- expect schools 1629 / primary_schools 6060, foreign_ids 0, dims 768/768
