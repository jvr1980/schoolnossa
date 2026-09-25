-- Same hierarchy for the German rows, so the navigation works across both
-- countries. Only fills NULLs. NOTE: the Lovable import whitelist does not
-- carry geo_*, so re-importing a German city through Admin -> Data Import
-- would blank these again until the whitelist is updated.
UPDATE public.schools SET
  geo_country = 'DE',
  geo_region = CASE city WHEN 'berlin' THEN 'Berlin' WHEN 'hamburg' THEN 'Hamburg'
    WHEN 'bremen' THEN 'Bremen' WHEN 'muenchen' THEN 'Bayern'
    WHEN 'koeln' THEN 'Nordrhein-Westfalen' WHEN 'duesseldorf' THEN 'Nordrhein-Westfalen'
    WHEN 'frankfurt' THEN 'Hessen' WHEN 'stuttgart' THEN 'Baden-Württemberg'
    WHEN 'dresden' THEN 'Sachsen' WHEN 'leipzig' THEN 'Sachsen' END,
  geo_municipality = CASE city WHEN 'berlin' THEN 'Berlin' WHEN 'hamburg' THEN 'Hamburg'
    WHEN 'bremen' THEN 'Bremen' WHEN 'muenchen' THEN 'München' WHEN 'koeln' THEN 'Köln'
    WHEN 'duesseldorf' THEN 'Düsseldorf' WHEN 'frankfurt' THEN 'Frankfurt am Main'
    WHEN 'stuttgart' THEN 'Stuttgart' WHEN 'dresden' THEN 'Dresden' WHEN 'leipzig' THEN 'Leipzig' END
WHERE geo_country IS NULL AND city IN ('berlin','hamburg','bremen','muenchen','koeln',
  'duesseldorf','frankfurt','stuttgart','dresden','leipzig');
UPDATE public.primary_schools SET
  geo_country = 'DE',
  geo_region = CASE city WHEN 'berlin' THEN 'Berlin' WHEN 'hamburg' THEN 'Hamburg'
    WHEN 'bremen' THEN 'Bremen' WHEN 'muenchen' THEN 'Bayern'
    WHEN 'koeln' THEN 'Nordrhein-Westfalen' WHEN 'duesseldorf' THEN 'Nordrhein-Westfalen'
    WHEN 'frankfurt' THEN 'Hessen' WHEN 'stuttgart' THEN 'Baden-Württemberg'
    WHEN 'dresden' THEN 'Sachsen' WHEN 'leipzig' THEN 'Sachsen' END,
  geo_municipality = CASE city WHEN 'berlin' THEN 'Berlin' WHEN 'hamburg' THEN 'Hamburg'
    WHEN 'bremen' THEN 'Bremen' WHEN 'muenchen' THEN 'München' WHEN 'koeln' THEN 'Köln'
    WHEN 'duesseldorf' THEN 'Düsseldorf' WHEN 'frankfurt' THEN 'Frankfurt am Main'
    WHEN 'stuttgart' THEN 'Stuttgart' WHEN 'dresden' THEN 'Dresden' WHEN 'leipzig' THEN 'Leipzig' END
WHERE geo_country IS NULL AND city IN ('berlin','hamburg','bremen','muenchen','koeln',
  'duesseldorf','frankfurt','stuttgart','dresden','leipzig');
