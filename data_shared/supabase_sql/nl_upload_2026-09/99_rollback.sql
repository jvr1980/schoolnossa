-- Removes every NL row. German rows are never matched (their ids have no NL- prefix).
DELETE FROM public.schools WHERE schulnummer LIKE 'NL-%';
DELETE FROM public.primary_schools WHERE schulnummer LIKE 'NL-%';
