-- One statement per table: all staged rows land together or not at all.
INSERT INTO public.schools SELECT * FROM public.nl_stage_schools
  ON CONFLICT (schulnummer) DO NOTHING;
INSERT INTO public.primary_schools SELECT * FROM public.nl_stage_primary_schools
  ON CONFLICT (schulnummer) DO NOTHING;
