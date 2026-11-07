# GTM plan explainer: deck and video (November 2026)

The plan itself is in `docs/GTM_PLAN_2026-11.md`.

- **Deck:** [SchoolNossa GTM Plan](https://claude.ai/artifact/HoNHHzH88Mbu6fLX63Lzbi) (Claude Slides artifact, 16 slides, private until shared). Its source is in `deck/project/`: one HTML `<section>` per slide. Each slide's speaker notes (`<aside>`) are the narration of the video.
- **Video:** `schoolnossa_gtm_explainer.mp4` (1920×1080, narrated, soft English subtitles), plus `schoolnossa_gtm_explainer.srt`.

## Rebuild the video

Requires ffmpeg, Google Chrome, `GEMINI_API_KEY` (from `.env`) and `playwright-core` (`npm i playwright-core@1.48.2` in `video_source/pw/`, or set `PW_CORE` to an installed copy).

```bash
cd marketing/gtm_2026-11/video_source
python3 -I tts.py ../deck work/audio            # Gemini TTS (gemini-3.8-flash-tts, voice Kore), one WAV per slide
python3 -I check_audio.py ../deck work/audio    # transcribes each WAV and compares it with the notes
node render_slides.js ../deck work/png          # one 1920×1080 PNG per slide
python3 -I build_video.py ../deck work/png work/audio ../schoolnossa_gtm_explainer.mp4
```

Two lessons from the first build:
- The TTS model reads any style instruction aloud ("Say in a warm voice: …"). Send the plain notes only; it doesn't accept a system instruction either. `check_audio.py` catches this.
- Write "School Nossa" in the TTS text, or the brand is mispronounced. `tts.py` replaces it.
- `check_audio.py` flags number-heavy slides (match < 0.85), because the transcript writes "109" where the narration says "one hundred and nine". Transcribe flagged slides with numbers written in words and read them. In the November build, `start`, `budget` and `success` were flagged this way and were complete.

The final file was loudness-normalised to about −16 LUFS (`loudnorm=I=-16:TP=-1.5:LRA=11`). Length 7:54.
