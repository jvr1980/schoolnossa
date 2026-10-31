# SchoolNossa beta video (9:16, October 2026)

Two versions of the same 63.5-second, 1080×1920 video for LinkedIn (vertical video):
- `schoolnossa_beta_9x16_narrated.mp4`: with the voice-over from the NotebookLM short
  "What's New in the SchoolNossa Beta". **Post this one.**
- `schoolnossa_beta_9x16_captions.mp4`: the same pictures with no sound.

The video uses real screenshots of the iPhone app, the website and the Claude and ChatGPT
connectors, in the website's colours:
- blue `#3B82F6` → sky `#0EA5E9`
- background `#F8FAFC`
- the Inter font

It doesn't mention pricing. It says SchoolNossa is an experimental side project and that the
first 500 testers use it for free. It deliberately doesn't say "non-commercial": SchoolNossa
OÜ has paid plans, so the video only describes what is true today (see the wording note in
`../linkedin_post_2026-10.md`).

Scenes, with times in seconds. Each scene starts in the short pause before its sentence in the
narration:

| Time | Scene | Narration |
|------|-------|-----------|
| 0–8.9 | The problem: facts scattered across many websites | "Choosing a school is a massive decision for parents, but the essential facts are frustratingly scattered across dozens of different websites." |
| 8.9–14.5 | Everything in one place: 220+ details, the map, commute times | "That is why the new SchoolNossa beta brings all this data into one central place." |
| 14.5–22.7 | New: the iPhone app | "First up: the new iPhone app. Parents can now access interactive maps, detailed filters, and their pinned shortlists on the go." |
| 22.7–32.6 | New: the Claude and ChatGPT connectors (no password is shared) | "Next: SchoolNossa now works inside Claude and ChatGPT. You can search in plain language, and the assistant only receives school data, never your password." |
| 32.6–41.85 | Admission and open days: 1,100+ German schools with an upcoming open day | "Third: tracked application windows and upcoming open days. The platform pulls the exact wording for these key dates directly from where schools publish them." |
| 41.85–47.3 | 10 German cities, 2,800+ schools | "And finally: a massive geographic expansion. The beta covers ten major German cities," |
| 47.3–51.5 | The Netherlands (alpha), 7,600+ schools, VMBO/HAVO/VWO | "plus a brand-new alpha release mapping the entire Netherlands." |
| 51.5–58.1 | An experimental side project, free for the first 500 testers, feedback welcome | "Because SchoolNossa is a purely experimental side project, your direct feedback shapes its future." |
| 58.1–63.5 | schoolnossa.com | "Try it at schoolnossa.com." |

Inside the scenes, the details follow the words too:
- the ChatGPT tab switches on as the narrator says "ChatGPT"
- the 🔒 caption appears with "never your password"
- the "Open days" chip appears with "open days"
- the feedback card appears with "your direct feedback"

## Rebuild

Everything is in `source/`. `scene.html` draws one frame for any time `t` through
`window.render(t)`, and `render.js` captures the frames with the system Chrome
(it uses Playwright from the npx cache).

```bash
cd marketing/beta_announcement/video/source
node render.js stills 4.5,18,26,39.5,62      # preview stills in stills/
node render.js frames 63.5 30                # frames in frames/
ffmpeg -framerate 30 -i frames/f_%05d.jpg -c:v libx264 -preset slow -crf 20 \
  -pix_fmt yuv420p -movflags +faststart ../schoolnossa_beta_9x16_captions.mp4
```

The screenshots are in `source/shots/`. They are the same images the website shows:
- iPhone screenshots from the app's landing page, from the Lovable asset store
- web and connector screenshots from `public/screenshots` in the app repo

The pricing screen (IMG_7803) and the personal-details screen (IMG_7805) are left out on purpose.

## Narration

The voice-over is the audio track of the NotebookLM short (64.27 s, AAC mono), downloaded
from Gemini Notebook (Studio → "What's New in the SchoolNossa Beta" → download). The
download itself isn't in git, and it must not be posted as it is: the original voice says
"…a purely non-commercial, experimental side project…".

The words "non-commercial," (53.935–54.890 s in the original) are cut out, so the voice says
"…a purely experimental side project…". The cut was placed using the hissing sounds around
it: the "sh" of "commercial" and the "ks" of "experimental". There is a 40 ms fade at the
joint so it doesn't click. Everything after the cut is 0.955 s earlier, and the narration
is then 63.32 s long. Then it is padded with silence to the video's length:

```bash
ffmpeg -i <notebooklm_short>.mp4 -filter_complex \
  "[0:a]atrim=0:53.935,asetpts=PTS-STARTPTS,afade=t=out:st=53.895:d=0.04[a1]; \
   [0:a]atrim=start=54.890,asetpts=PTS-STARTPTS,afade=t=in:st=0:d=0.005[a2]; \
   [a1][a2]concat=n=2:v=0:a=1[a]" -map "[a]" -c:a pcm_s16le narration_cut.wav
ffmpeg -i schoolnossa_beta_9x16_captions.mp4 -i narration_cut.wav \
  -map 0:v -map 1:a -c:v copy -af apad -c:a aac -b:a 160k -shortest \
  -movflags +faststart schoolnossa_beta_9x16_narrated.mp4
```

The scene times come from the NotebookLM video's burned-in captions (4 frames per second)
and from the pauses in its audio (`silencedetect=noise=-35dB:d=0.25`). The captions switch
to the next phrase as soon as the previous one ends, so across a pause they appear up to
about a second before the words. Each scene therefore starts inside the pause, about
0.3 s before the first word, not at the caption change.
