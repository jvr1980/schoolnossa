# SchoolNossa beta video (9:16, October 2026)

`schoolnossa_beta_9x16_captions.mp4`: 64 seconds, 1080×1920, with no sound. It's made
for LinkedIn (vertical video). It uses real screenshots of the iPhone app, the website
and the Claude and ChatGPT connectors, in the website's colours:
- blue `#3B82F6` → sky `#0EA5E9`
- background `#F8FAFC`
- the Inter font

It doesn't mention pricing. It says the project is experimental and not commercial.

Scenes, with times in seconds that follow the beats of the NotebookLM short "What's New in the
SchoolNossa Beta", so its narration can be laid over this video later:

| Time | Scene |
|------|-------|
| 0–6.5 | The problem: facts scattered across many websites |
| 6.5–13 | Everything in one place: 220+ details, the map, commute times |
| 13–22.5 | New: the iPhone app |
| 22.5–35 | New: the Claude and ChatGPT connectors (no password is shared) |
| 35–42 | Admission and open days: 1,100+ German schools with an upcoming open day |
| 42–48 | 10 German cities, 2,800+ schools |
| 48–51.5 | The Netherlands (alpha), 7,600+ schools, VMBO/HAVO/VWO |
| 51.5–58.5 | Experimental, not commercial, feedback welcome |
| 58.5–64.4 | schoolnossa.com |

## Rebuild

Everything is in `source/`. `scene.html` draws one frame for any time `t` through
`window.render(t)`, and `render.js` captures the frames with the system Chrome
(it uses Playwright from the npx cache).

```bash
cd marketing/beta_announcement/video/source
node render.js stills 4.5,18,26,39.5,62      # preview stills in stills/
node render.js frames 64.4 30                # frames in frames/
ffmpeg -framerate 30 -i frames/f_%05d.jpg -c:v libx264 -preset slow -crf 20 \
  -pix_fmt yuv420p -movflags +faststart ../schoolnossa_beta_9x16_captions.mp4
```

The screenshots are in `source/shots/`. They are the same images the website shows:
- iPhone screenshots from the app's landing page, from the Lovable asset store
- web and connector screenshots from `public/screenshots` in the app repo

The pricing screen (IMG_7803) and the personal-details screen (IMG_7805) are left out on purpose.

To add the NotebookLM narration:
1. Download the short in Gemini Notebook (Studio → "What's New in the SchoolNossa Beta" →
   download).
2. Run `ffmpeg -i video.mp4 -i narration.mp4 -map 0:v -map 1:a -c:v copy -c:a aac -shortest out.mp4`.
3. If a scene and the voice drift apart, shift the scene times (`data-s`/`data-e`) in
   `scene.html` and render again.
