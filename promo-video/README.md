# SchoolNossa Promotional Video

This is a [Remotion](https://www.remotion.dev/) project for creating programmatic promotional videos for SchoolNossa.

## Quick Start

```bash
# Install dependencies
npm install

# Start the Remotion Studio (live preview)
npm start

# Render the final video
npm run build
```

## Adding Your Screenshots

1. Take screenshots of your app features
2. Save them to `public/screenshots/` with these names:
   - `dashboard.png` - Main dashboard view
   - `filters.png` - Search and filter interface
   - `metrics.png` - Performance metrics/trends view
   - `details.png` - School detail page

3. Update `src/components/ScreenshotSlide.tsx` to use actual images:
   ```tsx
   // Uncomment this line:
   <Img src={staticFile(screenshot)} style={{ width: '100%', height: '100%', objectFit: 'cover' }} />
   ```

## Customizing Content

Edit `src/data/slides.ts` to change:
- Slide titles and descriptions
- Duration of each slide
- Brand colors
- Add or remove slides

## Project Structure

```
promo-video/
├── public/
│   └── screenshots/          # Your app screenshots go here
├── src/
│   ├── components/
│   │   ├── Intro.tsx        # Opening title sequence
│   │   ├── ScreenshotSlide.tsx  # Feature showcase slides
│   │   └── CallToAction.tsx # Closing CTA
│   ├── data/
│   │   └── slides.ts        # Content configuration
│   ├── PromoVideo.tsx       # Main video composition
│   ├── Root.tsx             # Remotion root config
│   └── index.ts             # Entry point
├── package.json
├── tsconfig.json
└── remotion.config.ts
```

## Commands

| Command | Description |
|---------|-------------|
| `npm start` | Open Remotion Studio with live preview |
| `npm run build` | Render video to `out/schoolnossa-promo.mp4` |
| `npm run build:gif` | Render as GIF (for previews) |

## Rendering Options

```bash
# Custom resolution
npx remotion render src/index.ts PromoVideo out/video.mp4 --width=1080 --height=1920

# Different codec
npx remotion render src/index.ts PromoVideo out/video.webm --codec=vp8

# Higher quality
npx remotion render src/index.ts PromoVideo out/video.mp4 --crf=18
```

## Using Claude Code with Remotion Skills

For AI-assisted video creation, install Remotion Skills:

```bash
npx skills add remotion-dev/skills
```

Then you can ask Claude to modify the video using natural language prompts.

## Resources

- [Remotion Documentation](https://www.remotion.dev/docs)
- [Animation Guide](https://www.remotion.dev/docs/animating-properties)
- [Transitions](https://www.remotion.dev/docs/transitioning)
