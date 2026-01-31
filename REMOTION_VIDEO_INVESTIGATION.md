# Remotion Video Investigation for SchoolNossa Promotional Video

## Executive Summary

Remotion is a powerful framework for creating videos programmatically using React. Combined with the new "Remotion Skills" for Claude Code (released January 2026), you can create professional promotional videos by simply describing what you want in natural language. This is ideal for your use case: creating a promotional video from screenshots and text explanations.

## What is Remotion?

Remotion is an open-source framework that lets you create videos using React components instead of traditional video editing software.

### Key Concepts

- **Each frame is a React component**: Your video is just a React app that renders different content based on the current frame number
- **Compositions**: Define video dimensions, frame rate, and duration
- **Interpolation**: Animate properties smoothly between values
- **Spring animations**: Natural-feeling animations out of the box
- **TransitionSeries**: Perfect for slideshow-style videos with smooth transitions

### Why Remotion for Your Use Case?

1. **Screenshot Slideshows**: Built-in support for image sequences with professional transitions
2. **Text Animations**: Easy text fade-ins, typewriter effects, and more
3. **Consistency**: Every video renders identically (no rendering artifacts)
4. **Programmability**: Can dynamically generate videos from data (screenshots + descriptions)
5. **AI Integration**: Remotion Skills for Claude Code means you can describe what you want in plain English

---

## Two Approaches to Create Your Video

### Approach 1: Traditional (Manual Coding)

Write React components yourself using Remotion's APIs.

```bash
# Setup
npx create-video@latest schoolnossa-promo
cd schoolnossa-promo
npm install
```

Basic structure:
```tsx
// src/Composition.tsx
import { AbsoluteFill, Img, useCurrentFrame, interpolate } from 'remotion';

export const ScreenshotSlide: React.FC<{
  screenshot: string;
  title: string;
  description: string;
}> = ({ screenshot, title, description }) => {
  const frame = useCurrentFrame();

  // Fade in over 30 frames
  const opacity = interpolate(frame, [0, 30], [0, 1], {
    extrapolateRight: 'clamp',
  });

  return (
    <AbsoluteFill style={{ backgroundColor: '#1a1a2e' }}>
      <div style={{ opacity }}>
        <Img src={screenshot} style={{ width: '60%', margin: '20px auto' }} />
        <h1>{title}</h1>
        <p>{description}</p>
      </div>
    </AbsoluteFill>
  );
};
```

### Approach 2: AI-Powered with Remotion Skills (Recommended)

Use Claude Code with Remotion Skills to generate the video code through natural language.

```bash
# Setup
npx create-video@latest schoolnossa-promo
cd schoolnossa-promo

# Install Remotion Skills for Claude Code
npx skills add remotion-dev/skills

# Initialize Claude Code in the project
# Then use /init command in Claude Code
```

Then simply tell Claude what you want:
> "Create a promotional video for SchoolNossa app. Use these screenshots: [list paths]. Add these text explanations for each: [list descriptions]. Use smooth fade transitions between slides, add a title intro and closing call-to-action."

---

## Practical Implementation Plan for SchoolNossa Promo

### Recommended Video Structure

```
┌─────────────────────────────────────────┐
│ 1. INTRO (3-5 seconds)                  │
│    - SchoolNossa logo/name              │
│    - Tagline: "Find the best school     │
│      for your child in Berlin"          │
├─────────────────────────────────────────┤
│ 2. PROBLEM STATEMENT (5 seconds)        │
│    - Text: "Choosing a school is hard"  │
│    - Brief pain point description       │
├─────────────────────────────────────────┤
│ 3. FEATURE SHOWCASE (4-6 slides)        │
│    Each slide (5-7 seconds):            │
│    - Screenshot of app feature          │
│    - Title text                         │
│    - Description text                   │
│    - Smooth transition to next          │
├─────────────────────────────────────────┤
│ 4. CALL TO ACTION (3-5 seconds)         │
│    - "Try SchoolNossa Today"            │
│    - Website/link                       │
└─────────────────────────────────────────┘
```

### Screenshot Suggestions

Based on your app features, you'd want screenshots showing:

1. **Dashboard Overview** - Main view with school listings
2. **Search & Filters** - Filtering by district, school type
3. **School Details** - Individual school view with metrics
4. **Year-over-Year Comparison** - Historical trend visualization
5. **Performance Metrics** - Abitur success rates, grades
6. **Map View** (if available) - Geographic school locations

### Project Structure

```
schoolnossa-promo/
├── public/
│   └── screenshots/           # Your app screenshots
│       ├── dashboard.png
│       ├── filters.png
│       ├── school-details.png
│       └── metrics.png
├── src/
│   ├── Root.tsx              # Entry point
│   ├── Composition.tsx       # Main video composition
│   ├── components/
│   │   ├── Intro.tsx
│   │   ├── ScreenshotSlide.tsx
│   │   ├── TextSlide.tsx
│   │   └── CallToAction.tsx
│   └── data/
│       └── slides.ts         # Screenshot paths + descriptions
└── remotion.config.ts
```

### Data-Driven Approach

Define your content in a simple data structure:

```typescript
// src/data/slides.ts
export const slideContent = [
  {
    screenshot: '/screenshots/dashboard.png',
    title: 'All Berlin Schools at a Glance',
    description: 'Browse comprehensive data on every school in Berlin',
    duration: 150, // frames (5 seconds at 30fps)
  },
  {
    screenshot: '/screenshots/filters.png',
    title: 'Smart Filtering',
    description: 'Filter by district, school type, and performance metrics',
    duration: 150,
  },
  {
    screenshot: '/screenshots/metrics.png',
    title: 'Track Performance Over Time',
    description: 'Year-over-year trends for informed decisions',
    duration: 150,
  },
  // ... more slides
];
```

---

## Step-by-Step Setup Guide

### Prerequisites

- Node.js 18+ (LTS recommended)
- npm or pnpm

### Installation

```bash
# 1. Create Remotion project
npx create-video@latest schoolnossa-promo
cd schoolnossa-promo

# 2. Install additional dependencies (optional)
npm install @remotion/transitions  # For smooth transitions

# 3. Start the development preview
npm start
# Opens http://localhost:3000 with live preview
```

### Rendering the Final Video

```bash
# Render to MP4
npx remotion render src/index.ts MainComposition out/promo.mp4

# Options
npx remotion render src/index.ts MainComposition out/promo.mp4 \
  --codec h264 \
  --quality 80 \
  --fps 30
```

### Using Claude Code with Remotion Skills

```bash
# Install skills
npx skills add remotion-dev/skills

# The skills are now in .claude/skills/remotion/
# Claude Code will automatically use these when working in the project
```

---

## Example Prompt for Claude Code

Once set up, you can use a prompt like this:

```
Create a 45-second promotional video for SchoolNossa with these specs:

**Brand:**
- Primary color: #1a1a2e (dark blue)
- Accent color: #e94560 (coral red)
- Font: Inter or system sans-serif

**Structure:**
1. Intro (3s): "SchoolNossa" title with tagline "Find the perfect school for your child in Berlin"

2. Problem (4s): Text slide - "Choosing the right school shouldn't be overwhelming"

3. Feature slides (5s each):
   - Dashboard: "All Berlin schools in one place"
   - Filters: "Filter by district, type, and performance"
   - Metrics: "Track year-over-year trends"
   - Details: "Detailed insights for every school"

4. Outro (3s): "Start your search at schoolnossa.de"

**Animations:**
- Fade transitions between slides
- Text should animate in with spring physics
- Screenshots should scale up slightly on entry

**Output:**
- 1920x1080 (HD)
- 30 fps
```

---

## Advanced Features

### Adding Background Music

```tsx
import { Audio, staticFile } from 'remotion';

export const VideoWithMusic = () => (
  <>
    <Audio src={staticFile('background-music.mp3')} volume={0.3} />
    {/* Rest of video */}
  </>
);
```

### Professional Transitions

```tsx
import { TransitionSeries, linearTiming } from '@remotion/transitions';
import { slide } from '@remotion/transitions/slide';
import { fade } from '@remotion/transitions/fade';

export const Slideshow = () => (
  <TransitionSeries>
    <TransitionSeries.Sequence durationInFrames={150}>
      <Slide1 />
    </TransitionSeries.Sequence>
    <TransitionSeries.Transition
      presentation={slide({ direction: 'from-right' })}
      timing={linearTiming({ durationInFrames: 30 })}
    />
    <TransitionSeries.Sequence durationInFrames={150}>
      <Slide2 />
    </TransitionSeries.Sequence>
  </TransitionSeries>
);
```

### Text Animations

```tsx
import { useCurrentFrame, spring, useVideoConfig } from 'remotion';

export const AnimatedTitle = ({ text }) => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();

  const scale = spring({
    frame,
    fps,
    config: { damping: 200 },
  });

  return (
    <h1 style={{ transform: `scale(${scale})` }}>
      {text}
    </h1>
  );
};
```

---

## Licensing Considerations

- **Individual/Non-profit**: Free
- **Companies ≤3 employees**: Free
- **Companies >3 employees**: Commercial license required

Check https://www.remotion.dev/license for current terms.

---

## Next Steps

1. **Prepare Screenshots**: Capture high-quality screenshots of SchoolNossa features
2. **Write Descriptions**: Draft short, compelling text for each feature
3. **Create Project**: Set up Remotion project in this repository
4. **Design & Iterate**: Use Remotion's live preview to refine the video
5. **Render**: Export final MP4

---

## Resources

- [Remotion Official Documentation](https://www.remotion.dev/docs)
- [Remotion GitHub Repository](https://github.com/remotion-dev/remotion)
- [Remotion + Claude Code Guide](https://www.remotion.dev/docs/ai/claude-code)
- [Remotion Transitions Documentation](https://www.remotion.dev/docs/transitioning)
- [Example Showcase](https://www.remotion.dev/showcase)
- [Remotion Animated Library](https://www.remotion-animated.dev/)
- [Free Templates & Effects](https://www.reactvideoeditor.com/remotion-templates)

---

## Conclusion

Remotion is an excellent choice for creating your SchoolNossa promotional video because:

1. **Perfect for your use case**: Screenshot slideshows with text are a core capability
2. **AI-powered workflow**: With Remotion Skills, you can generate video code through natural language
3. **Professional results**: Built-in animations and transitions look polished
4. **Full control**: Unlike drag-and-drop tools, you can programmatically generate variations
5. **React ecosystem**: Leverage existing knowledge and components

The recommended approach is to use **Remotion Skills with Claude Code** - this lets you describe your video in plain English and get working code generated automatically.
