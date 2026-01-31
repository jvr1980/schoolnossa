import React from "react";
import { Series } from "remotion";
import { Intro } from "./components/Intro";
import { ScreenshotSlide } from "./components/ScreenshotSlide";
import { CallToAction } from "./components/CallToAction";
import { slideContent } from "./data/slides";

/**
 * Main promotional video composition
 *
 * Structure:
 * 1. Intro (3 seconds)
 * 2. Feature slides (5 seconds each)
 * 3. Call to Action (4 seconds)
 */
export const PromoVideo: React.FC = () => {
  return (
    <Series>
      {/* Intro Section */}
      <Series.Sequence durationInFrames={90}>
        <Intro />
      </Series.Sequence>

      {/* Feature Slides */}
      {slideContent.map((slide) => (
        <Series.Sequence key={slide.id} durationInFrames={slide.durationInFrames}>
          <ScreenshotSlide
            screenshot={slide.screenshot}
            title={slide.title}
            description={slide.description}
          />
        </Series.Sequence>
      ))}

      {/* Call to Action */}
      <Series.Sequence durationInFrames={120}>
        <CallToAction />
      </Series.Sequence>
    </Series>
  );
};
