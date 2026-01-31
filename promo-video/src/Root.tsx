import React from "react";
import { Composition } from "remotion";
import { PromoVideo } from "./PromoVideo";
import { slideContent, videoConfig } from "./data/slides";

/**
 * Root component that registers all video compositions
 *
 * To add more video variations, add more <Composition> components here.
 */

// Calculate total duration based on slide content
const introDuration = 90;  // 3 seconds
const ctaDuration = 120;   // 4 seconds
const slidesDuration = slideContent.reduce((acc, slide) => acc + slide.durationInFrames, 0);
const totalDuration = introDuration + slidesDuration + ctaDuration;

export const RemotionRoot: React.FC = () => {
  return (
    <>
      {/* Main Promo Video */}
      <Composition
        id="PromoVideo"
        component={PromoVideo}
        durationInFrames={totalDuration}
        fps={videoConfig.fps}
        width={videoConfig.width}
        height={videoConfig.height}
      />

      {/* You can add more compositions here for variations */}
      {/* Example: Short version for social media */}
      {/* <Composition
        id="PromoVideoShort"
        component={PromoVideoShort}
        durationInFrames={450}
        fps={30}
        width={1080}
        height={1080}
      /> */}
    </>
  );
};
