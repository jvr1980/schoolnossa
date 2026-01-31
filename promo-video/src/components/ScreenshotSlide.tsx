import React from "react";
import {
  AbsoluteFill,
  Img,
  useCurrentFrame,
  useVideoConfig,
  spring,
  interpolate,
  staticFile,
} from "remotion";
import { brandColors } from "../data/slides";

interface ScreenshotSlideProps {
  screenshot: string;
  title: string;
  description: string;
}

export const ScreenshotSlide: React.FC<ScreenshotSlideProps> = ({
  screenshot,
  title,
  description,
}) => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();

  // Screenshot entrance animation
  const screenshotScale = spring({
    frame,
    fps,
    config: { damping: 200, stiffness: 80 },
    from: 0.9,
    to: 1,
  });

  const screenshotOpacity = interpolate(frame, [0, 20], [0, 1], {
    extrapolateRight: "clamp",
  });

  // Text animations (delayed)
  const titleOpacity = interpolate(frame, [15, 35], [0, 1], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
  });

  const titleX = interpolate(frame, [15, 35], [-30, 0], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
  });

  const descOpacity = interpolate(frame, [25, 45], [0, 1], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
  });

  const descX = interpolate(frame, [25, 45], [-30, 0], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
  });

  return (
    <AbsoluteFill
      style={{
        backgroundColor: brandColors.primary,
        fontFamily: "Inter, system-ui, sans-serif",
      }}
    >
      {/* Layout: Screenshot on right, text on left */}
      <div
        style={{
          display: "flex",
          width: "100%",
          height: "100%",
          padding: 80,
          boxSizing: "border-box",
        }}
      >
        {/* Text Section */}
        <div
          style={{
            flex: 1,
            display: "flex",
            flexDirection: "column",
            justifyContent: "center",
            paddingRight: 60,
          }}
        >
          <h2
            style={{
              fontSize: 64,
              fontWeight: 700,
              color: brandColors.text,
              margin: 0,
              marginBottom: 30,
              opacity: titleOpacity,
              transform: `translateX(${titleX}px)`,
              lineHeight: 1.2,
            }}
          >
            {title}
          </h2>
          <p
            style={{
              fontSize: 28,
              color: brandColors.textMuted,
              margin: 0,
              lineHeight: 1.6,
              opacity: descOpacity,
              transform: `translateX(${descX}px)`,
            }}
          >
            {description}
          </p>
        </div>

        {/* Screenshot Section */}
        <div
          style={{
            flex: 1.2,
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
          }}
        >
          <div
            style={{
              opacity: screenshotOpacity,
              transform: `scale(${screenshotScale})`,
              boxShadow: "0 20px 60px rgba(0, 0, 0, 0.5)",
              borderRadius: 16,
              overflow: "hidden",
              border: `2px solid ${brandColors.secondary}`,
            }}
          >
            {/* Placeholder for screenshot - replace with actual Img when screenshots are available */}
            <div
              style={{
                width: 900,
                height: 600,
                backgroundColor: brandColors.secondary,
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                color: brandColors.textMuted,
                fontSize: 24,
              }}
            >
              {/* When you have actual screenshots, uncomment this: */}
              {/* <Img src={staticFile(screenshot)} style={{ width: '100%', height: '100%', objectFit: 'cover' }} /> */}
              Screenshot: {screenshot}
            </div>
          </div>
        </div>
      </div>

      {/* Accent line at bottom */}
      <div
        style={{
          position: "absolute",
          bottom: 0,
          left: 0,
          right: 0,
          height: 6,
          backgroundColor: brandColors.accent,
        }}
      />
    </AbsoluteFill>
  );
};
