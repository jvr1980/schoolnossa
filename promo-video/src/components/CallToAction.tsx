import React from "react";
import {
  AbsoluteFill,
  useCurrentFrame,
  useVideoConfig,
  spring,
  interpolate,
} from "remotion";
import { brandColors } from "../data/slides";

export const CallToAction: React.FC = () => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();

  // Main text animation
  const textScale = spring({
    frame,
    fps,
    config: { damping: 200, stiffness: 100 },
  });

  // CTA button pulse
  const buttonScale = spring({
    frame: frame - 30,
    fps,
    config: { damping: 10, stiffness: 80 },
    from: 0.8,
    to: 1,
  });

  const buttonOpacity = interpolate(frame, [30, 50], [0, 1], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
  });

  // Website URL fade in
  const urlOpacity = interpolate(frame, [50, 70], [0, 1], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
  });

  return (
    <AbsoluteFill
      style={{
        backgroundColor: brandColors.primary,
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        justifyContent: "center",
        fontFamily: "Inter, system-ui, sans-serif",
      }}
    >
      {/* Main CTA text */}
      <h1
        style={{
          fontSize: 80,
          fontWeight: 800,
          color: brandColors.text,
          margin: 0,
          transform: `scale(${textScale})`,
          textAlign: "center",
        }}
      >
        Start Your Search Today
      </h1>

      {/* CTA Button */}
      <div
        style={{
          marginTop: 50,
          opacity: buttonOpacity,
          transform: `scale(${Math.max(0, buttonScale)})`,
        }}
      >
        <div
          style={{
            backgroundColor: brandColors.accent,
            padding: "20px 60px",
            borderRadius: 50,
            fontSize: 32,
            fontWeight: 600,
            color: brandColors.text,
          }}
        >
          Try SchoolNossa Free
        </div>
      </div>

      {/* Website URL */}
      <p
        style={{
          marginTop: 40,
          fontSize: 28,
          color: brandColors.textMuted,
          opacity: urlOpacity,
        }}
      >
        schoolnossa.de
      </p>
    </AbsoluteFill>
  );
};
