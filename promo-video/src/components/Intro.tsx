import React from "react";
import {
  AbsoluteFill,
  useCurrentFrame,
  useVideoConfig,
  spring,
  interpolate,
} from "remotion";
import { brandColors } from "../data/slides";

export const Intro: React.FC = () => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();

  // Logo scale animation
  const logoScale = spring({
    frame,
    fps,
    config: { damping: 200, stiffness: 100 },
  });

  // Tagline fade in (delayed)
  const taglineOpacity = interpolate(frame, [30, 60], [0, 1], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
  });

  const taglineY = interpolate(frame, [30, 60], [20, 0], {
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
      {/* Logo/Title */}
      <h1
        style={{
          fontSize: 120,
          fontWeight: 800,
          color: brandColors.text,
          margin: 0,
          transform: `scale(${logoScale})`,
          letterSpacing: "-2px",
        }}
      >
        School<span style={{ color: brandColors.accent }}>Nossa</span>
      </h1>

      {/* Tagline */}
      <p
        style={{
          fontSize: 36,
          color: brandColors.textMuted,
          marginTop: 30,
          opacity: taglineOpacity,
          transform: `translateY(${taglineY}px)`,
        }}
      >
        Find the perfect school for your child in Berlin
      </p>
    </AbsoluteFill>
  );
};
