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

// Mock UI component that looks like an app screenshot
const MockAppUI: React.FC<{ type: string }> = ({ type }) => {
  const mockData = [
    { name: "Friedrich-Engels-Gymnasium", district: "Pankow", score: "1.8" },
    { name: "Sophie-Scholl-Schule", district: "Tempelhof", score: "2.1" },
    { name: "John-Lennon-Gymnasium", district: "Mitte", score: "1.6" },
    { name: "Heinrich-Hertz-Gymnasium", district: "Friedrichshain", score: "1.9" },
  ];

  return (
    <div
      style={{
        width: 900,
        height: 600,
        backgroundColor: "#0f0f1a",
        display: "flex",
        flexDirection: "column",
        padding: 0,
        overflow: "hidden",
      }}
    >
      {/* App Header */}
      <div
        style={{
          backgroundColor: "#1a1a2e",
          padding: "16px 24px",
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          borderBottom: "1px solid #2a2a4e",
        }}
      >
        <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
          <div
            style={{
              fontSize: 22,
              fontWeight: 700,
              color: "#fff",
            }}
          >
            School<span style={{ color: brandColors.accent }}>Nossa</span>
          </div>
        </div>
        <div style={{ display: "flex", gap: 20 }}>
          <div style={{ color: "#888", fontSize: 14 }}>Dashboard</div>
          <div style={{ color: "#888", fontSize: 14 }}>Schools</div>
          <div style={{ color: "#888", fontSize: 14 }}>Compare</div>
        </div>
      </div>

      {/* Content Area */}
      <div style={{ flex: 1, padding: 24, display: "flex", gap: 20 }}>
        {/* Sidebar Filters */}
        {type.includes("filter") && (
          <div
            style={{
              width: 200,
              backgroundColor: "#1a1a2e",
              borderRadius: 12,
              padding: 16,
            }}
          >
            <div style={{ color: "#fff", fontSize: 14, fontWeight: 600, marginBottom: 16 }}>
              Filters
            </div>
            {["District", "School Type", "Rating"].map((filter) => (
              <div
                key={filter}
                style={{
                  backgroundColor: "#2a2a4e",
                  borderRadius: 8,
                  padding: "10px 12px",
                  marginBottom: 10,
                  color: "#aaa",
                  fontSize: 13,
                }}
              >
                {filter} ▼
              </div>
            ))}
          </div>
        )}

        {/* Main Content */}
        <div style={{ flex: 1 }}>
          {/* Search Bar */}
          <div
            style={{
              backgroundColor: "#1a1a2e",
              borderRadius: 12,
              padding: "12px 20px",
              marginBottom: 20,
              color: "#666",
              fontSize: 14,
            }}
          >
            🔍 Search schools in Berlin...
          </div>

          {/* School Cards */}
          <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
            {mockData.map((school) => (
              <div
                key={school.name}
                style={{
                  backgroundColor: "#1a1a2e",
                  borderRadius: 12,
                  padding: 16,
                  display: "flex",
                  justifyContent: "space-between",
                  alignItems: "center",
                }}
              >
                <div>
                  <div style={{ color: "#fff", fontSize: 16, fontWeight: 600 }}>
                    {school.name}
                  </div>
                  <div style={{ color: "#888", fontSize: 13, marginTop: 4 }}>
                    {school.district}
                  </div>
                </div>
                <div
                  style={{
                    backgroundColor: brandColors.accent,
                    color: "#fff",
                    padding: "8px 16px",
                    borderRadius: 20,
                    fontSize: 14,
                    fontWeight: 600,
                  }}
                >
                  {school.score}
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* Stats Panel for metrics view */}
        {type.includes("metric") && (
          <div
            style={{
              width: 220,
              backgroundColor: "#1a1a2e",
              borderRadius: 12,
              padding: 16,
            }}
          >
            <div style={{ color: "#fff", fontSize: 14, fontWeight: 600, marginBottom: 16 }}>
              Trends 2024-2025
            </div>
            {[
              { label: "Avg Grade", value: "2.1", change: "+0.2" },
              { label: "Abitur Rate", value: "94%", change: "+3%" },
              { label: "Students", value: "1,240", change: "+45" },
            ].map((stat) => (
              <div
                key={stat.label}
                style={{
                  marginBottom: 16,
                  padding: 12,
                  backgroundColor: "#2a2a4e",
                  borderRadius: 8,
                }}
              >
                <div style={{ color: "#888", fontSize: 12 }}>{stat.label}</div>
                <div style={{ color: "#fff", fontSize: 20, fontWeight: 700 }}>
                  {stat.value}
                </div>
                <div style={{ color: "#4ade80", fontSize: 12 }}>↑ {stat.change}</div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
};

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
              border: "2px solid #2a2a4e",
            }}
          >
            <MockAppUI type={screenshot} />
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
