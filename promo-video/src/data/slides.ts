/**
 * Slide content configuration
 *
 * Update this file with your actual screenshots and descriptions.
 * Screenshots should be placed in /public/screenshots/
 */

export interface SlideData {
  id: string;
  screenshot: string;
  title: string;
  description: string;
  durationInFrames: number;
}

export const slideContent: SlideData[] = [
  {
    id: "dashboard",
    screenshot: "/screenshots/dashboard.png",
    title: "All Berlin Schools at a Glance",
    description: "Browse comprehensive data on every school in Berlin with real-time updates from official sources.",
    durationInFrames: 150, // 5 seconds at 30fps
  },
  {
    id: "filters",
    screenshot: "/screenshots/filters.png",
    title: "Smart Filtering",
    description: "Filter by district, school type, and key performance metrics to find schools that match your criteria.",
    durationInFrames: 150,
  },
  {
    id: "metrics",
    screenshot: "/screenshots/metrics.png",
    title: "Track Performance Over Time",
    description: "Year-over-year trends help you understand how schools are improving or changing.",
    durationInFrames: 150,
  },
  {
    id: "details",
    screenshot: "/screenshots/details.png",
    title: "Detailed School Insights",
    description: "Get comprehensive information including Abitur success rates, student-teacher ratios, and more.",
    durationInFrames: 150,
  },
];

export const brandColors = {
  primary: "#1a1a2e",      // Dark blue
  secondary: "#16213e",    // Slightly lighter blue
  accent: "#e94560",       // Coral red
  text: "#ffffff",         // White
  textMuted: "#a0a0a0",    // Gray
};

export const videoConfig = {
  width: 1920,
  height: 1080,
  fps: 30,
  durationInFrames: 900, // 30 seconds total (will be calculated)
};
