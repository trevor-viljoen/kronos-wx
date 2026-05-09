/** @type {import('tailwind.config').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  darkMode: "class",
  theme: {
    extend: {
      "colors": {
        "tertiary-fixed-dim": "#ffb874",
        "secondary-fixed-dim": "#ffb3ac",
        "on-error-container": "#ffdad6",
        "on-secondary-fixed": "#410003",
        "outline": "#849495",
        "primary-fixed-dim": "#00dbe7",
        "surface-tint": "#00dbe7",
        "secondary-fixed": "#ffdad6",
        "background": "#12121f",
        "secondary-container": "#c40019",
        "on-error": "#690005",
        "on-tertiary-fixed-variant": "#6a3b00",
        "on-secondary-fixed-variant": "#930010",
        "on-primary-fixed-variant": "#004f54",
        "surface-container-low": "#1a1a27",
        "secondary": "#ffb3ac",
        "surface-container-lowest": "#0d0d19",
        "on-primary": "#00363a",
        "inverse-on-surface": "#2f2f3d",
        "on-background": "#e3e0f3",
        "error-container": "#93000a",
        "surface-container": "#1f1e2b",
        "on-tertiary": "#4b2800",
        "tertiary": "#fff6f1",
        "surface-container-high": "#292936",
        "surface": "#12121f",
        "error": "#ffb4ab",
        "primary-fixed": "#74f5ff",
        "tertiary-fixed": "#ffdcbf",
        "primary": "#e1fdff",
        "surface-bright": "#383846",
        "on-surface": "#e3e0f3",
        "outline-variant": "#3a494b",
        "on-tertiary-fixed": "#2d1600",
        "tertiary-container": "#ffd3ad",
        "surface-dim": "#12121f",
        "on-primary-container": "#006a71",
        "on-secondary": "#680008",
        "on-tertiary-container": "#8e5100",
        "primary-container": "#00f2ff",
        "inverse-surface": "#e3e0f3",
        "on-primary-fixed": "#002022",
        "surface-variant": "#343341",
        "on-secondary-container": "#ffd2cd",
        "inverse-primary": "#00696f",
        "surface-container-highest": "#343341",
        "on-surface-variant": "#b9cacb"
      },
      "keyframes": {
        "ticker": {
          "0%": { "transform": "translateX(0)" },
          "100%": { "transform": "translateX(-50%)" }
        }
      },
      "animation": {
        "ticker": "ticker 30s linear infinite"
      },
      "fontFamily": {
        "space": ["Space Grotesk", "sans-serif"],
        "mono": ["JetBrains Mono", "monospace"],
        "data-lg": ["JetBrains Mono"],
        "data-sm": ["JetBrains Mono"],
        "ui-bold": ["Space Grotesk"],
        "h1": ["Space Grotesk"],
        "data-md": ["JetBrains Mono"],
        "ui-medium": ["Space Grotesk"],
        "h3": ["Space Grotesk"],
        "h2": ["Space Grotesk"]
      },
    }
  },
  plugins: [],
}
