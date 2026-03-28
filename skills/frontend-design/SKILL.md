---
name: frontend-design
description: Create distinctive, production-grade frontend interfaces with high design quality. Use this skill when the user asks to build web components, pages, or applications. Generates creative, polished code that avoids generic AI aesthetics.
---

This skill guides creation of distinctive, production-grade frontend interfaces that avoid generic "AI slop" aesthetics. Implement real working code with exceptional attention to aesthetic details and creative choices.

The user provides frontend requirements: a component, page, application, or interface to build. They may include context about the purpose, audience, or technical constraints.

## Design Thinking

Before coding, understand the context and commit to a BOLD aesthetic direction:
- **Purpose**: What problem does this interface solve? Who uses it?
- **Tone**: Pick an extreme: brutally minimal, maximalist chaos, retro-futuristic, organic/natural, luxury/refined, playful/toy-like, editorial/magazine, brutalist/raw, art deco/geometric, soft/pastel, industrial/utilitarian, etc. There are so many flavors to choose from. Use these for inspiration but design one that is true to the aesthetic direction.
- **Constraints**: Technical requirements (framework, performance, accessibility).
- **Differentiation**: What makes this UNFORGETTABLE? What's the one thing someone will remember?

**CRITICAL**: Choose a clear conceptual direction and execute it with precision. Bold maximalism and refined minimalism both work - the key is intentionality, not intensity.

Then implement working code (HTML/CSS/JS, React, Vue, etc.) that is:
- Production-grade and functional
- Visually striking and memorable
- Cohesive with a clear aesthetic point-of-view
- Meticulously refined in every detail

## Frontend Aesthetics Guidelines

Focus on:
- **Typography**: Choose fonts that are beautiful, unique, and interesting. Avoid generic fonts like Arial and Inter; opt instead for distinctive choices that elevate the frontend's aesthetics; unexpected, characterful font choices. Pair a distinctive display font with a refined body font.
- **Color & Theme**: Commit to a cohesive aesthetic. Use CSS variables for consistency. Dominant colors with sharp accents outperform timid, evenly-distributed palettes.
- **Motion**: Use animations for effects and micro-interactions. Prioritize CSS-only solutions for HTML. Use Motion library for React when available. Focus on high-impact moments: one well-orchestrated page load with staggered reveals (animation-delay) creates more delight than scattered micro-interactions. Use scroll-triggering and hover states that surprise.
- **Spatial Composition**: Unexpected layouts. Asymmetry. Overlap. Diagonal flow. Grid-breaking elements. Generous negative space OR controlled density.
- **Backgrounds & Visual Details**: Create atmosphere and depth rather than defaulting to solid colors. Add contextual effects and textures that match the overall aesthetic. Apply creative forms like gradient meshes, noise textures, geometric patterns, layered transparencies, dramatic shadows, decorative borders, custom cursors, and grain overlays.

NEVER use generic AI-generated aesthetics like overused font families (Inter, Roboto, Arial, system fonts), cliched color schemes (particularly purple gradients on white backgrounds), predictable layouts and component patterns, and cookie-cutter design that lacks context-specific character.

Interpret creatively and make unexpected choices that feel genuinely designed for the context. No design should be the same. Vary between light and dark themes, different fonts, different aesthetics. NEVER converge on common choices (Space Grotesk, for example) across generations.

**IMPORTANT**: Match implementation complexity to the aesthetic vision. Maximalist designs need elaborate code with extensive animations and effects. Minimalist or refined designs need restraint, precision, and careful attention to spacing, typography, and subtle details. Elegance comes from executing the vision well.

Remember: Claude is capable of extraordinary creative work. Don't hold back, show what can truly be created when thinking outside the box and committing fully to a distinctive vision.

---

## FlycommC2 Context — C2 Spectral Awareness Dashboard

**This section is specific to the FlycommC2 project.**

### Aesthetic Direction: Military-Grade Industrial NOC

FlycommC2 is a **mission-critical RF threat detection Command & Control dashboard** used by security analysts and RF engineers in SOC/NOC environments. The aesthetic must be:

- **Industrial/Utilitarian** — every element serves a purpose, zero decoration
- **Dark theme is mandatory** — operators work in dimmed rooms for hours
- **Information-dense but readable** — large monitors (24"+), lots of data, clear hierarchy
- **Color is functional, never decorative** — red = CRITICAL, orange = HIGH, yellow = MEDIUM, green = healthy, cyan = accent/interactive, gray = inactive

### Typography for C2

- **Data values**: Use a distinctive monospace — `JetBrains Mono`, `IBM Plex Mono`, or `Fira Code` (load from Google Fonts CDN). NOT generic Consolas/Courier.
- **Labels and headers**: Use `IBM Plex Sans`, `DM Sans`, or `Outfit` — clean, professional, military-grade feel. NOT system-ui or Arial.
- **Import fonts**: Add Google Fonts link in `<head>`:
  ```html
  <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&family=IBM+Plex+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
  ```

### Color Palette (refined from current CSS variables)

```css
:root {
  --bg-primary: #080c14;      /* Deeper, more contrast */
  --bg-secondary: #0f1520;    /* Subtle lift */
  --bg-card: #151d2b;         /* Card surfaces */
  --bg-elevated: #1a2435;     /* Hover/active states */
  --border: #1e2d40;          /* Subtle borders */
  --border-active: #2a4060;   /* Active/focused borders */

  --text-primary: #e8edf5;    /* Slightly warmer white */
  --text-secondary: #8b9bb5;  /* Readable secondary */
  --text-dim: #556580;        /* Ambient/tertiary */

  --critical: #ff3b3b;        /* Brighter red — CRITICAL only */
  --critical-bg: rgba(255, 59, 59, 0.12);
  --critical-glow: rgba(255, 59, 59, 0.25);

  --high: #ff8c00;            /* Pure orange — HIGH only */
  --high-bg: rgba(255, 140, 0, 0.12);

  --medium: #ffc107;          /* Amber — MEDIUM only */
  --medium-bg: rgba(255, 193, 7, 0.12);

  --low: #5c6b80;
  --green: #00e676;           /* Vivid green — online/healthy */
  --green-bg: rgba(0, 230, 118, 0.12);

  --accent: #40a9ff;          /* Clean blue — interactive */
  --accent-bg: rgba(64, 169, 255, 0.10);

  --cyan: #00d4aa;            /* Teal — RSU/sensor accent */
  --cyan-bg: rgba(0, 212, 170, 0.08);

  --purple: #b388ff;          /* Purple — special indicators */
}
```

### Micro-animations (CSS only, no libraries)

```css
/* CRITICAL threat pulse — for stat card border */
@keyframes criticalPulse {
  0%, 100% { box-shadow: 0 0 8px var(--critical-bg), inset 0 0 0 1px var(--critical); }
  50% { box-shadow: 0 0 20px var(--critical-glow), inset 0 0 0 1px var(--critical); }
}

/* Scan progress bar sweep */
@keyframes scanSweep {
  0% { transform: translateX(-100%); }
  100% { transform: translateX(100%); }
}

/* Status dot pulse for ONLINE indicator */
@keyframes statusPulse {
  0%, 100% { opacity: 1; }
  50% { opacity: 0.5; }
}

/* Subtle fade-in for new data rows */
@keyframes fadeInRow {
  from { opacity: 0; transform: translateY(-4px); }
  to { opacity: 1; transform: translateY(0); }
}
```

### Design Principles for FlycommC2

1. **No rounded corners > 6px** — this is military/industrial, not consumer
2. **1px borders only** — subtle separation, not decoration
3. **Uppercase section headers with letter-spacing** — `text-transform: uppercase; letter-spacing: 1.5px; font-size: 10px`
4. **Monospace for ALL numeric data** — signal values, counts, IDs, timestamps
5. **Tables: tight rows, hover highlight, severity pills** — not colored text, actual pill badges with background
6. **Cards: subtle border, no shadow** — lift through border color, not box-shadow (except CRITICAL glow)
7. **Map: dark tiles, minimal controls** — the map serves the data, not the other way around
8. **Donut chart: severity colors (red/orange/yellow)** — NOT per-rule colors
9. **Timeline chart: proper axes, grid, legend** — not a raw canvas blob
10. **Transitions on everything interactive**: `transition: all 0.15s ease` — but keep it fast (150ms max)
