# Design System Specification: Observability Intelligence

## 1. Overview & Creative North Star
**Creative North Star: "The Cognitive Architecture"**

This design system is engineered for the high-velocity world of observability. It moves beyond the "standard dashboard" by treating data not as static charts, but as a living, breathing map of system health.

To achieve a signature, high-end feel, we break the "template" look through **Intentional Asymmetry** and **Tonal Depth**. While the industry standard relies on rigid grids and heavy borders, this system utilizes a "nested" layout strategy. We prioritize information density without sacrificing clarity, using high-contrast typography and subtle glassmorphism to create a UI that feels like a premium, specialized instrument rather than a generic web app.

---

## 2. Colors & Surface Philosophy

### The Tonal Palette
Our palette is rooted in deep, atmospheric charcoals to reduce eye strain during long-tail debugging sessions, punctuated by vibrant, high-performance accent colors.

* **Background:** `#0b1326` (The canvas)
* **Primary (Otel Blue):** `#8ed5ff` | **Container:** `#38bdf8`
* **Secondary (Action):** `#c0c1ff` | **Container:** `#3131c0`
* **Tertiary (Success Green):** `#56e5a9` | **Container:** `#30c88f`
* **Error (Critical Red):** `#ffb4ab` | **Container:** `#93000a`

### The "No-Line" Rule
To maintain an editorial, high-end aesthetic, **prohibit the use of 1px solid borders for primary sectioning.** Instead, boundaries must be defined through:
1. **Background Color Shifts:** Use `surface-container-low` against `background` to define logical groupings.
2. **The Glass & Gradient Rule:** For floating panels (modals, command menus), use `surface-variant` with a 60% opacity and a `20px` backdrop-blur. Apply a subtle linear gradient (from `primary` to `primary-container` at 5% opacity) to provide a "sheen" that conveys a premium finish.

### Surface Hierarchy & Nesting
Treat the UI as stacked sheets of fine material.
* **Level 0:** `surface_dim` (`#0b1326`) – The base.
* **Level 1:** `surface_container_low` (`#131b2e`) – Secondary sidebars or navigation wraps.
* **Level 2:** `surface_container` (`#171f33`) – Primary dashboard widgets.
* **Level 3:** `surface_container_highest` (`#2d3449`) – Active states, hover highlights, or nested "drill-down" cards.

---

## 3. Typography
We use **Inter** for its mathematical precision and exceptional readability at small sizes.

* **Display (lg/md/sm):** Used for "Big Numbers" (e.g., total error count). Use `display-sm` (2.25rem) with a negative letter-spacing (-0.02em) for an authoritative, "Linear-inspired" look.
* **Headline (lg/md/sm):** Reserved for page titles and major section headers.
* **Title (lg/md/sm):** The workhorse for widget titles. Use `title-sm` (1rem) for most headers to maintain high density.
* **Body (lg/md/sm):** For data labels and descriptions. `body-sm` (0.75rem) is the standard for metric units.
* **Label (md/sm):** Micro-copy, timestamps, and breadcrumbs.

**Editorial Tip:** Use high-contrast color shifts. Primary data points should use `on_surface`, while secondary metadata should drop to `on_surface_variant`.

---

## 4. Elevation & Depth

### The Layering Principle
Depth is achieved through **Tonal Layering**. Instead of shadows, place a `surface_container_lowest` widget inside a `surface_container_high` sidebar. This "inverted lift" creates a sophisticated, recessed look typical of high-end audio hardware or pro-grade developer tools.

### Ambient Shadows
For floating elements like tooltips or popovers, use **Ambient Shadows**:
* `Shadow:` 0px 8px 32px rgba(6, 14, 32, 0.5)
* The shadow color is a tinted version of `surface_container_lowest` to ensure it feels like a natural part of the dark-mode environment.

### The "Ghost Border" Fallback
If a border is required for DAG nodes or subgraph containers, use a **Ghost Border**:
* **Stroke:** 1px
* **Color:** `outline_variant` at 20% opacity.
* **Effect:** This provides a hint of structure without cluttering the visual field during high-density data visualization.

---

## 5. Components

### Navigation & Breadcrumbs
* **Breadcrumb-Rich Navigation:** Use `label-md` for the path. The current terminal node should be `on_surface` (high contrast), while parent nodes are `outline` (subdued). Separate with a chevron at 40% opacity.

### Data Visualization Primitives
* **DAG Nodes:** Background `surface_container_high`, 8px radius. Use a `primary` ghost border (20% opacity) for selected nodes.
* **Subgraph Containers:** `surface_container_lowest` background with a subtle dashed `outline_variant` border to indicate scope.
* **Metric Sparklines:** 1.5pt stroke width. Use `tertiary` for healthy metrics and `error` for spikes. Fill the area under the curve with a 10% opacity gradient of the stroke color.
* **Trace Timelines:** Use `surface_variant` for the track and `primary` or `primary_fixed_dim` for the span bars.

### Input & Action
* **Buttons:**
* *Primary:* `primary` background with `on_primary` text. No border. 8px radius.
* *Secondary:* `surface_container_highest` background with a ghost border.
* **Cards & Lists:** **Forbid divider lines.** Separate list items using 4px of vertical whitespace or a 2% background color shift on hover.
* **Chips:** Use `surface_container_highest` with `label-sm` text. For status chips (Success/Error), use a 2px "status dot" next to the text rather than coloring the whole chip.

---

## 6. Do’s and Don’ts

### Do
* **Use Density as a Feature:** Pack data tightly, but use `title-sm` headers to create clear entry points for the eye.
* **Embrace Glassmorphism:** Use backdrop-blur on the global navigation bar to let the dashboard metrics "bleed" through as the user scrolls.
* **Layer Surfaces:** Always place a lighter surface on a darker background to create a "lift."

### Don’t
* **Don't use 100% white text:** Always use `on_surface` (`#dae2fd`) to prevent visual vibration against the dark background.
* **Don't use solid 1px borders for layout:** Use background tonal shifts or negative space instead.
* **Don't use standard shadows:** Avoid black, high-opacity shadows. Keep them diffused and tinted.
* **Don't use "Default" Roundedness:** Stick strictly to the **8px (0.5rem)** radius for consistency, except for full-pill chips.

---
*End of Document*