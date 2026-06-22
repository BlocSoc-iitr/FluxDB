# FluxDB roadmap

A single static page that tracks what FluxDB has shipped and what's next — an
embedded, MVCC, OLTP storage engine written from scratch in Rust.

## Run it

```bash
cd planning
npm start          # → http://localhost:4321
```

Zero dependencies — the server and fonts are vendored, so it works offline. You
can also just open `index.html` in a browser; it's fully static.

## What it is

The landing page is a status-coded **roadmap timeline**: each phase carries a
badge — Shipped, In progress, Planned, or Future — a one-line summary, and a few
points on how it works or how it will be built. The status reflects the actual
state of the codebase, not aspiration.

Every phase also has **its own page** with the full implementation detail —
either how it works today (for shipped phases) or how it will be built (for
planned ones), written in third-person prose. Click a phase title, or its "How it
works" / "Implementation plan" link, to open it. Phase pages cross-link with
previous/next and a back link to the roadmap.

It is deliberately self-contained: the copy is written for a reader, not lifted
from internal notes, and it points at no other documents.

## Editing

All content lives in `assets/roadmap.js` (`window.FLUXDB`): the hero copy, the
spec sheet, and the `phases` array. Each phase has:

- `status`, `summary`, and `points` — what shows on the roadmap card (a point may
  carry its own `status` to show a mix of done and planned work within an
  in-progress phase);
- `detail` — the phase's own page: an array of `{ h, body }` blocks, where `body`
  is a list of paragraphs (strings) and bullet lists (arrays of strings).

Point-wise plans live in `window.FLUXDB.plans`, keyed by phase id — an array of
`{ s, d }` steps (lead + detail) rendered as a numbered "Implementation plan"
("Build order" for shipped phases) at the foot of each phase page.

Inline `` `code` `` is supported everywhere. Edit the file and reload — there is
no build step.

## Layout

```
planning/
  package.json        # npm start
  server.js           # zero-dependency static server
  index.html          # shell (top bar + content mount)
  assets/
    roadmap.js        # authored content — the single source for the page
    app.js            # renders the roadmap from roadmap.js
    styles.css        # IBM Plex type system, light-first, status palette
    fonts/            # IBM Plex Sans/Mono woff2 (latin subset, OFL)
  README.md
```
