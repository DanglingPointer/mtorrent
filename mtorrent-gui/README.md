# mtorrent GUI

Simple GUI for mtorrent based on Tauri.

## Branding

The GUI now includes a vector logo at `src/assets/mtorrent.svg`.

It uses a gradient outer swarm ring, internal piece blocks, and a stylized `m` path.

Colors are controlled via CSS custom properties in `styles.css`:

```
--color-brand-mtorrent
--color-brand-mtorrent-accent
```

Dark mode (if enabled) swaps these via `[data-theme="dark"]` on the root element.

To change the brand color, adjust those variables; the hover glow uses `--color-brand-mtorrent-accent`.
