<!-- ormdantic-benchmark-report -->
## Ormdantic benchmark report

**sqlite / ci:** Ormdantic is 2.10x vs SQLAlchemy and 2.22x vs SQLModel (geometric mean of comparable cases).

| Case | Ormdantic | vs SQLAlchemy | vs SQLModel | Base/head | Scope |
| --- | ---: | ---: | ---: | ---: | --- |
| schema create/drop | 7.890 ms | 2.41x (2.33–2.55) | 2.52x (2.44–2.75) | 1.05x (1.01–2.17) | comparable |
| raw batch insert | 76.468 ms | 1.63x (1.60–1.70) | 1.62x (1.57–1.66) | 1.01x (1.00–1.03) | comparable |
| orm insert models | 254.235 ms | 1.50x (1.33–1.62) | 2.22x (1.96–2.37) | 1.04x (0.87–1.11) | comparable |
| orm update filtered | 4.871 ms | 1.12x (1.09–1.25) | 1.17x (1.11–1.20) | 1.01x (0.96–1.05) | comparable |
| orm upsert mixed | 30.340 ms | 32.92x (24.25–36.19) | 33.77x (25.23–37.19) | 0.91x (0.68–1.39) | comparable |
| orm delete filtered | 8.046 ms | 0.83x (0.72–1.07) | 0.83x (0.73–1.09) | 0.67x (0.58–0.85) | comparable |
| count all rows | 0.455 ms | 3.45x (3.16–3.75) | 3.36x (3.07–3.61) | 0.98x (0.92–1.09) | comparable |
| count equality filter | 0.521 ms | 3.37x (3.12–3.78) | 3.35x (3.13–3.65) | 0.95x (0.89–1.10) | comparable |
| count range filter | 0.563 ms | 3.08x (2.88–3.20) | 3.06x (2.88–3.18) | 1.02x (0.95–1.10) | comparable |
| aggregate filtered rows | 0.955 ms | 2.30x (2.20–2.47) | 2.20x (2.14–2.33) | 1.00x (0.97–1.04) | comparable |
| scalar projection read | 1.789 ms | 1.98x (1.90–2.17) | 1.98x (1.89–2.18) | 0.93x (0.89–1.05) | comparable |
| batched primary-key lookup | 116.506 ms | 2.21x (2.13–2.31) | 2.22x (2.13–2.29) | 1.00x (0.99–1.04) | comparable |
| paginated find_many | 2.731 ms | 1.66x (1.56–1.70) | 1.88x (1.80–1.94) | 1.04x (0.98–1.09) | comparable |
| ordered find_many | 3.762 ms | 1.48x (1.38–8.00) | 1.69x (1.56–1.74) | 0.98x (0.92–1.03) | comparable |
| hydrate flat rows | 2.729 ms | 1.69x (1.55–1.80) | 1.93x (1.80–2.01) | 1.01x (0.95–1.04) | comparable |
| serialize simple payloads | 0.610 ms | 0.87x (0.85–0.90) | 2.09x (2.00–2.12) | 1.04x (1.00–1.05) | diagnostic |
| serialize nested payloads | 0.339 ms | 1.10x (0.95–2.19) | 1.12x (1.08–1.27) | 0.96x (0.88–1.05) | diagnostic |
| hydrate relationship results | 6.896 ms | 1.09x (1.00–1.11) | 1.16x (1.09–1.21) | 0.99x (0.94–1.07) | comparable |
| one-to-many relationship loading | 2.820 ms | 1.83x (1.64–2.06) | 1.92x (1.72–2.06) | 0.95x (0.93–0.99) | comparable |
| many-to-one relationship loading | 2.858 ms | 1.68x (1.50–1.70) | 1.69x (1.63–1.86) | 0.97x (0.94–1.02) | comparable |
| nested relationship loading | 6.102 ms | 1.54x (1.46–1.59) | 1.62x (1.53–1.74) | 1.01x (0.94–1.09) | comparable |
