<!-- ormdantic-benchmark-report -->
## Ormdantic benchmark report

**sqlite / ci:** Ormdantic is 2.15x vs SQLAlchemy and 2.28x vs SQLModel (geometric mean of comparable cases).

| Case | Ormdantic | vs SQLAlchemy | vs SQLModel | Base/head | Scope |
| --- | ---: | ---: | ---: | ---: | --- |
| schema create/drop | 4.967 ms | 2.62x (2.37–2.97) | 2.65x (2.40–2.98) | 1.09x (0.96–1.22) | comparable |
| raw batch insert | 75.162 ms | 1.63x (1.59–1.67) | 1.60x (1.56–1.65) | 1.00x (1.00–1.01) | comparable |
| orm insert models | 244.704 ms | 1.54x (1.51–1.56) | 2.46x (2.35–2.48) | 1.06x (0.99–1.13) | comparable |
| orm update filtered | 4.329 ms | 1.15x (1.14–1.20) | 1.14x (1.10–1.19) | 0.96x (0.93–1.03) | comparable |
| orm upsert mixed | 27.472 ms | 35.15x (33.59–36.23) | 37.16x (36.59–38.14) | 1.01x (0.99–1.03) | comparable |
| orm delete filtered | 4.650 ms | 1.27x (1.22–1.31) | 1.27x (1.21–1.31) | 0.98x (0.93–1.03) | comparable |
| count all rows | 0.386 ms | 3.34x (3.16–3.64) | 3.34x (3.16–3.58) | 0.93x (0.88–1.00) | comparable |
| count equality filter | 0.424 ms | 3.69x (3.43–4.13) | 3.66x (3.44–4.00) | 0.97x (0.90–1.07) | comparable |
| count range filter | 0.485 ms | 3.30x (3.22–3.62) | 3.16x (3.10–3.44) | 0.91x (0.87–0.99) | comparable |
| aggregate filtered rows | 0.837 ms | 2.23x (2.13–2.32) | 2.23x (2.14–2.49) | 0.97x (0.94–1.00) | comparable |
| scalar projection read | 1.495 ms | 2.10x (1.97–2.19) | 2.11x (1.97–2.17) | 1.08x (0.93–1.10) | comparable |
| batched primary-key lookup | 112.124 ms | 2.13x (2.09–2.38) | 2.32x (2.14–2.39) | 0.98x (0.90–1.01) | comparable |
| paginated find_many | 2.886 ms | 1.52x (1.47–1.61) | 1.71x (1.69–1.78) | 0.91x (0.89–1.00) | comparable |
| ordered find_many | 3.606 ms | 1.50x (1.45–7.62) | 1.65x (1.62–1.69) | 0.95x (0.95–0.97) | comparable |
| hydrate flat rows | 2.817 ms | 1.50x (1.41–1.60) | 1.75x (1.65–1.84) | 0.92x (0.86–1.03) | comparable |
| serialize simple payloads | 0.735 ms | 0.84x (0.82–0.91) | 1.90x (1.86–2.09) | 0.99x (0.96–1.07) | diagnostic |
| serialize nested payloads | 0.347 ms | 1.14x (1.01–1.32) | 1.25x (1.12–1.32) | 1.03x (0.93–1.15) | diagnostic |
| hydrate relationship results | 7.051 ms | 1.02x (0.97–1.07) | 1.10x (1.05–1.14) | 0.99x (0.96–1.07) | comparable |
| one-to-many relationship loading | 2.611 ms | 1.77x (1.67–1.87) | 1.88x (1.74–1.99) | 0.98x (0.96–1.01) | comparable |
| many-to-one relationship loading | 2.674 ms | 1.55x (1.44–1.62) | 1.65x (1.54–1.76) | 1.02x (0.97–1.04) | comparable |
| nested relationship loading | 5.950 ms | 1.51x (1.40–1.56) | 1.62x (1.54–1.70) | 0.99x (0.93–1.06) | comparable |
