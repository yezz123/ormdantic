<!-- ormdantic-benchmark-report -->
## Ormdantic benchmark report

**sqlite / ci:** Ormdantic is 2.20x vs SQLAlchemy and 2.35x vs SQLModel (geometric mean of comparable cases).

| Case | Ormdantic | vs SQLAlchemy | vs SQLModel | Base/head | Scope |
| --- | ---: | ---: | ---: | ---: | --- |
| schema create/drop | 6.632 ms | 2.62x (1.90–3.02) | 2.68x (1.95–2.94) | 0.92x (0.66–1.00) | comparable |
| raw batch insert | 85.879 ms | 1.80x (1.71–1.84) | 1.69x (1.66–1.75) | 0.95x (0.93–0.96) | comparable |
| orm insert models | 288.174 ms | 1.66x (1.64–1.68) | 2.62x (2.52–2.68) | 0.90x (0.89–0.91) | comparable |
| orm update filtered | 6.179 ms | 1.11x (1.08–1.16) | 1.09x (1.06–1.14) | 0.90x (0.88–1.02) | comparable |
| orm upsert mixed | 33.171 ms | 34.53x (32.80–35.73) | 35.02x (33.50–37.32) | 0.91x (0.87–0.94) | comparable |
| orm delete filtered | 6.445 ms | 1.31x (1.24–1.37) | 1.34x (1.24–1.39) | 0.94x (0.90–0.98) | comparable |
| count all rows | 0.516 ms | 3.63x (3.34–3.95) | 3.39x (3.11–3.64) | 0.82x (0.73–0.88) | comparable |
| count equality filter | 0.585 ms | 3.67x (3.12–4.38) | 3.97x (3.26–5.01) | 0.79x (0.66–0.95) | comparable |
| count range filter | 0.587 ms | 3.65x (3.30–3.78) | 3.52x (3.15–3.82) | 0.86x (0.78–0.91) | comparable |
| aggregate filtered rows | 1.028 ms | 2.47x (2.38–2.63) | 2.55x (2.41–2.80) | 0.91x (0.84–0.96) | comparable |
| scalar projection read | 1.916 ms | 2.15x (1.89–2.33) | 2.09x (1.89–2.31) | 0.85x (0.76–0.91) | comparable |
| batched primary-key lookup | 142.918 ms | 2.22x (2.11–2.33) | 2.33x (2.07–2.42) | 0.97x (0.95–0.99) | comparable |
| paginated find_many | 3.211 ms | 1.61x (1.47–1.70) | 1.85x (1.71–2.05) | 0.92x (0.86–0.98) | comparable |
| ordered find_many | 4.341 ms | 1.46x (1.43–9.08) | 1.63x (1.57–1.69) | 0.92x (0.88–0.97) | comparable |
| hydrate flat rows | 3.171 ms | 1.59x (1.55–1.65) | 1.87x (1.78–1.98) | 0.93x (0.89–0.95) | comparable |
| serialize simple payloads | 0.784 ms | 0.96x (0.83–1.02) | 2.08x (1.75–2.36) | 1.06x (0.87–1.16) | diagnostic |
| serialize nested payloads | 0.418 ms | 1.05x (0.89–1.19) | 1.28x (1.13–1.39) | 0.93x (0.84–1.07) | diagnostic |
| hydrate relationship results | 7.797 ms | 1.02x (0.95–1.06) | 1.11x (1.05–1.21) | 0.98x (0.96–1.03) | comparable |
| one-to-many relationship loading | 2.882 ms | 1.64x (1.59–1.78) | 1.80x (1.77–2.00) | 1.03x (0.97–1.17) | comparable |
| many-to-one relationship loading | 2.978 ms | 1.58x (1.51–1.71) | 1.71x (1.61–1.83) | 1.03x (1.00–1.09) | comparable |
| nested relationship loading | 6.607 ms | 1.49x (1.39–1.55) | 1.65x (1.50–1.72) | 1.05x (0.97–1.09) | comparable |
