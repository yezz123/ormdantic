<!-- ormdantic-benchmark-report -->
## Ormdantic benchmark report

**sqlite / ci:** Ormdantic is 2.17x vs SQLAlchemy and 2.27x vs SQLModel (geometric mean of comparable cases).

| Case | Ormdantic | vs SQLAlchemy | vs SQLModel | Base/head | Scope |
| --- | ---: | ---: | ---: | ---: | --- |
| schema create/drop | 5.042 ms | 2.83x (2.65–2.97) | 2.70x (2.57–2.76) | 1.12x (1.05–1.20) | comparable |
| raw batch insert | 74.750 ms | 1.67x (1.63–1.68) | 1.66x (1.63–1.68) | 1.02x (1.00–1.03) | comparable |
| orm insert models | 240.031 ms | 1.64x (1.63–1.65) | 2.47x (2.45–2.49) | 1.01x (1.00–1.02) | comparable |
| orm update filtered | 5.357 ms | 1.15x (1.08–1.18) | 1.16x (1.10–1.20) | 1.02x (0.96–1.04) | comparable |
| orm upsert mixed | 28.556 ms | 33.36x (31.34–33.94) | 32.96x (32.28–34.32) | 0.99x (0.98–1.02) | comparable |
| orm delete filtered | 5.938 ms | 1.26x (1.23–1.29) | 1.26x (1.24–1.30) | 0.98x (0.96–1.00) | comparable |
| count all rows | 0.413 ms | 3.34x (3.21–3.65) | 3.33x (3.20–3.60) | 0.98x (0.93–1.09) | comparable |
| count equality filter | 0.443 ms | 3.61x (3.52–3.73) | 3.59x (3.51–3.74) | 1.05x (0.99–1.08) | comparable |
| count range filter | 0.526 ms | 3.16x (2.89–3.44) | 3.17x (2.88–3.28) | 0.99x (0.86–1.05) | comparable |
| aggregate filtered rows | 0.936 ms | 2.27x (2.23–2.49) | 2.28x (2.25–2.44) | 0.97x (0.94–1.02) | comparable |
| scalar projection read | 1.599 ms | 2.15x (2.13–2.24) | 2.15x (2.12–2.16) | 0.99x (0.96–1.00) | comparable |
| batched primary-key lookup | 117.274 ms | 2.15x (1.81–2.24) | 2.06x (1.77–2.14) | 0.99x (0.81–1.03) | comparable |
| paginated find_many | 3.096 ms | 1.69x (1.35–2.17) | 1.73x (1.50–1.90) | 0.91x (0.81–1.04) | comparable |
| ordered find_many | 3.866 ms | 1.47x (1.42–7.59) | 1.65x (1.61–1.71) | 0.97x (0.96–1.02) | comparable |
| hydrate flat rows | 2.909 ms | 1.57x (1.51–1.86) | 1.83x (1.76–1.93) | 0.96x (0.90–1.01) | comparable |
| serialize simple payloads | 0.766 ms | 0.88x (0.81–0.90) | 2.01x (1.88–2.06) | 0.98x (0.92–1.00) | diagnostic |
| serialize nested payloads | 0.375 ms | 0.96x (0.86–1.10) | 1.18x (1.05–1.38) | 0.96x (0.86–1.07) | diagnostic |
| hydrate relationship results | 7.546 ms | 1.01x (0.95–1.06) | 1.11x (1.04–1.16) | 0.98x (0.94–1.38) | comparable |
| one-to-many relationship loading | 2.914 ms | 1.83x (1.44–2.31) | 1.77x (1.48–1.93) | 0.96x (0.82–1.02) | comparable |
| many-to-one relationship loading | 2.963 ms | 1.53x (1.50–1.63) | 1.78x (1.66–2.13) | 0.98x (0.96–1.02) | comparable |
| nested relationship loading | 6.745 ms | 1.47x (1.35–1.61) | 1.55x (1.45–1.69) | 0.91x (0.86–1.03) | comparable |
