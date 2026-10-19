<!-- ormdantic-benchmark-report -->
## Ormdantic benchmark report

**sqlite / ci:** Ormdantic is 2.21x vs SQLAlchemy and 2.35x vs SQLModel (geometric mean of comparable cases).

| Case | Ormdantic | vs SQLAlchemy | vs SQLModel | Base/head | Scope |
| --- | ---: | ---: | ---: | ---: | --- |
| schema create/drop | 6.142 ms | 2.69x (2.53–2.95) | 2.85x (2.59–3.25) | 0.97x (0.94–1.08) | comparable |
| raw batch insert | 82.526 ms | 1.62x (1.56–1.68) | 1.62x (1.57–1.67) | 0.99x (0.95–1.01) | comparable |
| orm insert models | 253.368 ms | 1.66x (1.63–1.72) | 2.57x (2.53–2.63) | 1.00x (0.98–1.01) | comparable |
| orm update filtered | 5.536 ms | 1.13x (1.11–1.18) | 1.12x (1.11–1.17) | 0.99x (0.95–1.04) | comparable |
| orm upsert mixed | 29.936 ms | 35.36x (33.99–37.46) | 37.73x (36.07–39.46) | 1.00x (0.95–1.02) | comparable |
| orm delete filtered | 5.959 ms | 1.29x (1.24–1.35) | 1.28x (1.24–1.40) | 0.98x (0.95–1.29) | comparable |
| count all rows | 0.373 ms | 3.84x (3.20–4.01) | 3.84x (3.21–4.07) | 1.07x (0.89–1.17) | comparable |
| count equality filter | 0.469 ms | 3.74x (3.53–3.91) | 3.73x (3.50–3.91) | 0.92x (0.85–0.97) | comparable |
| count range filter | 0.509 ms | 3.63x (3.32–3.87) | 3.58x (3.37–3.82) | 0.94x (0.85–1.03) | comparable |
| aggregate filtered rows | 0.870 ms | 2.42x (2.26–2.55) | 2.43x (2.28–2.49) | 0.99x (0.92–1.03) | comparable |
| scalar projection read | 1.569 ms | 2.17x (2.00–2.23) | 2.16x (1.98–2.22) | 0.97x (0.91–1.06) | comparable |
| batched primary-key lookup | 138.445 ms | 2.12x (2.05–2.25) | 2.20x (2.05–2.31) | 0.96x (0.94–1.01) | comparable |
| paginated find_many | 2.764 ms | 1.67x (1.59–1.72) | 1.90x (1.78–1.94) | 1.00x (0.95–1.03) | comparable |
| ordered find_many | 3.618 ms | 1.58x (1.50–7.32) | 1.68x (1.64–1.72) | 1.06x (1.03–1.08) | comparable |
| hydrate flat rows | 2.878 ms | 1.65x (1.52–1.76) | 1.89x (1.78–2.00) | 0.99x (0.94–1.10) | comparable |
| serialize simple payloads | 0.713 ms | 0.97x (0.94–1.00) | 2.03x (1.96–2.11) | 1.05x (0.98–1.08) | diagnostic |
| serialize nested payloads | 0.408 ms | 1.03x (0.94–1.15) | 1.07x (1.03–1.19) | 1.00x (0.90–1.05) | diagnostic |
| hydrate relationship results | 7.675 ms | 1.05x (0.94–1.08) | 1.09x (1.03–1.14) | 0.99x (0.95–1.04) | comparable |
| one-to-many relationship loading | 2.868 ms | 1.65x (1.54–1.84) | 1.79x (1.61–1.89) | 1.00x (0.94–1.09) | comparable |
| many-to-one relationship loading | 3.023 ms | 1.46x (1.42–1.70) | 1.63x (1.53–1.72) | 0.97x (0.96–1.01) | comparable |
| nested relationship loading | 6.543 ms | 1.45x (1.38–1.53) | 1.58x (1.49–1.64) | 1.00x (0.93–1.04) | comparable |
