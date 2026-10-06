<!-- ormdantic-benchmark-report -->
## Ormdantic benchmark report

**sqlite / ci:** Ormdantic is 2.20x vs SQLAlchemy and 2.35x vs SQLModel (geometric mean of comparable cases).

| Case | Ormdantic | vs SQLAlchemy | vs SQLModel | Base/head | Scope |
| --- | ---: | ---: | ---: | ---: | --- |
| schema create/drop | 6.045 ms | 2.80x (2.46–3.14) | 2.83x (2.50–3.59) | 1.05x (0.93–1.15) | comparable |
| raw batch insert | 81.079 ms | 1.67x (1.59–1.72) | 1.64x (1.60–1.71) | 1.02x (1.00–1.04) | comparable |
| orm insert models | 259.213 ms | 1.66x (1.58–1.69) | 2.63x (2.52–2.69) | 0.99x (0.95–1.03) | comparable |
| orm update filtered | 5.657 ms | 1.09x (1.05–1.16) | 1.07x (1.04–1.16) | 0.97x (0.94–1.50) | comparable |
| orm upsert mixed | 29.852 ms | 35.86x (34.60–38.35) | 39.25x (36.98–41.02) | 0.98x (0.96–1.02) | comparable |
| orm delete filtered | 5.847 ms | 1.24x (1.17–1.31) | 1.26x (1.18–1.32) | 0.99x (0.93–1.05) | comparable |
| count all rows | 0.401 ms | 3.63x (3.44–3.84) | 3.68x (3.39–3.92) | 0.97x (0.90–1.00) | comparable |
| count equality filter | 0.443 ms | 3.88x (3.56–4.29) | 3.79x (3.48–4.20) | 1.05x (0.93–1.15) | comparable |
| count range filter | 0.501 ms | 3.40x (3.26–3.67) | 3.61x (3.34–3.88) | 0.99x (0.96–1.14) | comparable |
| aggregate filtered rows | 0.868 ms | 2.44x (2.32–2.57) | 2.38x (2.25–2.54) | 1.00x (0.94–1.03) | comparable |
| scalar projection read | 1.566 ms | 2.13x (2.11–2.23) | 2.13x (2.09–2.26) | 0.97x (0.95–1.02) | comparable |
| batched primary-key lookup | 136.740 ms | 2.16x (2.12–2.26) | 2.21x (2.15–2.26) | 1.01x (1.00–1.01) | comparable |
| paginated find_many | 2.864 ms | 1.61x (1.56–1.68) | 1.82x (1.78–1.92) | 0.95x (0.92–0.99) | comparable |
| ordered find_many | 3.711 ms | 1.49x (1.39–6.73) | 1.67x (1.56–1.74) | 0.98x (0.92–1.02) | comparable |
| hydrate flat rows | 2.750 ms | 1.63x (1.61–1.79) | 1.89x (1.85–1.99) | 1.01x (0.98–1.02) | comparable |
| serialize simple payloads | 0.724 ms | 0.94x (0.90–0.96) | 1.99x (1.96–2.07) | 1.00x (0.98–1.01) | diagnostic |
| serialize nested payloads | 0.347 ms | 1.09x (1.01–1.16) | 1.23x (1.13–1.33) | 1.00x (0.90–1.08) | diagnostic |
| hydrate relationship results | 7.478 ms | 1.07x (0.97–1.10) | 1.14x (1.07–1.17) | 1.00x (0.93–1.07) | comparable |
| one-to-many relationship loading | 2.805 ms | 1.63x (1.57–1.78) | 1.79x (1.69–1.83) | 1.01x (0.99–1.02) | comparable |
| many-to-one relationship loading | 2.899 ms | 1.55x (1.43–1.62) | 1.58x (1.50–1.83) | 1.01x (0.93–1.04) | comparable |
| nested relationship loading | 6.377 ms | 1.49x (1.41–1.57) | 1.59x (1.47–1.66) | 0.99x (0.93–1.07) | comparable |
