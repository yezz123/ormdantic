<!-- ormdantic-benchmark-report -->
## Ormdantic benchmark report

**sqlite / ci:** Ormdantic is 2.07x vs SQLAlchemy and 2.20x vs SQLModel (geometric mean of comparable cases).

| Case | Ormdantic | vs SQLAlchemy | vs SQLModel | Base/head | Scope |
| --- | ---: | ---: | ---: | ---: | --- |
| schema create/drop | 7.804 ms | 2.46x (2.36–2.65) | 2.48x (2.43–2.56) | 1.08x (1.07–1.20) | comparable |
| raw batch insert | 62.370 ms | 1.79x (1.74–1.88) | 1.76x (1.75–1.90) | 1.00x (0.98–1.05) | comparable |
| orm insert models | 204.324 ms | 1.50x (1.43–1.57) | 2.21x (2.05–2.25) | 1.11x (0.99–1.17) | comparable |
| orm update filtered | 4.607 ms | 1.07x (0.95–1.20) | 1.10x (0.95–1.17) | 1.00x (0.87–1.85) | comparable |
| orm upsert mixed | 23.391 ms | 34.39x (34.07–35.63) | 35.91x (34.99–37.42) | 1.00x (0.99–1.03) | comparable |
| orm delete filtered | 8.139 ms | 0.67x (0.65–1.22) | 0.67x (0.64–1.23) | 0.70x (0.57–1.71) | comparable |
| count all rows | 0.348 ms | 3.22x (3.09–3.42) | 3.48x (3.29–3.68) | 0.99x (0.90–1.08) | comparable |
| count equality filter | 0.364 ms | 3.51x (3.39–3.92) | 3.58x (3.37–3.92) | 0.97x (0.89–1.05) | comparable |
| count range filter | 0.391 ms | 3.31x (3.06–3.79) | 3.30x (3.06–3.65) | 1.00x (0.93–1.11) | comparable |
| aggregate filtered rows | 0.768 ms | 2.24x (2.05–2.48) | 2.33x (2.08–2.46) | 0.97x (0.95–1.01) | comparable |
| scalar projection read | 1.283 ms | 2.11x (2.04–2.19) | 2.16x (2.04–2.19) | 1.02x (0.98–1.08) | comparable |
| batched primary-key lookup | 93.690 ms | 2.23x (2.10–2.30) | 2.18x (2.06–2.41) | 1.01x (0.98–1.03) | comparable |
| paginated find_many | 2.155 ms | 1.62x (1.56–1.69) | 1.86x (1.82–1.92) | 1.03x (0.99–1.06) | comparable |
| ordered find_many | 2.976 ms | 1.48x (1.38–7.99) | 1.61x (1.56–1.64) | 1.00x (0.97–1.02) | comparable |
| hydrate flat rows | 2.149 ms | 1.63x (1.59–1.68) | 1.87x (1.81–1.88) | 1.07x (1.05–1.11) | comparable |
| serialize simple payloads | 0.501 ms | 0.87x (0.84–0.90) | 2.02x (1.94–2.10) | 1.01x (0.97–1.08) | diagnostic |
| serialize nested payloads | 0.249 ms | 1.00x (0.95–1.11) | 1.20x (1.14–1.33) | 1.05x (0.97–1.18) | diagnostic |
| hydrate relationship results | 5.576 ms | 1.01x (0.93–1.07) | 1.09x (1.00–1.14) | 1.02x (0.95–1.09) | comparable |
| one-to-many relationship loading | 2.261 ms | 1.77x (1.49–2.01) | 1.73x (1.68–1.90) | 1.02x (0.98–1.06) | comparable |
| many-to-one relationship loading | 2.293 ms | 1.55x (1.52–1.67) | 1.64x (1.54–1.74) | 1.01x (1.00–1.05) | comparable |
| nested relationship loading | 4.981 ms | 1.44x (1.39–1.56) | 1.59x (1.48–1.67) | 1.00x (0.97–1.08) | comparable |
