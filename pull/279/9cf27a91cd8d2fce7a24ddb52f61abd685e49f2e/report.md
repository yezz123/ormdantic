<!-- ormdantic-benchmark-report -->
## Ormdantic benchmark report

**sqlite / ci:** Ormdantic is 2.19x vs SQLAlchemy and 2.33x vs SQLModel (geometric mean of comparable cases).

| Case | Ormdantic | vs SQLAlchemy | vs SQLModel | Base/head | Scope |
| --- | ---: | ---: | ---: | ---: | --- |
| schema create/drop | 6.235 ms | 2.82x (2.71–2.95) | 2.80x (2.71–2.92) | 0.98x (0.94–1.12) | comparable |
| raw batch insert | 81.649 ms | 1.56x (1.54–1.60) | 1.55x (1.55–1.60) | 1.00x (1.00–1.01) | comparable |
| orm insert models | 249.544 ms | 1.64x (1.62–1.64) | 2.67x (2.61–2.68) | 1.07x (0.99–1.08) | comparable |
| orm update filtered | 5.423 ms | 1.11x (1.08–1.16) | 1.12x (1.09–1.18) | 1.00x (0.99–1.03) | comparable |
| orm upsert mixed | 28.806 ms | 36.31x (35.74–37.72) | 39.89x (38.64–41.11) | 1.00x (0.99–1.07) | comparable |
| orm delete filtered | 5.671 ms | 1.24x (1.19–1.29) | 1.26x (1.19–1.28) | 1.00x (0.97–1.02) | comparable |
| count all rows | 0.360 ms | 3.83x (3.44–4.19) | 3.54x (3.42–3.95) | 0.90x (0.86–1.04) | comparable |
| count equality filter | 0.424 ms | 3.86x (3.27–4.07) | 3.92x (3.13–4.01) | 0.90x (0.77–0.93) | comparable |
| count range filter | 0.445 ms | 3.51x (3.20–3.79) | 3.51x (3.16–3.88) | 0.98x (0.90–1.03) | comparable |
| aggregate filtered rows | 0.819 ms | 2.32x (2.19–2.50) | 2.30x (2.13–2.46) | 1.01x (0.90–1.08) | comparable |
| scalar projection read | 1.414 ms | 2.17x (2.10–2.25) | 2.21x (2.16–2.31) | 1.03x (1.00–1.10) | comparable |
| batched primary-key lookup | 140.412 ms | 2.14x (2.07–2.22) | 2.14x (2.07–2.34) | 0.97x (0.96–0.98) | comparable |
| paginated find_many | 2.589 ms | 1.65x (1.54–1.75) | 1.89x (1.78–1.92) | 0.99x (0.94–1.03) | comparable |
| ordered find_many | 3.461 ms | 1.53x (1.36–6.65) | 1.69x (1.56–1.82) | 1.00x (0.91–1.03) | comparable |
| hydrate flat rows | 2.631 ms | 1.64x (1.53–1.80) | 1.91x (1.79–1.98) | 0.99x (0.93–1.06) | comparable |
| serialize simple payloads | 0.747 ms | 0.93x (0.90–0.94) | 1.93x (1.91–1.97) | 0.97x (0.94–1.00) | diagnostic |
| serialize nested payloads | 0.343 ms | 1.11x (1.04–1.17) | 1.29x (1.20–1.34) | 1.03x (0.96–1.16) | diagnostic |
| hydrate relationship results | 7.393 ms | 1.02x (0.95–1.08) | 1.11x (1.05–1.16) | 0.98x (0.94–1.03) | comparable |
| one-to-many relationship loading | 2.798 ms | 1.66x (1.48–1.79) | 1.72x (1.63–1.79) | 0.98x (0.95–1.01) | comparable |
| many-to-one relationship loading | 2.918 ms | 1.45x (1.32–1.53) | 1.54x (1.46–1.64) | 0.97x (0.92–1.02) | comparable |
| nested relationship loading | 6.228 ms | 1.44x (1.34–1.50) | 1.57x (1.46–1.64) | 0.98x (0.92–1.04) | comparable |
