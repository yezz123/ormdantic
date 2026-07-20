<!-- ormdantic-benchmark-report -->
## Ormdantic benchmark report

**sqlite / ci:** Ormdantic is 2.16x vs SQLAlchemy and 2.29x vs SQLModel (geometric mean of comparable cases).

| Case | Ormdantic | vs SQLAlchemy | vs SQLModel | Base/head | Scope |
| --- | ---: | ---: | ---: | ---: | --- |
| schema create/drop | 5.153 ms | 2.81x (2.71–2.90) | 2.78x (2.66–2.93) | 1.13x (1.11–1.25) | comparable |
| raw batch insert | 76.451 ms | 1.63x (1.61–1.68) | 1.63x (1.59–1.66) | 1.01x (1.00–1.03) | comparable |
| orm insert models | 241.882 ms | 1.69x (1.68–1.71) | 2.51x (2.43–2.53) | 1.10x (1.02–1.13) | comparable |
| orm update filtered | 5.365 ms | 1.13x (1.07–1.16) | 1.14x (1.10–1.20) | 1.06x (0.99–1.13) | comparable |
| orm upsert mixed | 29.029 ms | 33.46x (32.43–34.27) | 34.54x (33.42–37.19) | 0.99x (0.98–1.04) | comparable |
| orm delete filtered | 6.039 ms | 1.26x (1.23–1.28) | 1.22x (1.21–1.25) | 0.98x (0.96–1.01) | comparable |
| count all rows | 0.434 ms | 3.50x (3.26–3.63) | 3.31x (3.24–3.49) | 0.95x (0.94–1.09) | comparable |
| count equality filter | 0.478 ms | 3.70x (3.38–3.93) | 3.58x (3.38–3.85) | 0.98x (0.94–1.06) | comparable |
| count range filter | 0.523 ms | 3.33x (3.10–3.89) | 3.46x (3.11–3.77) | 1.05x (0.99–1.09) | comparable |
| aggregate filtered rows | 0.927 ms | 2.31x (2.16–2.47) | 2.35x (2.20–2.43) | 1.03x (0.98–1.13) | comparable |
| scalar projection read | 1.628 ms | 2.08x (1.90–2.17) | 2.06x (1.88–2.20) | 1.00x (0.91–1.16) | comparable |
| batched primary-key lookup | 119.629 ms | 2.20x (2.00–2.40) | 2.21x (2.00–2.48) | 0.99x (0.94–1.01) | comparable |
| paginated find_many | 2.818 ms | 1.62x (1.60–1.68) | 1.87x (1.82–1.92) | 0.99x (0.96–1.00) | comparable |
| ordered find_many | 3.729 ms | 1.50x (1.43–7.00) | 1.68x (1.63–1.73) | 1.00x (0.97–1.04) | comparable |
| hydrate flat rows | 2.856 ms | 1.61x (1.45–1.70) | 1.87x (1.72–1.93) | 0.98x (0.90–1.07) | comparable |
| serialize simple payloads | 0.749 ms | 0.89x (0.85–0.92) | 2.09x (2.01–2.17) | 0.99x (0.95–1.13) | diagnostic |
| serialize nested payloads | 0.377 ms | 1.09x (0.87–1.45) | 1.15x (1.06–1.24) | 1.06x (0.92–1.13) | diagnostic |
| hydrate relationship results | 7.607 ms | 0.97x (0.91–1.02) | 1.04x (0.96–1.11) | 1.01x (0.98–1.10) | comparable |
| one-to-many relationship loading | 2.824 ms | 1.62x (1.54–1.76) | 1.77x (1.65–1.92) | 1.01x (0.95–1.02) | comparable |
| many-to-one relationship loading | 2.954 ms | 1.47x (1.39–1.62) | 1.61x (1.57–1.70) | 1.00x (0.98–1.03) | comparable |
| nested relationship loading | 6.265 ms | 1.52x (1.41–1.57) | 1.60x (1.51–1.63) | 0.99x (0.95–1.06) | comparable |
