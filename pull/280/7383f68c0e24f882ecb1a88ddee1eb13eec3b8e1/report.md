<!-- ormdantic-benchmark-report -->
## Ormdantic benchmark report

**sqlite / ci:** Ormdantic is 2.23x vs SQLAlchemy and 2.37x vs SQLModel (geometric mean of comparable cases).

| Case | Ormdantic | vs SQLAlchemy | vs SQLModel | Base/head | Scope |
| --- | ---: | ---: | ---: | ---: | --- |
| schema create/drop | 6.698 ms | 2.80x (1.76–3.05) | 2.73x (1.71–2.89) | 0.90x (0.57–0.96) | comparable |
| raw batch insert | 81.407 ms | 1.69x (1.66–1.73) | 1.65x (1.64–1.71) | 1.00x (0.99–1.06) | comparable |
| orm insert models | 270.895 ms | 1.65x (1.59–1.72) | 2.58x (2.51–2.70) | 1.00x (0.95–1.03) | comparable |
| orm update filtered | 5.866 ms | 1.09x (0.84–1.12) | 1.11x (0.84–1.16) | 0.98x (0.76–1.02) | comparable |
| orm upsert mixed | 30.916 ms | 36.71x (35.27–39.08) | 38.22x (37.35–39.64) | 0.99x (0.94–1.04) | comparable |
| orm delete filtered | 5.962 ms | 1.26x (1.21–1.41) | 1.30x (1.25–1.36) | 1.03x (0.97–1.06) | comparable |
| count all rows | 0.425 ms | 3.56x (3.17–4.02) | 3.56x (3.25–3.89) | 0.98x (0.89–1.13) | comparable |
| count equality filter | 0.455 ms | 3.96x (3.64–4.17) | 3.95x (3.43–4.26) | 1.02x (0.92–1.07) | comparable |
| count range filter | 0.518 ms | 3.56x (3.30–3.77) | 3.55x (3.28–3.75) | 1.00x (0.93–1.10) | comparable |
| aggregate filtered rows | 0.864 ms | 2.52x (2.32–2.63) | 2.52x (2.31–2.66) | 1.05x (0.96–1.09) | comparable |
| scalar projection read | 1.631 ms | 2.13x (1.97–2.23) | 2.08x (1.98–2.33) | 1.00x (0.94–1.03) | comparable |
| batched primary-key lookup | 143.963 ms | 2.35x (2.22–2.47) | 2.18x (2.08–2.34) | 1.00x (0.98–1.04) | comparable |
| paginated find_many | 2.808 ms | 1.66x (1.49–1.72) | 1.93x (1.73–2.00) | 1.05x (0.94–1.12) | comparable |
| ordered find_many | 3.852 ms | 1.49x (1.40–6.64) | 1.66x (1.61–1.74) | 1.03x (0.97–1.06) | comparable |
| hydrate flat rows | 2.833 ms | 1.62x (1.53–1.79) | 1.92x (1.82–1.98) | 1.04x (0.97–1.12) | comparable |
| serialize simple payloads | 0.743 ms | 0.90x (0.87–0.95) | 2.01x (1.93–2.07) | 1.04x (1.00–1.08) | diagnostic |
| serialize nested payloads | 0.373 ms | 1.05x (0.93–1.17) | 1.24x (1.09–1.41) | 0.99x (0.85–1.17) | diagnostic |
| hydrate relationship results | 7.529 ms | 1.06x (0.98–1.08) | 1.16x (1.07–1.20) | 1.01x (0.94–1.07) | comparable |
| one-to-many relationship loading | 2.898 ms | 1.74x (1.66–1.88) | 1.87x (1.77–1.97) | 1.00x (0.97–1.03) | comparable |
| many-to-one relationship loading | 3.008 ms | 1.52x (1.43–1.63) | 1.65x (1.54–1.77) | 1.03x (0.98–1.08) | comparable |
| nested relationship loading | 6.489 ms | 1.51x (1.40–1.62) | 1.67x (1.56–1.70) | 1.01x (0.94–1.11) | comparable |
