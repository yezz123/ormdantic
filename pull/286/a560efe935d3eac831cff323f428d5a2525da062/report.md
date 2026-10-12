<!-- ormdantic-benchmark-report -->
## Ormdantic benchmark report

**sqlite / ci:** Ormdantic is 2.22x vs SQLAlchemy and 2.36x vs SQLModel (geometric mean of comparable cases).

| Case | Ormdantic | vs SQLAlchemy | vs SQLModel | Base/head | Scope |
| --- | ---: | ---: | ---: | ---: | --- |
| schema create/drop | 5.756 ms | 2.98x (2.85–3.33) | 2.95x (2.83–3.11) | 1.04x (1.00–1.12) | comparable |
| raw batch insert | 80.421 ms | 1.66x (1.63–1.70) | 1.65x (1.61–1.70) | 1.02x (1.00–1.06) | comparable |
| orm insert models | 251.435 ms | 1.66x (1.63–1.72) | 2.64x (2.61–2.69) | 1.01x (0.99–1.02) | comparable |
| orm update filtered | 5.375 ms | 1.13x (1.09–1.19) | 1.12x (1.09–1.16) | 1.01x (0.99–1.05) | comparable |
| orm upsert mixed | 29.907 ms | 37.63x (36.02–39.06) | 39.16x (37.96–40.10) | 0.98x (0.96–1.00) | comparable |
| orm delete filtered | 5.952 ms | 1.30x (1.25–1.37) | 1.27x (1.20–1.36) | 0.98x (0.92–1.05) | comparable |
| count all rows | 0.389 ms | 3.81x (3.45–3.99) | 3.79x (3.52–3.89) | 0.99x (0.90–1.20) | comparable |
| count equality filter | 0.424 ms | 4.24x (3.93–4.63) | 3.99x (3.87–4.37) | 0.97x (0.92–1.07) | comparable |
| count range filter | 0.546 ms | 3.22x (3.00–3.57) | 3.24x (2.98–3.50) | 0.88x (0.82–0.99) | comparable |
| aggregate filtered rows | 0.870 ms | 2.41x (2.31–2.50) | 2.41x (2.35–2.50) | 0.94x (0.92–1.05) | comparable |
| scalar projection read | 1.561 ms | 2.15x (2.00–2.23) | 2.14x (2.02–2.23) | 0.98x (0.89–1.01) | comparable |
| batched primary-key lookup | 139.428 ms | 2.11x (2.07–2.33) | 2.40x (2.15–2.43) | 0.98x (0.97–0.99) | comparable |
| paginated find_many | 2.806 ms | 1.62x (1.54–1.85) | 1.84x (1.76–1.95) | 0.97x (0.93–1.04) | comparable |
| ordered find_many | 3.767 ms | 1.51x (1.41–6.81) | 1.66x (1.64–1.74) | 0.96x (0.96–1.01) | comparable |
| hydrate flat rows | 2.868 ms | 1.65x (1.56–1.72) | 1.88x (1.84–1.95) | 0.96x (0.94–1.01) | comparable |
| serialize simple payloads | 0.714 ms | 0.97x (0.92–0.99) | 2.08x (1.93–2.10) | 0.97x (0.91–0.98) | diagnostic |
| serialize nested payloads | 0.370 ms | 1.03x (0.96–1.11) | 1.20x (1.12–1.35) | 0.90x (0.85–1.03) | diagnostic |
| hydrate relationship results | 7.475 ms | 1.02x (0.97–1.08) | 1.12x (1.08–1.14) | 1.01x (0.96–1.04) | comparable |
| one-to-many relationship loading | 2.865 ms | 1.70x (1.57–1.83) | 1.74x (1.65–1.83) | 0.97x (0.96–1.00) | comparable |
| many-to-one relationship loading | 2.970 ms | 1.46x (1.40–1.51) | 1.58x (1.53–1.69) | 0.99x (0.94–1.03) | comparable |
| nested relationship loading | 6.443 ms | 1.46x (1.35–1.52) | 1.55x (1.42–1.64) | 0.98x (0.89–1.02) | comparable |
