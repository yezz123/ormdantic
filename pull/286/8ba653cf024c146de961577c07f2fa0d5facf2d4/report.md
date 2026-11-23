<!-- ormdantic-benchmark-report -->
## Ormdantic benchmark report

**sqlite / ci:** Ormdantic is 2.22x vs SQLAlchemy and 2.36x vs SQLModel (geometric mean of comparable cases).

| Case | Ormdantic | vs SQLAlchemy | vs SQLModel | Base/head | Scope |
| --- | ---: | ---: | ---: | ---: | --- |
| schema create/drop | 5.493 ms | 3.01x (2.89–3.29) | 3.09x (2.99–3.21) | 0.98x (0.97–1.14) | comparable |
| raw batch insert | 78.701 ms | 1.66x (1.60–1.74) | 1.62x (1.57–1.68) | 1.04x (1.00–1.05) | comparable |
| orm insert models | 253.173 ms | 1.65x (1.62–1.71) | 2.57x (2.53–2.66) | 1.03x (0.99–1.08) | comparable |
| orm update filtered | 5.539 ms | 1.14x (1.12–1.20) | 1.12x (1.10–1.19) | 1.03x (1.00–1.11) | comparable |
| orm upsert mixed | 29.609 ms | 35.24x (33.61–37.14) | 37.33x (35.50–38.70) | 1.10x (1.05–1.18) | comparable |
| orm delete filtered | 5.984 ms | 1.28x (1.16–1.35) | 1.28x (1.17–1.37) | 1.10x (0.98–1.16) | comparable |
| count all rows | 0.378 ms | 4.00x (3.38–4.56) | 4.00x (3.44–4.69) | 1.31x (1.10–1.52) | comparable |
| count equality filter | 0.507 ms | 3.68x (3.34–4.16) | 3.63x (3.45–4.17) | 1.08x (1.04–1.37) | comparable |
| count range filter | 0.553 ms | 3.33x (2.97–3.57) | 3.52x (3.16–3.75) | 1.21x (1.03–1.37) | comparable |
| aggregate filtered rows | 0.967 ms | 2.36x (2.23–2.53) | 2.24x (2.13–2.46) | 1.03x (0.96–1.09) | comparable |
| scalar projection read | 1.689 ms | 2.08x (1.96–2.16) | 2.08x (1.96–2.19) | 1.12x (1.05–1.18) | comparable |
| batched primary-key lookup | 134.676 ms | 2.19x (2.10–2.31) | 2.14x (2.09–2.23) | 1.02x (1.00–1.03) | comparable |
| paginated find_many | 3.301 ms | 1.59x (1.34–1.80) | 1.77x (1.59–1.99) | 0.86x (0.79–0.97) | comparable |
| ordered find_many | 4.303 ms | 1.51x (1.43–9.62) | 1.66x (1.56–1.75) | 0.92x (0.88–0.96) | comparable |
| hydrate flat rows | 3.443 ms | 1.52x (1.44–1.69) | 1.75x (1.68–1.89) | 0.85x (0.82–0.92) | comparable |
| serialize simple payloads | 1.010 ms | 0.79x (0.73–0.88) | 1.83x (1.67–2.01) | 0.75x (0.70–0.87) | diagnostic |
| serialize nested payloads | 0.447 ms | 1.05x (0.83–1.19) | 1.41x (1.11–1.70) | 0.86x (0.66–1.01) | diagnostic |
| hydrate relationship results | 7.938 ms | 1.10x (1.03–1.20) | 1.22x (1.15–1.25) | 1.00x (0.95–1.04) | comparable |
| one-to-many relationship loading | 3.137 ms | 1.75x (1.58–1.84) | 1.93x (1.88–2.09) | 0.98x (0.96–1.05) | comparable |
| many-to-one relationship loading | 3.286 ms | 1.54x (1.49–1.73) | 1.72x (1.51–1.92) | 0.93x (0.91–1.00) | comparable |
| nested relationship loading | 6.907 ms | 1.62x (1.46–1.68) | 1.70x (1.53–1.82) | 0.99x (0.87–1.22) | comparable |
