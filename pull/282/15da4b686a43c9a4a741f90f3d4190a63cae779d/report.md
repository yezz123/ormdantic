<!-- ormdantic-benchmark-report -->
## Ormdantic benchmark report

**sqlite / ci:** Ormdantic is 2.10x vs SQLAlchemy and 2.22x vs SQLModel (geometric mean of comparable cases).

| Case | Ormdantic | vs SQLAlchemy | vs SQLModel | Base/head | Scope |
| --- | ---: | ---: | ---: | ---: | --- |
| schema create/drop | 6.499 ms | 2.57x (2.10–5.97) | 2.51x (2.10–3.24) | 1.01x (0.84–4.66) | comparable |
| raw batch insert | 58.791 ms | 1.95x (1.59–2.35) | 1.81x (1.52–2.16) | 1.01x (0.85–2.18) | comparable |
| orm insert models | 310.594 ms | 1.56x (0.70–2.41) | 2.12x (1.25–3.63) | 0.84x (0.50–1.43) | comparable |
| orm update filtered | 4.688 ms | 1.09x (1.02–1.18) | 1.19x (1.09–2.11) | 2.40x (0.98–2.58) | comparable |
| orm upsert mixed | 23.376 ms | 32.35x (24.12–33.23) | 33.32x (24.82–35.17) | 0.99x (0.74–2.04) | comparable |
| orm delete filtered | 7.682 ms | 0.83x (0.64–1.17) | 0.81x (0.63–1.16) | 0.85x (0.66–1.74) | comparable |
| count all rows | 0.329 ms | 3.45x (3.05–3.71) | 3.57x (3.07–3.71) | 0.97x (0.85–1.09) | comparable |
| count equality filter | 0.359 ms | 3.93x (3.62–4.12) | 3.80x (3.45–4.13) | 1.00x (0.92–1.05) | comparable |
| count range filter | 0.435 ms | 3.13x (2.63–3.30) | 3.27x (2.70–3.43) | 0.94x (0.79–1.11) | comparable |
| aggregate filtered rows | 0.759 ms | 2.28x (2.22–2.47) | 2.29x (2.20–2.39) | 0.98x (0.95–1.03) | comparable |
| scalar projection read | 1.276 ms | 2.12x (1.97–2.23) | 2.21x (2.06–2.30) | 0.98x (0.89–1.02) | comparable |
| batched primary-key lookup | 89.179 ms | 2.32x (2.05–2.42) | 2.11x (2.06–2.47) | 1.02x (0.97–1.06) | comparable |
| paginated find_many | 2.325 ms | 1.57x (1.52–1.69) | 1.82x (1.77–1.93) | 0.93x (0.91–1.03) | comparable |
| ordered find_many | 2.990 ms | 1.52x (1.35–8.46) | 1.69x (1.55–1.73) | 0.96x (0.88–1.00) | comparable |
| hydrate flat rows | 2.327 ms | 1.53x (1.48–1.77) | 1.77x (1.70–1.87) | 0.93x (0.90–1.01) | comparable |
| serialize simple payloads | 0.571 ms | 0.89x (0.87–0.91) | 1.99x (1.95–2.03) | 1.01x (0.97–1.03) | diagnostic |
| serialize nested payloads | 0.279 ms | 0.97x (0.83–1.06) | 1.26x (1.04–1.36) | 1.02x (0.85–1.11) | diagnostic |
| hydrate relationship results | 5.835 ms | 1.00x (0.93–1.05) | 1.07x (0.99–1.15) | 1.00x (0.92–1.06) | comparable |
| one-to-many relationship loading | 2.273 ms | 1.63x (1.49–1.79) | 1.73x (1.71–1.85) | 0.99x (0.97–1.02) | comparable |
| many-to-one relationship loading | 2.359 ms | 1.43x (1.35–1.63) | 1.64x (1.46–1.79) | 0.99x (0.92–1.02) | comparable |
| nested relationship loading | 4.976 ms | 1.48x (1.36–1.53) | 1.58x (1.44–1.62) | 0.99x (0.93–1.07) | comparable |
