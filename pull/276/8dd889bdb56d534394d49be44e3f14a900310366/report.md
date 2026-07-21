<!-- ormdantic-benchmark-report -->
## Ormdantic benchmark report

**sqlite / ci:** Ormdantic is 2.17x vs SQLAlchemy and 2.29x vs SQLModel (geometric mean of comparable cases).

| Case | Ormdantic | vs SQLAlchemy | vs SQLModel | Base/head | Scope |
| --- | ---: | ---: | ---: | ---: | --- |
| schema create/drop | 5.373 ms | 2.61x (2.55–2.79) | 2.70x (2.63–2.89) | 0.95x (0.93–1.02) | comparable |
| raw batch insert | 73.914 ms | 1.66x (1.64–1.68) | 1.65x (1.62–1.66) | 1.03x (1.01–1.04) | comparable |
| orm insert models | 240.291 ms | 1.65x (1.62–1.76) | 2.46x (2.36–2.54) | 1.09x (1.01–1.10) | comparable |
| orm update filtered | 5.220 ms | 1.12x (1.08–1.14) | 1.12x (1.08–1.15) | 1.01x (0.97–1.02) | comparable |
| orm upsert mixed | 28.302 ms | 34.15x (32.17–35.17) | 35.41x (33.71–36.62) | 1.01x (0.98–1.03) | comparable |
| orm delete filtered | 5.867 ms | 1.23x (1.20–1.28) | 1.24x (1.20–1.29) | 0.99x (0.97–1.07) | comparable |
| count all rows | 0.410 ms | 3.56x (3.29–4.04) | 3.49x (3.20–3.90) | 1.00x (0.90–1.14) | comparable |
| count equality filter | 0.417 ms | 3.93x (3.78–4.12) | 3.90x (3.73–3.98) | 1.08x (1.05–1.11) | comparable |
| count range filter | 0.511 ms | 3.26x (3.03–3.57) | 3.20x (3.04–3.39) | 0.95x (0.90–1.04) | comparable |
| aggregate filtered rows | 0.911 ms | 2.33x (2.20–2.43) | 2.32x (2.19–2.42) | 1.03x (0.96–1.08) | comparable |
| scalar projection read | 1.544 ms | 2.17x (2.10–2.21) | 2.12x (2.10–2.21) | 1.03x (0.98–1.11) | comparable |
| batched primary-key lookup | 120.315 ms | 2.17x (1.98–2.46) | 2.19x (2.04–2.52) | 0.96x (0.95–1.09) | comparable |
| paginated find_many | 2.808 ms | 1.67x (1.54–1.70) | 1.89x (1.75–1.94) | 0.99x (0.92–1.01) | comparable |
| ordered find_many | 3.785 ms | 1.51x (1.40–7.25) | 1.64x (1.57–1.70) | 1.01x (0.97–1.05) | comparable |
| hydrate flat rows | 2.814 ms | 1.59x (1.53–2.01) | 1.85x (1.77–1.89) | 0.99x (0.96–1.05) | comparable |
| serialize simple payloads | 0.750 ms | 0.90x (0.87–0.91) | 2.05x (2.03–2.10) | 0.96x (0.95–1.00) | diagnostic |
| serialize nested payloads | 0.376 ms | 0.97x (0.89–1.05) | 1.17x (1.09–1.28) | 1.02x (0.91–1.13) | diagnostic |
| hydrate relationship results | 7.594 ms | 0.99x (0.94–1.06) | 1.06x (0.98–1.10) | 0.99x (0.94–1.07) | comparable |
| one-to-many relationship loading | 2.794 ms | 1.65x (1.50–1.77) | 1.72x (1.65–1.88) | 1.01x (0.98–1.04) | comparable |
| many-to-one relationship loading | 2.977 ms | 1.50x (1.41–1.59) | 1.63x (1.55–1.71) | 0.98x (0.93–1.00) | comparable |
| nested relationship loading | 6.500 ms | 1.46x (1.36–1.62) | 1.53x (1.42–1.69) | 0.94x (0.88–1.02) | comparable |
