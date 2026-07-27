<!-- ormdantic-benchmark-report -->
## Ormdantic benchmark report

**sqlite / ci:** Ormdantic is 2.16x vs SQLAlchemy and 2.14x vs SQLModel (geometric mean of comparable cases).

| Case | Ormdantic | vs SQLAlchemy | vs SQLModel | Base/head | Scope |
| --- | ---: | ---: | ---: | ---: | --- |
| schema create/drop | 120.076 ms | 0.97x (0.33–5.91) | 1.30x (0.15–13.61) | 0.11x (0.04–1.28) | comparable |
| raw batch insert | 121.806 ms | 7.89x (2.61–13.25) | 4.21x (1.45–7.20) | 1.04x (0.36–1.71) | comparable |
| orm insert models | 1342.542 ms | 1.14x (0.60–2.43) | 1.38x (1.04–2.53) | 0.58x (0.19–0.91) | comparable |
| orm update filtered | 5.990 ms | 1.45x (0.74–3.05) | 0.96x (0.71–18.53) | 1.32x (0.91–22.55) | comparable |
| orm upsert mixed | 63.035 ms | 16.62x (6.81–30.56) | 16.93x (7.21–30.20) | 0.53x (0.23–1.80) | comparable |
| orm delete filtered | 7.453 ms | 1.34x (0.46–2.90) | 0.88x (0.41–11.36) | 1.01x (0.35–1.68) | comparable |
| count all rows | 0.480 ms | 3.10x (2.79–3.41) | 3.02x (2.72–3.31) | 0.96x (0.81–1.19) | comparable |
| count equality filter | 0.418 ms | 3.83x (3.64–4.44) | 4.05x (3.78–4.38) | 1.25x (1.17–1.39) | comparable |
| count range filter | 0.506 ms | 3.20x (2.95–3.62) | 3.16x (3.01–3.54) | 1.08x (0.98–1.36) | comparable |
| aggregate filtered rows | 0.863 ms | 2.36x (2.06–2.60) | 2.31x (2.03–2.40) | 1.08x (0.91–1.12) | comparable |
| scalar projection read | 1.674 ms | 2.04x (1.94–2.11) | 2.05x (1.95–2.14) | 0.95x (0.90–1.02) | comparable |
| batched primary-key lookup | 107.109 ms | 2.30x (2.16–2.38) | 2.25x (2.17–2.48) | 1.01x (0.99–1.02) | comparable |
| paginated find_many | 2.694 ms | 1.59x (1.53–1.63) | 1.87x (1.78–2.03) | 0.99x (0.95–1.05) | comparable |
| ordered find_many | 3.574 ms | 1.50x (1.44–7.45) | 1.62x (1.57–1.71) | 1.02x (0.99–1.11) | comparable |
| hydrate flat rows | 2.646 ms | 1.62x (1.48–1.70) | 1.92x (1.73–2.04) | 1.00x (0.91–1.15) | comparable |
| serialize simple payloads | 0.618 ms | 0.86x (0.85–0.89) | 2.00x (1.97–2.11) | 0.99x (0.97–1.02) | diagnostic |
| serialize nested payloads | 0.315 ms | 1.09x (0.91–1.11) | 1.15x (1.07–1.20) | 0.97x (0.90–1.08) | diagnostic |
| hydrate relationship results | 6.686 ms | 1.05x (0.96–1.08) | 1.12x (1.05–1.17) | 0.97x (0.91–1.09) | comparable |
| one-to-many relationship loading | 2.583 ms | 1.80x (1.63–1.86) | 1.93x (1.69–1.97) | 1.03x (0.97–1.07) | comparable |
| many-to-one relationship loading | 2.709 ms | 1.66x (1.51–1.69) | 1.85x (1.69–1.92) | 1.03x (0.97–1.08) | comparable |
| nested relationship loading | 5.738 ms | 1.56x (1.48–1.64) | 1.74x (1.58–1.80) | 1.03x (0.98–1.06) | comparable |
