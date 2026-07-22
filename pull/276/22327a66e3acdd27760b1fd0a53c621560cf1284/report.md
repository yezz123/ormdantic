<!-- ormdantic-benchmark-report -->
## Ormdantic benchmark report

**sqlite / ci:** Ormdantic is 2.22x vs SQLAlchemy and 2.33x vs SQLModel (geometric mean of comparable cases).

| Case | Ormdantic | vs SQLAlchemy | vs SQLModel | Base/head | Scope |
| --- | ---: | ---: | ---: | ---: | --- |
| schema create/drop | 6.356 ms | 2.88x (1.96–3.11) | 2.80x (2.06–3.02) | 1.07x (0.76–1.14) | comparable |
| raw batch insert | 80.613 ms | 1.67x (1.57–1.98) | 1.65x (1.59–1.69) | 1.02x (0.98–1.04) | comparable |
| orm insert models | 254.557 ms | 1.71x (1.63–1.72) | 2.65x (2.60–2.67) | 1.05x (0.99–1.08) | comparable |
| orm update filtered | 6.379 ms | 0.98x (0.80–1.13) | 0.99x (0.80–1.15) | 0.89x (0.73–1.02) | comparable |
| orm upsert mixed | 29.938 ms | 37.21x (34.21–40.88) | 38.85x (36.46–39.92) | 1.02x (0.95–1.04) | comparable |
| orm delete filtered | 5.996 ms | 1.24x (1.14–1.28) | 1.22x (1.14–1.25) | 1.03x (0.92–1.06) | comparable |
| count all rows | 0.400 ms | 3.55x (3.03–3.88) | 3.82x (3.13–4.11) | 1.09x (0.93–1.28) | comparable |
| count equality filter | 0.410 ms | 4.13x (3.63–4.48) | 4.00x (3.54–4.20) | 1.42x (1.28–1.64) | comparable |
| count range filter | 0.508 ms | 3.65x (3.31–3.95) | 3.43x (3.31–3.75) | 1.41x (1.29–1.47) | comparable |
| aggregate filtered rows | 0.843 ms | 2.57x (2.31–2.68) | 2.41x (2.23–2.60) | 1.32x (1.15–1.37) | comparable |
| scalar projection read | 1.604 ms | 2.29x (2.03–2.88) | 2.12x (2.02–2.28) | 1.41x (1.27–1.79) | comparable |
| batched primary-key lookup | 141.946 ms | 2.12x (2.05–2.25) | 2.17x (2.05–2.33) | 1.06x (1.02–1.11) | comparable |
| paginated find_many | 2.718 ms | 1.69x (1.60–1.75) | 1.95x (1.89–1.97) | 1.27x (1.15–1.38) | comparable |
| ordered find_many | 3.726 ms | 1.56x (1.43–6.77) | 1.70x (1.65–1.75) | 1.33x (1.20–1.39) | comparable |
| hydrate flat rows | 2.873 ms | 1.65x (1.49–1.80) | 1.81x (1.74–1.92) | 1.27x (1.13–1.37) | comparable |
| serialize simple payloads | 0.743 ms | 0.93x (0.86–0.95) | 1.95x (1.81–1.96) | 1.22x (1.12–1.29) | diagnostic |
| serialize nested payloads | 0.352 ms | 1.05x (1.00–1.16) | 1.29x (1.16–1.34) | 1.19x (1.12–1.26) | diagnostic |
| hydrate relationship results | 7.768 ms | 1.03x (0.96–1.12) | 1.12x (1.07–1.18) | 1.03x (0.98–1.10) | comparable |
| one-to-many relationship loading | 2.938 ms | 1.64x (1.47–1.90) | 1.80x (1.60–1.89) | 1.00x (0.94–1.02) | comparable |
| many-to-one relationship loading | 2.971 ms | 1.49x (1.33–1.58) | 1.57x (1.40–1.75) | 1.00x (0.89–1.07) | comparable |
| nested relationship loading | 6.612 ms | 1.41x (1.34–1.56) | 1.54x (1.47–1.65) | 1.02x (0.99–1.12) | comparable |
