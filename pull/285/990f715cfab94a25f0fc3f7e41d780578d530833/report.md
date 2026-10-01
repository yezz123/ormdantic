<!-- ormdantic-benchmark-report -->
## Ormdantic benchmark report

**sqlite / ci:** Ormdantic is 2.18x vs SQLAlchemy and 2.35x vs SQLModel (geometric mean of comparable cases).

| Case | Ormdantic | vs SQLAlchemy | vs SQLModel | Base/head | Scope |
| --- | ---: | ---: | ---: | ---: | --- |
| schema create/drop | 5.620 ms | 2.96x (2.70–3.09) | 2.91x (2.63–3.16) | 1.35x (1.06–1.56) | comparable |
| raw batch insert | 79.295 ms | 1.65x (1.50–1.69) | 1.67x (1.52–1.72) | 1.03x (0.94–1.06) | comparable |
| orm insert models | 253.296 ms | 1.65x (1.59–1.70) | 2.66x (2.49–2.74) | 1.08x (1.02–1.13) | comparable |
| orm update filtered | 5.394 ms | 1.15x (1.08–1.19) | 1.16x (1.10–1.21) | 1.01x (0.95–1.07) | comparable |
| orm upsert mixed | 29.986 ms | 35.11x (32.88–37.95) | 36.75x (34.56–38.83) | 1.02x (0.96–1.08) | comparable |
| orm delete filtered | 5.667 ms | 1.29x (1.26–1.34) | 1.30x (1.22–1.41) | 1.06x (1.03–1.13) | comparable |
| count all rows | 0.419 ms | 3.41x (3.12–3.82) | 3.55x (3.03–4.06) | 1.07x (0.93–1.27) | comparable |
| count equality filter | 0.446 ms | 4.06x (3.43–4.42) | 4.11x (3.33–4.47) | 1.03x (0.85–1.10) | comparable |
| count range filter | 0.523 ms | 3.34x (3.16–3.72) | 3.36x (3.17–3.76) | 1.07x (1.00–1.16) | comparable |
| aggregate filtered rows | 0.869 ms | 2.43x (2.28–2.63) | 2.45x (2.23–2.58) | 1.10x (1.02–1.15) | comparable |
| scalar projection read | 1.579 ms | 2.13x (2.08–2.26) | 2.22x (2.07–2.29) | 1.11x (1.01–1.20) | comparable |
| batched primary-key lookup | 138.216 ms | 2.09x (2.03–2.21) | 2.10x (2.06–2.28) | 1.02x (0.99–1.03) | comparable |
| paginated find_many | 2.823 ms | 1.62x (1.54–1.70) | 1.82x (1.73–1.95) | 1.04x (0.99–1.11) | comparable |
| ordered find_many | 3.697 ms | 1.49x (1.42–6.73) | 1.67x (1.59–1.77) | 1.05x (0.98–1.09) | comparable |
| hydrate flat rows | 2.774 ms | 1.61x (1.56–1.74) | 1.84x (1.82–1.93) | 1.00x (0.98–1.10) | comparable |
| serialize simple payloads | 0.724 ms | 0.94x (0.92–0.95) | 1.99x (1.96–2.00) | 1.04x (1.01–1.12) | diagnostic |
| serialize nested payloads | 0.356 ms | 1.04x (0.96–1.17) | 1.22x (1.14–1.33) | 1.05x (0.97–1.13) | diagnostic |
| hydrate relationship results | 7.726 ms | 0.99x (0.95–1.05) | 1.10x (1.04–1.21) | 0.99x (0.93–1.07) | comparable |
| one-to-many relationship loading | 2.837 ms | 1.64x (1.57–1.79) | 1.74x (1.70–1.88) | 1.01x (0.98–1.05) | comparable |
| many-to-one relationship loading | 2.912 ms | 1.45x (1.41–1.47) | 1.63x (1.56–1.68) | 1.03x (1.00–1.04) | comparable |
| nested relationship loading | 6.511 ms | 1.43x (1.35–1.49) | 1.57x (1.47–1.60) | 1.03x (0.93–1.05) | comparable |
