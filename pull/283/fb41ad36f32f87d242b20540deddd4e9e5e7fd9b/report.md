<!-- ormdantic-benchmark-report -->
## Ormdantic benchmark report

**sqlite / ci:** Ormdantic is 2.19x vs SQLAlchemy and 2.34x vs SQLModel (geometric mean of comparable cases).

| Case | Ormdantic | vs SQLAlchemy | vs SQLModel | Base/head | Scope |
| --- | ---: | ---: | ---: | ---: | --- |
| schema create/drop | 6.120 ms | 2.84x (2.70–3.01) | 2.89x (2.74–3.09) | 1.02x (0.94–1.16) | comparable |
| raw batch insert | 80.963 ms | 1.62x (1.59–1.68) | 1.59x (1.57–1.62) | 0.98x (0.96–1.00) | comparable |
| orm insert models | 255.868 ms | 1.65x (1.58–1.68) | 2.67x (2.57–2.74) | 1.00x (0.96–1.04) | comparable |
| orm update filtered | 5.487 ms | 1.14x (1.10–1.18) | 1.12x (1.10–1.18) | 0.99x (0.97–1.01) | comparable |
| orm upsert mixed | 29.752 ms | 38.09x (35.01–38.95) | 37.54x (34.94–39.39) | 1.01x (0.95–1.03) | comparable |
| orm delete filtered | 5.932 ms | 1.26x (1.21–1.30) | 1.27x (1.23–1.32) | 1.04x (0.97–1.14) | comparable |
| count all rows | 0.428 ms | 3.50x (3.36–3.82) | 3.46x (3.34–3.84) | 1.04x (0.96–1.14) | comparable |
| count equality filter | 0.440 ms | 4.09x (3.85–4.40) | 3.93x (3.68–4.11) | 1.02x (0.97–1.04) | comparable |
| count range filter | 0.542 ms | 3.37x (3.32–3.60) | 3.29x (3.19–3.59) | 0.99x (0.95–1.07) | comparable |
| aggregate filtered rows | 0.965 ms | 2.34x (2.25–2.57) | 2.30x (2.23–2.47) | 0.92x (0.86–0.96) | comparable |
| scalar projection read | 1.610 ms | 2.13x (2.07–2.22) | 2.15x (2.07–2.35) | 0.96x (0.91–1.04) | comparable |
| batched primary-key lookup | 141.590 ms | 2.06x (2.04–2.22) | 2.08x (2.06–2.21) | 0.99x (0.97–1.00) | comparable |
| paginated find_many | 2.927 ms | 1.63x (1.55–1.69) | 1.93x (1.80–1.97) | 0.99x (0.93–1.04) | comparable |
| ordered find_many | 3.954 ms | 1.47x (1.40–7.75) | 1.67x (1.55–1.80) | 0.91x (0.86–0.99) | comparable |
| hydrate flat rows | 2.835 ms | 1.61x (1.57–1.74) | 1.96x (1.85–2.01) | 0.96x (0.95–1.00) | comparable |
| serialize simple payloads | 0.775 ms | 0.89x (0.87–0.91) | 1.91x (1.89–1.99) | 0.95x (0.93–1.13) | diagnostic |
| serialize nested payloads | 0.401 ms | 0.96x (0.83–1.01) | 1.17x (1.01–1.25) | 0.93x (0.83–1.06) | diagnostic |
| hydrate relationship results | 7.601 ms | 1.05x (0.98–1.10) | 1.16x (1.10–1.19) | 0.99x (0.94–1.07) | comparable |
| one-to-many relationship loading | 2.942 ms | 1.70x (1.61–1.75) | 1.77x (1.69–1.95) | 0.95x (0.93–0.98) | comparable |
| many-to-one relationship loading | 3.002 ms | 1.47x (1.43–1.56) | 1.64x (1.52–1.79) | 1.00x (0.96–1.02) | comparable |
| nested relationship loading | 6.573 ms | 1.47x (1.43–1.55) | 1.64x (1.54–1.70) | 0.99x (0.93–1.05) | comparable |
