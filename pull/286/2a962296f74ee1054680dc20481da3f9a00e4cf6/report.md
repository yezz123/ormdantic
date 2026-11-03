<!-- ormdantic-benchmark-report -->
## Ormdantic benchmark report

**sqlite / ci:** Ormdantic is 2.16x vs SQLAlchemy and 2.30x vs SQLModel (geometric mean of comparable cases).

| Case | Ormdantic | vs SQLAlchemy | vs SQLModel | Base/head | Scope |
| --- | ---: | ---: | ---: | ---: | --- |
| schema create/drop | 5.019 ms | 2.69x (2.55–3.24) | 2.56x (2.50–2.95) | 0.94x (0.92–1.03) | comparable |
| raw batch insert | 58.457 ms | 1.88x (1.68–2.07) | 1.92x (1.66–2.23) | 0.99x (0.97–1.03) | comparable |
| orm insert models | 190.515 ms | 1.73x (1.59–3.05) | 2.64x (2.23–2.81) | 1.23x (1.01–1.78) | comparable |
| orm update filtered | 4.315 ms | 1.14x (1.12–1.17) | 1.15x (1.13–1.16) | 1.05x (1.00–1.38) | comparable |
| orm upsert mixed | 22.373 ms | 31.79x (30.48–32.57) | 32.97x (32.15–34.83) | 1.06x (1.01–1.09) | comparable |
| orm delete filtered | 5.853 ms | 1.07x (0.80–1.28) | 1.09x (0.80–1.36) | 0.92x (0.67–1.41) | comparable |
| count all rows | 0.354 ms | 3.32x (2.99–3.62) | 3.26x (2.98–3.42) | 1.04x (0.95–1.10) | comparable |
| count equality filter | 0.360 ms | 4.02x (3.41–4.12) | 3.94x (3.42–4.27) | 1.09x (0.95–1.14) | comparable |
| count range filter | 0.426 ms | 3.25x (2.85–3.61) | 3.40x (2.87–3.64) | 1.00x (0.86–1.10) | comparable |
| aggregate filtered rows | 0.786 ms | 2.39x (2.21–2.43) | 2.35x (2.22–2.44) | 0.97x (0.93–1.04) | comparable |
| scalar projection read | 1.333 ms | 2.10x (1.94–2.18) | 2.11x (1.95–2.19) | 0.96x (0.86–1.06) | comparable |
| batched primary-key lookup | 95.603 ms | 2.17x (2.03–2.26) | 2.20x (1.99–2.30) | 0.94x (0.90–0.97) | comparable |
| paginated find_many | 2.229 ms | 1.63x (1.56–1.67) | 1.86x (1.79–2.11) | 0.98x (0.94–1.04) | comparable |
| ordered find_many | 2.975 ms | 1.51x (1.41–7.99) | 1.64x (1.55–1.70) | 0.98x (0.92–1.00) | comparable |
| hydrate flat rows | 2.174 ms | 1.70x (1.55–1.72) | 1.92x (1.78–1.97) | 0.98x (0.91–1.01) | comparable |
| serialize simple payloads | 0.554 ms | 0.89x (0.84–0.92) | 2.03x (1.93–2.14) | 0.98x (0.93–1.01) | diagnostic |
| serialize nested payloads | 0.296 ms | 1.06x (0.83–1.16) | 1.07x (1.01–1.25) | 0.91x (0.85–1.07) | diagnostic |
| hydrate relationship results | 5.949 ms | 0.98x (0.94–1.00) | 1.05x (0.99–1.09) | 1.00x (0.96–1.05) | comparable |
| one-to-many relationship loading | 2.309 ms | 1.61x (1.54–1.81) | 1.74x (1.63–1.83) | 0.98x (0.95–1.04) | comparable |
| many-to-one relationship loading | 2.380 ms | 1.54x (1.42–1.58) | 1.69x (1.53–1.82) | 1.00x (0.99–1.02) | comparable |
| nested relationship loading | 5.122 ms | 1.45x (1.32–1.60) | 1.61x (1.46–1.78) | 0.99x (0.93–1.05) | comparable |
