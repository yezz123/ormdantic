<!-- ormdantic-benchmark-report -->
## Ormdantic benchmark report

**sqlite / ci:** Ormdantic is 2.18x vs SQLAlchemy and 2.33x vs SQLModel (geometric mean of comparable cases).

| Case | Ormdantic | vs SQLAlchemy | vs SQLModel | Base/head | Scope |
| --- | ---: | ---: | ---: | ---: | --- |
| schema create/drop | 4.732 ms | 2.75x (2.55–2.87) | 2.74x (2.58–2.86) | 1.04x (0.98–1.08) | comparable |
| raw batch insert | 59.959 ms | 1.86x (1.40–2.09) | 2.00x (1.61–3.02) | 1.01x (0.86–1.05) | comparable |
| orm insert models | 209.418 ms | 1.59x (1.26–1.77) | 2.53x (1.82–2.95) | 1.04x (0.78–1.40) | comparable |
| orm update filtered | 5.953 ms | 1.10x (1.01–1.23) | 1.11x (1.06–1.25) | 1.01x (0.99–1.17) | comparable |
| orm upsert mixed | 24.425 ms | 28.51x (26.77–30.74) | 30.00x (28.58–32.73) | 1.09x (0.98–1.14) | comparable |
| orm delete filtered | 7.852 ms | 1.34x (1.09–1.38) | 1.28x (1.01–1.39) | 1.13x (0.86–1.38) | comparable |
| count all rows | 0.296 ms | 3.92x (3.38–4.19) | 4.18x (3.63–4.48) | 1.31x (1.13–1.34) | comparable |
| count equality filter | 0.350 ms | 3.97x (3.75–4.21) | 3.98x (3.78–4.37) | 1.12x (1.01–1.17) | comparable |
| count range filter | 0.401 ms | 3.49x (3.18–3.78) | 3.48x (3.14–3.59) | 1.13x (0.98–1.20) | comparable |
| aggregate filtered rows | 0.728 ms | 2.38x (2.25–2.43) | 2.43x (2.27–2.48) | 1.08x (1.01–1.20) | comparable |
| scalar projection read | 1.280 ms | 2.16x (2.09–2.26) | 2.13x (2.08–2.23) | 1.01x (0.98–1.09) | comparable |
| batched primary-key lookup | 88.244 ms | 2.27x (2.09–2.45) | 2.17x (2.11–2.35) | 0.99x (0.92–1.06) | comparable |
| paginated find_many | 2.272 ms | 1.60x (1.48–1.68) | 1.86x (1.73–2.00) | 1.06x (0.96–1.14) | comparable |
| ordered find_many | 2.974 ms | 1.55x (1.47–8.95) | 1.69x (1.63–1.73) | 1.04x (1.01–1.08) | comparable |
| hydrate flat rows | 2.220 ms | 1.61x (1.57–1.70) | 1.87x (1.83–1.93) | 1.06x (1.05–1.16) | comparable |
| serialize simple payloads | 0.564 ms | 0.88x (0.86–0.92) | 1.98x (1.95–2.06) | 1.01x (0.98–1.05) | diagnostic |
| serialize nested payloads | 0.280 ms | 0.96x (0.88–1.06) | 1.18x (1.08–1.29) | 1.09x (0.93–1.18) | diagnostic |
| hydrate relationship results | 6.063 ms | 0.97x (0.89–1.02) | 1.03x (0.97–1.06) | 1.04x (0.97–1.20) | comparable |
| one-to-many relationship loading | 2.289 ms | 1.59x (1.49–1.76) | 1.71x (1.61–1.84) | 1.02x (0.98–1.10) | comparable |
| many-to-one relationship loading | 2.372 ms | 1.44x (1.36–1.48) | 1.52x (1.50–1.67) | 1.00x (0.99–1.06) | comparable |
| nested relationship loading | 5.035 ms | 1.44x (1.37–1.51) | 1.62x (1.49–1.71) | 1.03x (0.93–1.06) | comparable |
