<!-- ormdantic-benchmark-report -->
## Ormdantic benchmark report

**sqlite / ci:** Ormdantic is 2.22x vs SQLAlchemy and 2.22x vs SQLModel (geometric mean of comparable cases).

| Case | Ormdantic | vs SQLAlchemy | vs SQLModel | Base/head | Scope |
| --- | ---: | ---: | ---: | ---: | --- |
| schema create/drop | 7.366 ms | 3.59x (1.59–5.49) | 2.70x (1.20–3.00) | 1.37x (0.49–19.88) | comparable |
| raw batch insert | 63.002 ms | 3.00x (1.48–4.17) | 1.87x (1.37–2.11) | 1.03x (0.73–8.98) | comparable |
| orm insert models | 218.371 ms | 1.49x (1.21–3.52) | 2.63x (1.78–3.09) | 0.92x (0.75–1.18) | comparable |
| orm update filtered | 6.801 ms | 1.02x (0.52–1.53) | 0.69x (0.48–0.90) | 0.58x (0.41–0.77) | comparable |
| orm upsert mixed | 26.363 ms | 31.28x (10.45–39.58) | 32.77x (10.72–39.55) | 0.83x (0.28–1.08) | comparable |
| orm delete filtered | 5.328 ms | 1.11x (0.85–1.29) | 1.00x (0.80–1.14) | 1.25x (0.94–1.63) | comparable |
| count all rows | 0.295 ms | 3.68x (3.48–3.81) | 3.46x (3.32–4.06) | 1.18x (1.15–1.25) | comparable |
| count equality filter | 0.364 ms | 3.50x (3.29–3.88) | 3.53x (3.26–4.03) | 0.97x (0.90–1.09) | comparable |
| count range filter | 0.416 ms | 3.12x (2.79–3.72) | 3.41x (3.06–3.70) | 0.99x (0.90–1.33) | comparable |
| aggregate filtered rows | 0.725 ms | 2.14x (1.97–2.31) | 2.13x (1.99–2.30) | 1.00x (0.95–1.06) | comparable |
| scalar projection read | 1.239 ms | 2.16x (2.06–2.29) | 2.16x (2.02–2.37) | 1.03x (0.99–1.08) | comparable |
| batched primary-key lookup | 94.135 ms | 2.16x (2.12–2.28) | 2.16x (2.09–2.30) | 1.02x (0.99–1.04) | comparable |
| paginated find_many | 2.092 ms | 1.68x (1.57–1.83) | 1.88x (1.76–1.91) | 1.01x (0.95–1.06) | comparable |
| ordered find_many | 2.917 ms | 1.47x (1.44–8.16) | 1.65x (1.63–1.76) | 0.98x (0.97–1.01) | comparable |
| hydrate flat rows | 2.151 ms | 1.65x (1.50–1.83) | 1.84x (1.74–2.00) | 1.01x (0.94–1.05) | comparable |
| serialize simple payloads | 0.508 ms | 0.85x (0.84–0.86) | 2.02x (1.99–2.03) | 0.98x (0.97–1.00) | diagnostic |
| serialize nested payloads | 0.260 ms | 0.95x (0.90–1.12) | 1.17x (1.11–1.30) | 0.99x (0.95–1.47) | diagnostic |
| hydrate relationship results | 5.635 ms | 1.01x (0.96–1.12) | 1.12x (1.03–1.16) | 1.00x (0.96–1.05) | comparable |
| one-to-many relationship loading | 2.073 ms | 1.78x (1.55–1.95) | 1.88x (1.74–1.95) | 1.08x (0.99–1.10) | comparable |
| many-to-one relationship loading | 2.330 ms | 1.54x (1.37–1.73) | 1.58x (1.42–1.96) | 1.01x (0.89–1.15) | comparable |
| nested relationship loading | 4.680 ms | 1.51x (1.44–1.62) | 1.68x (1.51–1.73) | 1.05x (0.99–1.12) | comparable |
