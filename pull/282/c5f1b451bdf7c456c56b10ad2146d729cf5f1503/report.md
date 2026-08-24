<!-- ormdantic-benchmark-report -->
## Ormdantic benchmark report

**sqlite / ci:** Ormdantic is 2.21x vs SQLAlchemy and 2.36x vs SQLModel (geometric mean of comparable cases).

| Case | Ormdantic | vs SQLAlchemy | vs SQLModel | Base/head | Scope |
| --- | ---: | ---: | ---: | ---: | --- |
| schema create/drop | 5.986 ms | 2.92x (2.74–3.02) | 2.85x (2.76–3.03) | 1.10x (1.01–1.13) | comparable |
| raw batch insert | 81.774 ms | 1.60x (1.59–1.65) | 1.60x (1.59–1.64) | 1.00x (0.99–1.02) | comparable |
| orm insert models | 251.204 ms | 1.63x (1.62–1.64) | 2.61x (2.53–2.65) | 1.02x (1.00–1.03) | comparable |
| orm update filtered | 5.637 ms | 1.12x (1.05–1.16) | 1.12x (1.04–1.18) | 1.04x (0.97–1.19) | comparable |
| orm upsert mixed | 30.395 ms | 36.60x (35.32–37.37) | 38.56x (37.22–40.13) | 0.99x (0.94–1.03) | comparable |
| orm delete filtered | 6.307 ms | 1.22x (1.17–1.30) | 1.33x (1.28–1.57) | 0.99x (0.92–1.09) | comparable |
| count all rows | 0.427 ms | 3.65x (3.30–3.76) | 3.42x (3.17–3.71) | 0.86x (0.79–1.00) | comparable |
| count equality filter | 0.450 ms | 3.86x (3.61–4.27) | 3.98x (3.65–4.11) | 0.93x (0.80–0.98) | comparable |
| count range filter | 0.513 ms | 3.52x (3.25–3.67) | 3.47x (3.29–3.77) | 0.94x (0.85–0.99) | comparable |
| aggregate filtered rows | 0.942 ms | 2.41x (2.29–2.54) | 2.41x (2.28–2.60) | 0.93x (0.91–0.98) | comparable |
| scalar projection read | 1.570 ms | 2.20x (2.12–2.28) | 2.23x (2.15–2.27) | 0.99x (0.95–1.01) | comparable |
| batched primary-key lookup | 141.273 ms | 2.15x (2.11–2.28) | 2.13x (2.07–2.26) | 0.98x (0.96–0.99) | comparable |
| paginated find_many | 2.816 ms | 1.67x (1.55–1.86) | 1.90x (1.77–2.00) | 1.00x (0.91–1.05) | comparable |
| ordered find_many | 3.848 ms | 1.54x (1.46–7.89) | 1.68x (1.64–1.72) | 0.99x (0.93–1.06) | comparable |
| hydrate flat rows | 2.766 ms | 1.66x (1.55–1.89) | 1.97x (1.82–2.04) | 1.02x (0.94–1.08) | comparable |
| serialize simple payloads | 0.751 ms | 0.91x (0.84–0.93) | 1.91x (1.78–1.95) | 0.99x (0.92–1.02) | diagnostic |
| serialize nested payloads | 0.369 ms | 1.00x (0.87–1.09) | 1.15x (1.05–1.35) | 0.98x (0.87–1.14) | diagnostic |
| hydrate relationship results | 7.836 ms | 1.05x (0.91–1.08) | 1.11x (0.99–1.16) | 1.00x (0.90–1.05) | comparable |
| one-to-many relationship loading | 2.966 ms | 1.68x (1.58–1.81) | 1.88x (1.70–2.01) | 1.00x (0.97–1.03) | comparable |
| many-to-one relationship loading | 3.107 ms | 1.44x (1.36–1.56) | 1.58x (1.48–1.73) | 1.00x (0.95–1.08) | comparable |
| nested relationship loading | 6.552 ms | 1.50x (1.40–1.53) | 1.65x (1.48–1.75) | 1.01x (0.94–1.06) | comparable |
