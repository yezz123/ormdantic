<!-- ormdantic-benchmark-report -->
## Ormdantic benchmark report

**sqlite / ci:** Ormdantic is 2.17x vs SQLAlchemy and 2.33x vs SQLModel (geometric mean of comparable cases).

| Case | Ormdantic | vs SQLAlchemy | vs SQLModel | Base/head | Scope |
| --- | ---: | ---: | ---: | ---: | --- |
| schema create/drop | 6.416 ms | 2.71x (2.61–2.81) | 2.80x (2.64–2.86) | 0.98x (0.93–1.02) | comparable |
| raw batch insert | 81.247 ms | 1.60x (1.56–1.64) | 1.58x (1.56–1.63) | 1.00x (0.98–1.01) | comparable |
| orm insert models | 250.032 ms | 1.63x (1.61–1.64) | 2.64x (2.54–2.66) | 0.98x (0.97–0.99) | comparable |
| orm update filtered | 5.543 ms | 1.09x (1.03–1.15) | 1.12x (1.05–1.18) | 1.00x (0.94–1.06) | comparable |
| orm upsert mixed | 28.760 ms | 36.83x (36.11–38.76) | 40.22x (39.15–41.92) | 1.01x (1.00–1.03) | comparable |
| orm delete filtered | 5.866 ms | 1.21x (1.19–1.28) | 1.25x (1.17–1.31) | 0.99x (0.97–1.07) | comparable |
| count all rows | 0.350 ms | 3.64x (3.07–4.03) | 3.82x (2.98–4.16) | 1.12x (0.93–1.23) | comparable |
| count equality filter | 0.413 ms | 3.66x (3.40–4.14) | 3.54x (3.34–3.85) | 0.96x (0.88–1.05) | comparable |
| count range filter | 0.421 ms | 3.63x (3.52–3.98) | 3.65x (3.40–3.90) | 1.09x (1.04–1.16) | comparable |
| aggregate filtered rows | 0.794 ms | 2.44x (2.41–2.65) | 2.46x (2.43–2.70) | 1.02x (0.98–1.06) | comparable |
| scalar projection read | 1.447 ms | 2.15x (2.04–2.24) | 2.14x (2.05–2.21) | 0.99x (0.96–1.07) | comparable |
| batched primary-key lookup | 135.935 ms | 2.22x (2.06–2.39) | 2.14x (2.14–2.47) | 1.01x (1.00–1.02) | comparable |
| paginated find_many | 2.547 ms | 1.67x (1.55–1.70) | 1.89x (1.81–2.02) | 1.01x (0.95–1.06) | comparable |
| ordered find_many | 3.385 ms | 1.54x (1.47–6.60) | 1.72x (1.67–1.79) | 1.03x (1.00–1.12) | comparable |
| hydrate flat rows | 2.681 ms | 1.58x (1.50–1.68) | 1.86x (1.77–1.99) | 1.02x (0.97–1.08) | comparable |
| serialize simple payloads | 0.720 ms | 0.92x (0.90–0.95) | 1.91x (1.88–1.99) | 1.02x (0.97–1.05) | diagnostic |
| serialize nested payloads | 0.386 ms | 1.07x (0.92–1.11) | 1.10x (1.00–1.12) | 1.10x (0.93–1.14) | diagnostic |
| hydrate relationship results | 7.556 ms | 1.00x (0.90–1.05) | 1.08x (1.02–1.12) | 0.98x (0.95–1.04) | comparable |
| one-to-many relationship loading | 2.797 ms | 1.57x (1.48–1.70) | 1.72x (1.59–1.83) | 0.98x (0.94–1.03) | comparable |
| many-to-one relationship loading | 2.937 ms | 1.39x (1.32–1.65) | 1.52x (1.46–1.67) | 0.96x (0.92–1.05) | comparable |
| nested relationship loading | 6.197 ms | 1.48x (1.38–1.54) | 1.58x (1.52–1.60) | 1.01x (0.99–1.05) | comparable |
