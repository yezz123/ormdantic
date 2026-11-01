<!-- ormdantic-benchmark-report -->
## Ormdantic benchmark report

**sqlite / ci:** Ormdantic is 2.10x vs SQLAlchemy and 2.22x vs SQLModel (geometric mean of comparable cases).

| Case | Ormdantic | vs SQLAlchemy | vs SQLModel | Base/head | Scope |
| --- | ---: | ---: | ---: | ---: | --- |
| schema create/drop | 5.090 ms | 2.53x (1.99–2.68) | 2.59x (2.03–2.74) | 0.94x (0.75–0.96) | comparable |
| raw batch insert | 59.386 ms | 1.68x (1.60–1.91) | 1.63x (1.60–1.69) | 1.00x (0.96–1.01) | comparable |
| orm insert models | 187.101 ms | 1.65x (1.61–1.70) | 2.39x (2.33–2.49) | 0.98x (0.96–0.99) | comparable |
| orm update filtered | 5.957 ms | 1.09x (1.04–1.28) | 1.15x (1.06–1.24) | 1.00x (0.99–1.00) | comparable |
| orm upsert mixed | 23.686 ms | 29.33x (28.85–30.45) | 30.57x (29.98–32.15) | 1.00x (0.99–1.00) | comparable |
| orm delete filtered | 7.875 ms | 1.25x (1.22–1.31) | 1.20x (1.14–1.30) | 1.00x (0.98–1.01) | comparable |
| count all rows | 0.345 ms | 3.35x (3.03–3.49) | 3.42x (3.12–3.50) | 0.94x (0.87–0.96) | comparable |
| count equality filter | 0.388 ms | 3.46x (3.32–3.67) | 3.44x (3.30–3.68) | 0.91x (0.84–0.97) | comparable |
| count range filter | 0.420 ms | 3.23x (3.15–3.41) | 3.21x (3.14–3.39) | 1.09x (0.94–1.16) | comparable |
| aggregate filtered rows | 0.726 ms | 2.30x (2.15–2.40) | 2.26x (2.15–2.32) | 1.03x (0.98–1.08) | comparable |
| scalar projection read | 1.248 ms | 2.08x (2.07–2.17) | 2.11x (2.07–2.19) | 1.00x (0.99–1.05) | comparable |
| batched primary-key lookup | 91.796 ms | 2.19x (1.99–2.30) | 2.15x (2.08–2.35) | 0.96x (0.95–1.01) | comparable |
| paginated find_many | 2.214 ms | 1.61x (1.48–1.66) | 1.83x (1.74–1.88) | 0.96x (0.92–1.04) | comparable |
| ordered find_many | 2.917 ms | 1.50x (1.43–8.66) | 1.65x (1.61–1.70) | 1.00x (0.99–1.02) | comparable |
| hydrate flat rows | 2.153 ms | 1.60x (1.53–1.73) | 1.83x (1.77–1.90) | 0.99x (0.96–1.01) | comparable |
| serialize simple payloads | 0.575 ms | 0.86x (0.82–0.87) | 1.95x (1.89–2.02) | 0.95x (0.91–0.97) | diagnostic |
| serialize nested payloads | 0.287 ms | 0.93x (0.88–1.01) | 1.14x (1.06–1.21) | 0.94x (0.87–1.07) | diagnostic |
| hydrate relationship results | 5.971 ms | 0.94x (0.89–0.97) | 1.02x (0.98–1.05) | 0.99x (0.95–1.06) | comparable |
| one-to-many relationship loading | 2.244 ms | 1.63x (1.51–1.72) | 1.71x (1.62–1.79) | 1.01x (0.97–1.03) | comparable |
| many-to-one relationship loading | 2.317 ms | 1.42x (1.35–1.43) | 1.52x (1.46–1.62) | 1.00x (0.98–1.06) | comparable |
| nested relationship loading | 4.932 ms | 1.41x (1.33–1.44) | 1.52x (1.42–1.56) | 0.99x (0.93–1.06) | comparable |
