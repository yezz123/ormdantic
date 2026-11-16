<!-- ormdantic-benchmark-report -->
## Ormdantic benchmark report

**sqlite / ci:** Ormdantic is 2.16x vs SQLAlchemy and 2.27x vs SQLModel (geometric mean of comparable cases).

| Case | Ormdantic | vs SQLAlchemy | vs SQLModel | Base/head | Scope |
| --- | ---: | ---: | ---: | ---: | --- |
| schema create/drop | 5.263 ms | 2.30x (2.09–2.40) | 2.35x (2.10–2.46) | 0.96x (0.85–0.98) | comparable |
| raw batch insert | 44.056 ms | 1.80x (1.74–1.83) | 1.85x (1.78–1.95) | 0.96x (0.94–0.98) | comparable |
| orm insert models | 150.741 ms | 1.71x (1.52–1.77) | 2.41x (2.28–2.77) | 0.97x (0.92–1.00) | comparable |
| orm update filtered | 3.983 ms | 1.15x (1.11–1.79) | 1.15x (1.06–1.21) | 0.97x (0.90–1.03) | comparable |
| orm upsert mixed | 19.017 ms | 26.33x (24.55–29.04) | 28.20x (25.91–29.24) | 1.00x (0.94–1.06) | comparable |
| orm delete filtered | 7.953 ms | 0.76x (0.65–1.08) | 0.74x (0.65–1.03) | 0.70x (0.53–1.14) | comparable |
| count all rows | 0.311 ms | 3.38x (3.20–3.61) | 3.45x (3.27–3.95) | 1.05x (1.00–1.09) | comparable |
| count equality filter | 0.337 ms | 3.96x (3.58–4.23) | 3.72x (3.55–4.25) | 0.99x (0.95–1.11) | comparable |
| count range filter | 0.297 ms | 3.86x (3.40–4.16) | 3.91x (3.37–4.34) | 1.23x (1.10–1.29) | comparable |
| aggregate filtered rows | 0.602 ms | 2.61x (2.32–2.88) | 2.59x (2.28–2.90) | 1.12x (1.00–1.26) | comparable |
| scalar projection read | 1.084 ms | 2.31x (2.14–2.44) | 2.30x (2.17–2.39) | 1.00x (0.94–1.13) | comparable |
| batched primary-key lookup | 46.726 ms | 3.00x (2.48–3.52) | 3.05x (2.58–3.58) | 1.12x (0.94–1.33) | comparable |
| paginated find_many | 1.844 ms | 1.70x (1.56–1.79) | 1.94x (1.77–2.05) | 0.97x (0.88–1.02) | comparable |
| ordered find_many | 2.343 ms | 1.58x (1.47–11.44) | 1.72x (1.60–1.83) | 1.05x (0.98–1.09) | comparable |
| hydrate flat rows | 1.854 ms | 1.73x (1.59–1.82) | 1.84x (1.76–1.98) | 1.07x (1.03–1.24) | comparable |
| serialize simple payloads | 0.390 ms | 0.91x (0.87–0.94) | 2.18x (2.10–2.28) | 0.98x (0.96–1.09) | diagnostic |
| serialize nested payloads | 0.211 ms | 0.90x (0.83–1.06) | 1.17x (1.02–1.36) | 1.07x (0.93–1.29) | diagnostic |
| hydrate relationship results | 4.803 ms | 1.00x (0.89–1.06) | 1.06x (0.98–1.17) | 1.02x (0.95–1.08) | comparable |
| one-to-many relationship loading | 1.913 ms | 1.57x (1.45–1.71) | 1.63x (1.53–1.79) | 0.99x (0.92–1.03) | comparable |
| many-to-one relationship loading | 1.998 ms | 1.47x (1.33–1.57) | 1.63x (1.48–1.69) | 1.06x (0.95–1.13) | comparable |
| nested relationship loading | 4.304 ms | 1.43x (1.30–1.55) | 1.51x (1.39–1.61) | 0.95x (0.89–1.02) | comparable |
