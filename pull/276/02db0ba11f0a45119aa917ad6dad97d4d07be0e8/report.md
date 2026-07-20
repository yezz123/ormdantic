<!-- ormdantic-benchmark-report -->
## Ormdantic benchmark report

**sqlite / ci:** Ormdantic is 2.27x vs SQLAlchemy and 2.44x vs SQLModel (geometric mean of comparable cases).

| Case | Ormdantic | vs SQLAlchemy | vs SQLModel | Base/head | Scope |
| --- | ---: | ---: | ---: | ---: | --- |
| schema create/drop | 7.180 ms | 2.60x (2.55–2.86) | 2.84x (2.66–3.04) | 0.93x (0.88–1.03) | comparable |
| raw batch insert | 85.186 ms | 1.75x (1.71–1.82) | 1.76x (1.72–1.79) | 0.98x (0.95–0.99) | comparable |
| orm insert models | 292.184 ms | 1.67x (1.63–1.71) | 2.61x (2.53–2.72) | 0.96x (0.89–0.98) | comparable |
| orm update filtered | 6.080 ms | 1.14x (1.09–1.15) | 1.18x (1.12–1.30) | 0.93x (0.90–0.96) | comparable |
| orm upsert mixed | 32.885 ms | 34.71x (33.96–36.07) | 36.33x (35.77–38.60) | 0.94x (0.92–1.04) | comparable |
| orm delete filtered | 6.438 ms | 1.32x (1.27–1.38) | 1.34x (1.25–1.41) | 0.97x (0.92–1.01) | comparable |
| count all rows | 0.457 ms | 3.88x (3.59–3.97) | 3.89x (3.54–3.99) | 0.95x (0.90–1.06) | comparable |
| count equality filter | 0.509 ms | 3.93x (3.68–4.31) | 3.84x (3.66–4.26) | 0.96x (0.89–1.05) | comparable |
| count range filter | 0.584 ms | 3.54x (3.11–3.69) | 3.69x (3.14–3.87) | 0.91x (0.81–0.98) | comparable |
| aggregate filtered rows | 0.989 ms | 2.50x (2.37–2.65) | 2.56x (2.48–2.58) | 0.94x (0.91–0.97) | comparable |
| scalar projection read | 1.732 ms | 2.26x (2.04–2.35) | 2.36x (2.14–2.43) | 0.97x (0.89–1.04) | comparable |
| batched primary-key lookup | 145.275 ms | 2.12x (2.09–2.31) | 2.22x (2.17–2.36) | 0.97x (0.96–0.98) | comparable |
| paginated find_many | 3.126 ms | 1.68x (1.57–1.79) | 1.87x (1.80–2.00) | 0.91x (0.88–0.96) | comparable |
| ordered find_many | 4.218 ms | 1.54x (1.48–9.06) | 1.70x (1.65–1.75) | 0.91x (0.86–0.97) | comparable |
| hydrate flat rows | 3.285 ms | 1.66x (1.54–1.84) | 1.91x (1.79–2.06) | 0.88x (0.82–0.95) | comparable |
| serialize simple payloads | 0.829 ms | 0.90x (0.88–0.96) | 2.14x (2.02–2.23) | 0.90x (0.87–0.95) | diagnostic |
| serialize nested payloads | 0.399 ms | 1.05x (0.84–1.20) | 1.50x (1.24–1.59) | 1.07x (0.88–1.14) | diagnostic |
| hydrate relationship results | 8.044 ms | 1.15x (1.11–1.19) | 1.19x (1.14–1.31) | 0.96x (0.93–1.01) | comparable |
| one-to-many relationship loading | 3.043 ms | 1.84x (1.76–1.98) | 1.94x (1.86–2.25) | 0.97x (0.95–1.00) | comparable |
| many-to-one relationship loading | 3.168 ms | 1.63x (1.51–1.73) | 1.75x (1.64–1.89) | 0.97x (0.92–1.02) | comparable |
| nested relationship loading | 7.081 ms | 1.51x (1.47–1.62) | 1.68x (1.62–1.81) | 0.93x (0.91–1.02) | comparable |
