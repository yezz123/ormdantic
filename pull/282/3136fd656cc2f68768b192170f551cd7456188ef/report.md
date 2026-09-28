<!-- ormdantic-benchmark-report -->
## Ormdantic benchmark report

**sqlite / ci:** Ormdantic is 2.07x vs SQLAlchemy and 2.19x vs SQLModel (geometric mean of comparable cases).

| Case | Ormdantic | vs SQLAlchemy | vs SQLModel | Base/head | Scope |
| --- | ---: | ---: | ---: | ---: | --- |
| schema create/drop | 5.377 ms | 2.26x (2.03–2.54) | 2.33x (2.13–2.75) | 0.93x (0.85–1.05) | comparable |
| raw batch insert | 42.959 ms | 1.79x (1.69–1.89) | 1.80x (1.73–1.88) | 0.95x (0.93–1.02) | comparable |
| orm insert models | 145.414 ms | 1.53x (1.49–1.87) | 2.33x (2.20–2.43) | 0.99x (0.94–1.02) | comparable |
| orm update filtered | 3.849 ms | 1.07x (1.03–1.11) | 1.09x (1.04–1.16) | 1.52x (0.92–1.79) | comparable |
| orm upsert mixed | 17.495 ms | 27.66x (26.72–29.13) | 29.89x (28.13–31.76) | 1.01x (0.97–1.06) | comparable |
| orm delete filtered | 7.949 ms | 0.67x (0.60–0.78) | 0.66x (0.58–0.74) | 0.90x (0.46–1.11) | comparable |
| count all rows | 0.324 ms | 3.11x (2.89–4.21) | 3.17x (2.77–4.25) | 0.83x (0.79–1.11) | comparable |
| count equality filter | 0.272 ms | 3.90x (3.25–4.63) | 3.86x (3.33–4.54) | 0.94x (0.81–1.20) | comparable |
| count range filter | 0.359 ms | 3.32x (3.16–3.69) | 3.35x (3.24–3.62) | 0.88x (0.78–0.97) | comparable |
| aggregate filtered rows | 0.553 ms | 2.36x (1.92–2.72) | 2.40x (1.93–2.79) | 1.05x (0.82–1.12) | comparable |
| scalar projection read | 1.018 ms | 1.99x (1.87–2.30) | 2.15x (1.89–2.58) | 0.92x (0.87–1.08) | comparable |
| batched primary-key lookup | 51.175 ms | 2.72x (2.42–2.84) | 2.56x (2.43–2.75) | 1.02x (0.95–1.11) | comparable |
| paginated find_many | 1.745 ms | 1.62x (1.50–1.83) | 1.86x (1.75–2.07) | 0.99x (0.93–1.11) | comparable |
| ordered find_many | 2.107 ms | 1.68x (1.50–10.70) | 1.72x (1.54–1.81) | 1.19x (1.06–1.21) | comparable |
| hydrate flat rows | 1.734 ms | 1.61x (1.39–1.83) | 1.72x (1.55–1.91) | 0.98x (0.90–1.10) | comparable |
| serialize simple payloads | 0.369 ms | 0.89x (0.86–0.91) | 2.17x (2.06–2.23) | 1.09x (1.00–1.13) | diagnostic |
| serialize nested payloads | 0.184 ms | 0.99x (0.89–1.13) | 1.21x (1.08–1.37) | 1.05x (0.94–1.17) | diagnostic |
| hydrate relationship results | 4.471 ms | 0.99x (0.97–1.16) | 1.05x (1.00–1.10) | 1.01x (0.94–1.15) | comparable |
| one-to-many relationship loading | 1.826 ms | 1.60x (1.43–1.79) | 1.66x (1.55–1.78) | 0.99x (0.96–1.07) | comparable |
| many-to-one relationship loading | 1.894 ms | 1.51x (1.38–1.58) | 1.60x (1.38–1.64) | 1.04x (0.96–1.09) | comparable |
| nested relationship loading | 3.691 ms | 1.48x (1.36–1.56) | 1.69x (1.47–1.74) | 1.07x (0.94–1.14) | comparable |
