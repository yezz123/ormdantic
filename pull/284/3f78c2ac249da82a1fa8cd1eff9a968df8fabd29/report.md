<!-- ormdantic-benchmark-report -->
## Ormdantic benchmark report

**sqlite / ci:** Ormdantic is 2.13x vs SQLAlchemy and 2.29x vs SQLModel (geometric mean of comparable cases).

| Case | Ormdantic | vs SQLAlchemy | vs SQLModel | Base/head | Scope |
| --- | ---: | ---: | ---: | ---: | --- |
| schema create/drop | 5.205 ms | 2.85x (2.71–2.99) | 2.97x (2.77–3.10) | 1.16x (1.07–1.35) | comparable |
| raw batch insert | 77.689 ms | 1.64x (1.60–1.69) | 1.65x (1.59–1.70) | 1.00x (0.98–1.03) | comparable |
| orm insert models | 251.121 ms | 1.64x (1.60–1.69) | 2.46x (2.42–2.52) | 1.02x (0.99–1.04) | comparable |
| orm update filtered | 5.540 ms | 1.13x (1.10–1.16) | 1.12x (1.10–1.16) | 1.04x (0.96–1.08) | comparable |
| orm upsert mixed | 30.360 ms | 30.87x (29.91–32.92) | 31.85x (31.26–32.81) | 0.98x (0.96–1.01) | comparable |
| orm delete filtered | 6.044 ms | 1.30x (1.19–1.38) | 1.29x (1.18–1.45) | 1.02x (0.94–1.07) | comparable |
| count all rows | 0.457 ms | 3.29x (3.06–3.66) | 3.26x (3.06–3.61) | 1.01x (0.94–1.10) | comparable |
| count equality filter | 0.489 ms | 3.62x (3.49–3.90) | 3.59x (3.45–3.89) | 1.01x (0.96–1.06) | comparable |
| count range filter | 0.587 ms | 3.10x (2.91–3.28) | 3.21x (3.02–3.40) | 0.96x (0.89–1.00) | comparable |
| aggregate filtered rows | 1.001 ms | 2.21x (2.14–2.31) | 2.30x (2.22–2.37) | 1.01x (0.97–1.04) | comparable |
| scalar projection read | 1.738 ms | 2.06x (1.95–2.10) | 2.12x (2.02–2.17) | 1.00x (0.95–1.14) | comparable |
| batched primary-key lookup | 115.401 ms | 2.11x (2.06–2.33) | 2.18x (2.14–2.27) | 1.00x (0.96–1.05) | comparable |
| paginated find_many | 2.930 ms | 1.64x (1.56–1.81) | 1.84x (1.74–1.93) | 0.99x (0.94–1.04) | comparable |
| ordered find_many | 3.813 ms | 1.52x (1.45–7.48) | 1.73x (1.66–1.76) | 1.01x (0.99–1.04) | comparable |
| hydrate flat rows | 2.952 ms | 1.60x (1.55–1.67) | 1.90x (1.77–1.95) | 0.99x (0.96–1.08) | comparable |
| serialize simple payloads | 0.752 ms | 0.89x (0.85–0.92) | 2.00x (1.97–2.05) | 1.02x (0.99–1.04) | diagnostic |
| serialize nested payloads | 0.385 ms | 0.92x (0.78–1.01) | 1.10x (0.94–1.22) | 1.05x (0.89–1.13) | diagnostic |
| hydrate relationship results | 7.550 ms | 0.98x (0.85–1.03) | 1.07x (0.94–1.11) | 1.02x (0.89–1.05) | comparable |
| one-to-many relationship loading | 2.925 ms | 1.66x (1.59–1.82) | 1.86x (1.74–1.95) | 1.02x (0.98–1.04) | comparable |
| many-to-one relationship loading | 2.951 ms | 1.47x (1.40–1.57) | 1.57x (1.47–1.69) | 1.03x (0.97–1.08) | comparable |
| nested relationship loading | 6.588 ms | 1.50x (1.38–1.57) | 1.58x (1.44–1.73) | 1.01x (0.95–1.05) | comparable |
