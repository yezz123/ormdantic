<!-- ormdantic-benchmark-report -->
## Ormdantic benchmark report

**sqlite / ci:** Ormdantic is 2.15x vs SQLAlchemy and 2.29x vs SQLModel (geometric mean of comparable cases).

| Case | Ormdantic | vs SQLAlchemy | vs SQLModel | Base/head | Scope |
| --- | ---: | ---: | ---: | ---: | --- |
| schema create/drop | 4.937 ms | 2.79x (2.54–2.90) | 2.88x (2.58–2.92) | 1.03x (0.93–1.10) | comparable |
| raw batch insert | 76.272 ms | 1.64x (1.59–1.77) | 1.67x (1.60–1.76) | 0.98x (0.96–1.02) | comparable |
| orm insert models | 245.891 ms | 1.67x (1.62–1.75) | 2.46x (2.37–2.53) | 1.00x (0.98–1.07) | comparable |
| orm update filtered | 5.264 ms | 1.15x (1.12–1.22) | 1.15x (1.13–1.18) | 1.01x (0.98–1.04) | comparable |
| orm upsert mixed | 29.708 ms | 31.01x (29.80–31.76) | 31.74x (31.09–34.12) | 0.97x (0.94–1.03) | comparable |
| orm delete filtered | 5.878 ms | 1.32x (1.29–1.36) | 1.30x (1.26–1.36) | 0.98x (0.96–1.02) | comparable |
| count all rows | 0.429 ms | 3.56x (3.32–3.76) | 3.34x (3.25–3.56) | 0.98x (0.92–1.06) | comparable |
| count equality filter | 0.482 ms | 3.55x (3.39–3.83) | 3.57x (3.41–3.87) | 0.97x (0.93–1.04) | comparable |
| count range filter | 0.537 ms | 3.44x (3.19–3.58) | 3.42x (3.06–3.49) | 0.96x (0.87–0.99) | comparable |
| aggregate filtered rows | 0.986 ms | 2.22x (2.15–2.33) | 2.23x (2.20–2.38) | 0.97x (0.93–1.01) | comparable |
| scalar projection read | 1.617 ms | 2.10x (2.08–2.17) | 2.09x (2.06–2.11) | 0.98x (0.97–1.07) | comparable |
| batched primary-key lookup | 116.787 ms | 2.02x (2.00–2.11) | 2.15x (2.01–2.29) | 1.01x (0.86–1.03) | comparable |
| paginated find_many | 2.865 ms | 1.61x (1.51–1.65) | 1.83x (1.71–1.86) | 0.98x (0.92–0.99) | comparable |
| ordered find_many | 3.780 ms | 1.48x (1.42–7.95) | 1.64x (1.57–1.69) | 0.98x (0.94–1.03) | comparable |
| hydrate flat rows | 2.839 ms | 1.59x (1.50–1.76) | 1.80x (1.73–1.93) | 0.99x (0.95–1.08) | comparable |
| serialize simple payloads | 0.724 ms | 0.93x (0.91–0.96) | 2.08x (2.01–2.16) | 0.99x (0.97–1.14) | diagnostic |
| serialize nested payloads | 0.365 ms | 1.02x (0.84–1.06) | 1.21x (1.05–1.30) | 1.08x (0.90–1.14) | diagnostic |
| hydrate relationship results | 7.515 ms | 1.02x (0.96–1.05) | 1.09x (1.02–1.16) | 1.00x (0.95–1.04) | comparable |
| one-to-many relationship loading | 2.850 ms | 1.65x (1.59–1.85) | 1.90x (1.75–2.01) | 1.00x (0.98–1.04) | comparable |
| many-to-one relationship loading | 2.931 ms | 1.51x (1.45–1.61) | 1.68x (1.58–1.82) | 1.00x (0.97–1.03) | comparable |
| nested relationship loading | 6.480 ms | 1.45x (1.39–1.58) | 1.59x (1.54–1.66) | 0.96x (0.95–1.05) | comparable |
