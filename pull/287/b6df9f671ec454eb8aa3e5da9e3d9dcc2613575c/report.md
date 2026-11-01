<!-- ormdantic-benchmark-report -->
## Ormdantic benchmark report

**sqlite / ci:** Ormdantic is 2.19x vs SQLAlchemy and 2.32x vs SQLModel (geometric mean of comparable cases).

| Case | Ormdantic | vs SQLAlchemy | vs SQLModel | Base/head | Scope |
| --- | ---: | ---: | ---: | ---: | --- |
| schema create/drop | 5.085 ms | 2.33x (2.20–2.48) | 2.41x (2.29–2.55) | 1.06x (1.00–1.11) | comparable |
| raw batch insert | 43.370 ms | 1.75x (1.66–2.06) | 1.74x (1.72–1.86) | 1.03x (1.01–1.10) | comparable |
| orm insert models | 145.726 ms | 1.54x (1.48–1.69) | 2.23x (2.10–2.33) | 1.00x (0.95–1.04) | comparable |
| orm update filtered | 3.685 ms | 1.15x (1.07–1.19) | 1.15x (1.07–1.18) | 1.09x (0.99–1.15) | comparable |
| orm upsert mixed | 18.433 ms | 29.59x (27.28–30.30) | 29.61x (28.15–30.39) | 1.01x (0.96–1.07) | comparable |
| orm delete filtered | 4.689 ms | 1.12x (0.63–1.26) | 1.13x (0.63–1.82) | 1.70x (0.95–2.03) | comparable |
| count all rows | 0.230 ms | 4.18x (3.98–4.52) | 4.00x (3.76–4.68) | 1.13x (1.05–1.48) | comparable |
| count equality filter | 0.270 ms | 4.20x (3.96–4.72) | 4.46x (3.88–5.16) | 1.24x (1.18–1.39) | comparable |
| count range filter | 0.295 ms | 3.76x (3.43–4.23) | 3.77x (3.41–4.23) | 1.23x (1.10–1.36) | comparable |
| aggregate filtered rows | 0.587 ms | 2.44x (2.37–2.55) | 2.54x (2.29–2.63) | 1.13x (1.05–1.17) | comparable |
| scalar projection read | 0.939 ms | 2.22x (2.15–2.30) | 2.20x (2.14–2.28) | 1.09x (1.04–1.14) | comparable |
| batched primary-key lookup | 53.417 ms | 2.51x (2.45–2.87) | 2.55x (2.48–2.99) | 0.95x (0.92–1.06) | comparable |
| paginated find_many | 1.802 ms | 1.67x (1.54–1.77) | 1.85x (1.70–2.02) | 0.96x (0.88–1.05) | comparable |
| ordered find_many | 2.421 ms | 1.56x (1.44–10.41) | 1.69x (1.56–1.76) | 0.93x (0.85–0.97) | comparable |
| hydrate flat rows | 1.669 ms | 1.72x (1.57–1.89) | 1.97x (1.79–2.08) | 0.95x (0.87–1.02) | comparable |
| serialize simple payloads | 0.377 ms | 0.93x (0.90–0.95) | 2.22x (2.19–2.30) | 0.95x (0.92–0.98) | diagnostic |
| serialize nested payloads | 0.209 ms | 0.93x (0.77–1.04) | 1.25x (1.03–1.36) | 0.93x (0.73–1.14) | diagnostic |
| hydrate relationship results | 4.804 ms | 0.97x (0.88–1.05) | 1.05x (0.97–1.10) | 0.96x (0.90–1.04) | comparable |
| one-to-many relationship loading | 1.928 ms | 1.55x (1.43–1.68) | 1.69x (1.52–1.72) | 1.00x (0.93–1.04) | comparable |
| many-to-one relationship loading | 1.985 ms | 1.45x (1.31–1.61) | 1.57x (1.38–1.62) | 0.94x (0.89–1.01) | comparable |
| nested relationship loading | 4.249 ms | 1.37x (0.87–1.49) | 1.47x (0.94–1.56) | 0.92x (0.60–0.99) | comparable |
