<!-- ormdantic-benchmark-report -->
## Ormdantic benchmark report

**sqlite / ci:** Ormdantic is 2.11x vs SQLAlchemy and 2.19x vs SQLModel (geometric mean of comparable cases).

| Case | Ormdantic | vs SQLAlchemy | vs SQLModel | Base/head | Scope |
| --- | ---: | ---: | ---: | ---: | --- |
| schema create/drop | 7.841 ms | 2.48x (1.55–2.98) | 2.87x (1.66–6.40) | 0.86x (0.54–1.31) | comparable |
| raw batch insert | 68.567 ms | 2.75x (1.55–4.37) | 2.56x (1.58–4.53) | 0.89x (0.55–1.02) | comparable |
| orm insert models | 316.027 ms | 1.89x (1.23–3.10) | 2.14x (1.64–3.77) | 2.25x (0.77–4.44) | comparable |
| orm update filtered | 5.089 ms | 1.14x (0.77–1.97) | 1.18x (0.77–1.80) | 1.39x (0.72–10.50) | comparable |
| orm upsert mixed | 34.311 ms | 22.38x (2.68–38.11) | 23.06x (2.86–32.73) | 0.76x (0.09–1.41) | comparable |
| orm delete filtered | 7.493 ms | 0.92x (0.70–1.49) | 0.89x (0.71–1.31) | 0.77x (0.57–1.12) | comparable |
| count all rows | 0.399 ms | 3.02x (2.76–3.62) | 2.97x (2.79–3.41) | 1.02x (0.97–1.21) | comparable |
| count equality filter | 0.393 ms | 3.66x (3.03–4.20) | 3.55x (3.12–3.82) | 1.07x (0.89–1.16) | comparable |
| count range filter | 0.443 ms | 3.27x (2.93–3.47) | 3.06x (2.76–3.41) | 1.20x (1.02–1.29) | comparable |
| aggregate filtered rows | 0.814 ms | 2.14x (1.86–2.29) | 2.14x (1.84–2.35) | 1.02x (0.87–1.10) | comparable |
| scalar projection read | 1.304 ms | 2.18x (1.99–2.26) | 2.09x (1.94–2.15) | 1.09x (0.98–1.13) | comparable |
| batched primary-key lookup | 91.056 ms | 2.18x (2.02–2.35) | 2.36x (2.00–2.56) | 0.97x (0.94–1.05) | comparable |
| paginated find_many | 2.457 ms | 1.57x (1.42–1.62) | 1.67x (1.51–1.84) | 0.98x (0.90–1.07) | comparable |
| ordered find_many | 3.149 ms | 1.46x (1.38–8.93) | 1.60x (1.51–1.77) | 1.01x (0.95–1.13) | comparable |
| hydrate flat rows | 2.445 ms | 1.56x (1.48–1.93) | 1.74x (1.66–1.87) | 1.05x (1.00–1.11) | comparable |
| serialize simple payloads | 0.596 ms | 0.87x (0.85–0.88) | 1.96x (1.89–2.00) | 1.03x (1.00–1.07) | diagnostic |
| serialize nested payloads | 0.284 ms | 0.96x (0.84–1.05) | 1.27x (1.06–1.36) | 1.24x (1.05–1.28) | diagnostic |
| hydrate relationship results | 6.008 ms | 1.03x (0.98–1.07) | 1.06x (1.03–1.21) | 1.07x (1.00–1.10) | comparable |
| one-to-many relationship loading | 2.275 ms | 1.67x (1.48–1.81) | 1.75x (1.66–1.89) | 1.07x (1.03–1.13) | comparable |
| many-to-one relationship loading | 2.369 ms | 1.49x (1.45–1.57) | 1.59x (1.48–1.70) | 1.07x (1.01–1.14) | comparable |
| nested relationship loading | 5.160 ms | 1.42x (0.68–1.50) | 1.64x (0.78–1.69) | 1.04x (0.50–1.08) | comparable |
