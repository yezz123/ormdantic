from __future__ import annotations

from benchmark.run import main


def test_cli_rejects_billion_profile_without_confirmation(capsys) -> None:
    exit_code = main(["--backend", "sqlite", "--profile", "billion"])

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "--i-understand-this-may-be-expensive" in captured.err


def test_cli_accepts_smoke_profile_with_backend_and_disabled_docs_charts(
    monkeypatch,
    tmp_path,
) -> None:
    calls = {}

    def fake_run_from_config(config, *, allow_missing=False, progress=None):
        calls["config"] = config
        calls["allow_missing"] = allow_missing
        return [
            BenchmarkMeasurement(
                backend=config.backend,
                profile=config.profile,
                case="count all rows",
                rows=config.rows,
                orm="ormdantic",
                median_ms=1.0,
                samples_ms=(1.0,),
            )
        ]

    from benchmark.charts import BenchmarkMeasurement

    monkeypatch.setattr("benchmark.run.run_from_config", fake_run_from_config)
    monkeypatch.setattr(
        "benchmark.run.write_chart_bundle",
        lambda measurements, output_dir, backend=None, profile=None: type(
            "Artifacts",
            (),
            {
                "latency_svg": tmp_path / "latency.svg",
                "speedup_svg": tmp_path / "speedup.svg",
                "summary_csv": tmp_path / "summary.csv",
            },
        )(),
    )
    (tmp_path / "summary.csv").write_text("backend,profile,case\n", encoding="utf-8")

    exit_code = main(
        [
            "--backend",
            "sqlite",
            "--profile",
            "smoke",
            "--output",
            str(tmp_path / "result.json"),
            "--charts-dir",
            str(tmp_path / "charts"),
            "--docs-charts-dir",
            "",
            "--allow-missing",
        ]
    )

    assert exit_code == 0
    assert calls["config"].backend == "sqlite"
    assert calls["config"].profile == "smoke"
    assert calls["allow_missing"] is True


def test_cli_reports_non_runtime_execution_errors(monkeypatch, capsys) -> None:
    def fail_run_from_config(config, *, allow_missing=False, progress=None):
        raise ValueError("driver exploded")

    monkeypatch.setattr("benchmark.run.run_from_config", fail_run_from_config)

    exit_code = main(["--backend", "sqlite", "--profile", "smoke"])

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "driver exploded" in captured.err
