"""The acceptance command is the scripted demo, not a live provider."""

from examples import real_model_experiments
from nervipulsa.demo import main


def test_scripted_demo_exits_zero() -> None:
    assert main(["--scripted"]) == 0


def test_demo_without_scripted_is_not_a_real_model_run() -> None:
    assert main([]) == 2

def test_live_experiment_workspaces_are_unique_and_preserve_old_files(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(real_model_experiments, "SCRATCH", tmp_path)
    first = real_model_experiments.fresh_workspace("exp1")
    marker = first / "prior-run.txt"
    marker.write_text("keep", encoding="utf-8")

    second = real_model_experiments.fresh_workspace("exp1")

    assert second != first
    assert second.is_dir()
    assert marker.read_text(encoding="utf-8") == "keep"
