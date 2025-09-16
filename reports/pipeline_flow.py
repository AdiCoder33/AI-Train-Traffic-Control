from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch

STEPS = [
    "Raw CSVs\nTrain_details.csv",
    "Normalize Events\n`to_train_events()`",
    "Slice Corridor / National\n`corridor.slice()`",
    "Build Section Graph\n`graph.build()`",
    "Baseline Replay\n`national_replay.run()`",
    "Risk Analysis\n`risk.analyze()`",
    "Optimizer\n`opt.engine.propose()`",
    "Apply & Validate Plan\n`apply_plan.apply_and_validate()`",
    "KPIs & Charts\n`plan_apply_report.json`\n`plot_plan_impact.py`",
]

COLORS = {
    "prep": "#eef6ff",
    "run": "#f7f9fc",
    "output": "#f0fff4",
}

STYLES = [
    (0, "prep"),
    (1, "prep"),
    (2, "prep"),
    (3, "prep"),
    (4, "run"),
    (5, "run"),
    (6, "run"),
    (7, "run"),
    (8, "output"),
]


def draw_flow(filename: Path) -> None:
    fig, ax = plt.subplots(figsize=(12, 2.8))
    ax.axis("off")

    total = len(STEPS)
    x_positions = [i for i in range(total)]

    for idx, (label, (_, style)) in enumerate(zip(STEPS, STYLES)):
        x = x_positions[idx]
        box = FancyBboxPatch(
            (x, 0.2),
            0.9,
            1.45,
            boxstyle="round,pad=0.2",
            linewidth=1.5,
            edgecolor="#5b8def" if style != "output" else "#32a852",
            facecolor=COLORS[style],
        )
        ax.add_patch(box)
        ax.text(
            x + 0.45,
            0.95,
            label,
            ha="center",
            va="center",
            fontsize=10,
            color="#203864" if style != "output" else "#205c2a",
            wrap=True,
        )
        if idx < total - 1:
            ax.annotate(
                "",
                xy=(x + 0.9, 0.92),
                xytext=(x + 1.0, 0.92),
                arrowprops=dict(arrowstyle="-|>", color="#5b8def", linewidth=1.8),
            )

    ax.set_xlim(-0.1, total - 0.0)
    ax.set_ylim(0, 1.9)
    fig.tight_layout()
    filename.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(filename, dpi=200, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    out = Path("reports/pipeline_flow.png")
    draw_flow(out)
    print(f"Saved {out.resolve()}")


if __name__ == "__main__":
    main()
