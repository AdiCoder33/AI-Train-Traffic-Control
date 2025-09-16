"""Generate annotated before/after impact charts from plan_apply_report.json."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, Iterable, Tuple

import matplotlib.pyplot as plt

NUMBER_FMT = "{:,}"


def load_report(path: Path) -> Dict[str, object]:
    with path.open('r', encoding='utf-8') as fh:
        return json.load(fh)


def _percent_reduction(before: float, after: float) -> float:
    if before <= 0:
        return 0.0
    return ((before - after) / before) * 100.0


def _format_percent(value: float) -> str:
    sign = '+' if value > 0 else ''
    return f"{sign}{value:.1f}%"


def _annotate_values(ax, positions: Iterable[Tuple[float, float]], labels: Iterable[str]) -> None:
    for (x, y), label in zip(positions, labels):
        ax.text(x, y, label, ha='center', va='bottom', fontsize=8)


def build_chart(data: Dict[str, object], title: str | None = None, *, sample_size: str | None = None,
                time_window: str | None = None, method: str | None = None) -> plt.Figure:
    delay_before = float(data.get('wait_minutes_before') or 0.0)
    delay_after = float(data.get('wait_minutes_after') or 0.0)
    risks_before = float(data.get('baseline_risks') or 0.0)
    risks_after = float(data.get('applied_risks') or 0.0)

    fig, axes = plt.subplots(1, 2, figsize=(10, 4))

    metrics = [
        ('Delay Minutes', delay_before, delay_after, 'Minutes'),
        ('Risk Events', risks_before, risks_after, 'Events'),
    ]
    colors = ('#5bc0be', '#9b5de5')

    for idx, (label, before_val, after_val, ylabel) in enumerate(metrics):
        ax = axes[idx]
        bars_x = [0, 1]
        width = 0.6
        before_bar = ax.bar(bars_x[0], before_val, width, label='Before', color=colors[0])
        after_bar = ax.bar(bars_x[1], after_val, width, label='After', color=colors[1])

        ax.set_xticks(bars_x)
        ax.set_xticklabels(['Before', 'After'])
        ax.set_ylabel(ylabel)
        reduction = _percent_reduction(before_val, after_val)
        ax.set_title(f"{label} ({_format_percent(reduction)})")
        ax.grid(axis='y', linestyle='--', alpha=0.3)

        max_val = max(before_val, after_val)
        upper = max_val * 1.2 if max_val > 0 else 1.0
        ax.set_ylim(0, upper)
        pad = upper * 0.04
        before_y = min(before_val + pad, upper - pad)
        after_y = min(after_val + pad, upper - pad)
        pct_y = min(max(before_y, after_y) + pad, upper - pad / 2)

        before_label = NUMBER_FMT.format(round(before_val))
        after_label = NUMBER_FMT.format(round(after_val))
        pct_label = f"{_format_percent(reduction)} vs baseline"
        _annotate_values(
            ax,
            [
                (bars_x[0], before_y),
                (bars_x[1], after_y),
                (bars_x[1], pct_y),
            ],
            [before_label, after_label, pct_label],
        )

        if idx == 0:
            ax.legend(loc='upper right')

    if title:
        fig.suptitle(title, fontsize=14)

    meta_bits = []
    if sample_size:
        meta_bits.append(f"N = {sample_size}")
    if time_window:
        meta_bits.append(f"Window: {time_window}")
    if method:
        meta_bits.append(f"Method: {method}")
    if meta_bits:
        fig.text(0.99, 0.02, " | ".join(meta_bits), ha='right', va='bottom', fontsize=9, color='#333333')

    fig.tight_layout(rect=[0, 0.04, 1, 0.96] if title else None)
    return fig


def main() -> int:
    parser = argparse.ArgumentParser(description='Plot before/after delays and risks from plan apply report.')
    parser.add_argument('report', type=Path, help='Path to plan_apply_report.json')
    parser.add_argument('-o', '--output', type=Path, default=Path('plan_impact.png'), help='Output image path (PNG)')
    parser.add_argument('--title', default='Plan Impact: Delay & Risk Reduction', help='Chart title text')
    parser.add_argument('--sample-size', dest='sample_size', help='Sample size (N) to annotate on the plot')
    parser.add_argument('--time-window', dest='time_window', help='Time window description to display')
    parser.add_argument('--method', dest='method', help='Methodology/optimizer label to display')
    args = parser.parse_args()

    data = load_report(args.report)
    fig = build_chart(
        data,
        args.title,
        sample_size=args.sample_size,
        time_window=args.time_window,
        method=args.method,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(args.output, dpi=150)
    plt.close(fig)
    print(f'Saved {args.output.resolve()}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
