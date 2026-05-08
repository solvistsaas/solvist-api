"""
Opportunity Matrix bubble chart.
X = Ease of Close (1 - close_probability), Y = Financial Impact (expected_value)
Size = expected_value, Color = opportunity type.
Uses numbered markers (#1, #2...) on bubbles for clean readability.
System names appear in the adjacent table. X-axis scaled 0–1.0.
"""
from report.charts.base import BaseChart
import matplotlib.ticker as mticker
import matplotlib.pyplot as plt


class OpportunityMatrixChart(BaseChart):
    """Bubble chart: effort vs impact for all scored opportunities."""

    # Taller figure for better Y-axis label spacing
    FIGURE_SIZE = (7.5, 3.2)

    def __init__(self, data):
        self.data = data
        self.fig, self.ax = plt.subplots(figsize=self.FIGURE_SIZE)

    def render(self):
        ax = self.ax
        self.style_axis(ax)

        if not self.data:
            ax.text(0.5, 0.5, 'Sin oportunidades para mostrar',
                    ha='center', va='center', fontsize=10, color='#6B7280')
            return

        # Sort by expected_value descending for numbering
        sorted_opps = sorted(self.data, key=lambda o: o.get('expected_value', 0), reverse=True)

        for i, opp in enumerate(sorted_opps):
            sys = opp.get('_system', {})
            reason = opp.get('primary_reason', 'unknown')
            color = self.PALETTE.get(reason, '#6B7280')
            ev = opp.get('expected_value', 0)
            cp = opp.get('close_probability', 0.5)
            x = 1 - cp   # effort (inverted: higher close_prob = easier = left)

            # Plot bubble
            ax.scatter(
                x, ev,
                s=max(ev / 400, 40),
                c=color,
                alpha=0.7,
                edgecolors='white',
                linewidth=0.5,
                zorder=3,
            )

            # Numbered marker centered on bubble
            ax.text(
                x, ev,
                str(i + 1),
                fontsize=7,
                fontweight='bold',
                color='white',
                ha='center',
                va='center',
                zorder=4,
            )

        # X-axis: scale 0 to 1.0 with clear ticks
        ax.set_xlim(-0.05, 1.05)
        ax.set_xticks([0, 0.25, 0.5, 0.75, 1.0])
        ax.set_xticklabels(['0', '0.25', '0.5', '0.75', '1.0'])

        # Y-axis: format as currency
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(
            lambda x, _: f'${x/1000:.0f}K' if x >= 1000 else f'${x:.0f}'
        ))

        ax.set_xlabel('Facilidad de Cierre →', fontsize=9, color='#6B7280')
        ax.set_ylabel('Impacto Financiero →', fontsize=9, color='#6B7280')
