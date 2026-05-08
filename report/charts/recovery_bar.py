import matplotlib.ticker as mticker

from report.charts.base import BaseChart
from report.i18n import opp_name


class RecoveryBarChart(BaseChart):
    def render(self):
        ax = self.ax
        self.style_axis(ax)
        ax.xaxis.set_major_formatter(
            mticker.FuncFormatter(
                lambda x, _: f"${x/1e6:.1f}M" if x >= 1e6 else f"${x/1e3:.0f}K"
            )
        )

        # data es un dict {type: value}
        types_sorted = sorted(self.data.items(), key=lambda x: x[1], reverse=True)

        if not types_sorted:
            ax.text(0.5, 0.5, "Sin valor recuperable", ha="center", va="center", color="#6B7280")
            ax.set_axis_off()
            return

        labels = [opp_name(item[0]) for item in types_sorted]
        values = [item[1] for item in types_sorted]
        colors = [self.PALETTE.get(item[0], '#6B7280') for item in types_sorted]

        bars = ax.barh(labels, values, color=colors, height=0.6)

        for bar, val in zip(bars, values):
            ax.text(
                bar.get_width() + max(values) * 0.02,
                bar.get_y() + bar.get_height() / 2,
                f'${val:,.0f}',
                va='center', fontsize=9, color='#1A1A2E',
            )
