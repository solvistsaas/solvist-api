from report.charts.base import BaseChart


class OpportunityMatrixChart(BaseChart):
    def render(self):
        ax = self.ax
        self.style_axis(ax)

        if not self.data:
            ax.text(0.5, 0.5, "No opportunities", ha="center", va="center", color="#6B7280")
            ax.set_axis_off()
            return

        # Ordenar por valor para determinar cuales llevan label
        sorted_data = sorted(self.data, key=lambda x: x['expected_value'], reverse=True)
        top_5_names = {s['_system']['name'] for s in sorted_data[:self.MAX_LABELS]}

        for opp in self.data:
            color = self.PALETTE.get(opp['opportunity_reason'], '#6B7280')
            ax.scatter(
                1 - opp['close_probability'],  # effort (inverted)
                opp['expected_value'],           # impact
                s=max(opp['expected_value'] / 500, 30),  # size, min 30pt
                c=color,
                alpha=0.7,
                edgecolors='white',
                linewidth=0.5,
            )
            # Solo label para top 5
            if opp['_system']['name'] in top_5_names:
                ax.annotate(
                    opp['_system']['name'],
                    (1 - opp['close_probability'], opp['expected_value']),
                    fontsize=7,
                    color='#374151',
                    ha='center',
                    va='bottom',
                )

        ax.set_xlabel('Ease of Close ->', fontsize=9, color='#6B7280')
        ax.set_ylabel('Financial Impact ->', fontsize=9, color='#6B7280')
