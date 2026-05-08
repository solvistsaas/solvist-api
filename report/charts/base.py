import io
import base64
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend, required for server-side
import matplotlib.pyplot as plt

from report.i18n import t, opp_name  # noqa: F401 — re-exported for chart subclasses

# Fuente consistente en todos los charts generados server-side
plt.rcParams.update({'font.family': 'DejaVu Sans'})


class BaseChart:
    """Todos los charts heredan esta clase."""

    FIGURE_SIZE = (7.5, 3.5)   # Pulgadas, ancho de columna
    DPI = 200                   # Print resolution
    BG_COLOR = 'none'           # Transparente

    PALETTE = {
        'battery_upgrade': '#4A6FA5',
        'peak_shaving': '#7C9885',
        'maintenance': '#B8A9C9',
        'ev_charger': '#E6B566',
        'inverter_replacement': '#D4836D',
        'system_expansion': '#8DB580',
        'vpp_monetization': '#9BB7D4',
        'tropical_degradation': '#C4A77D',
    }

    MAX_LABELS = 5  # Máximo de labels visibles en cualquier chart

    def __init__(self, data, hide_axes=False):
        self.data = data
        self.hide_axes = hide_axes
        self.fig, self.ax = plt.subplots(figsize=self.FIGURE_SIZE)

    def style_axis(self, ax):
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['left'].set_color('#D1D5DB')
        ax.spines['bottom'].set_color('#D1D5DB')
        ax.tick_params(colors='#6B7280', labelsize=8)
        ax.grid(axis='y', color='#F3F4F6', linewidth=0.5)
        ax.grid(axis='x', visible=False)

    def render(self):
        """Override en cada chart. Renderiza el chart en self.ax."""
        raise NotImplementedError

    def to_base64(self) -> str:
        """Convierte chart a PNG base64 para embeber en HTML."""
        self.render()
        buf = io.BytesIO()
        self.fig.savefig(buf, format='png', dpi=self.DPI,
                        bbox_inches='tight', transparent=True,
                        pad_inches=0.2)
        plt.close(self.fig)
        buf.seek(0)
        return base64.b64encode(buf.read()).decode('utf-8')
