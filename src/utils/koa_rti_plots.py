from datetime import timedelta

from bokeh.models import ColumnDataSource, FactorRange
from bokeh.transform import factor_cmap
from bokeh.palettes import Category10, Category20

from bokeh.models import ColumnDataSource
from bokeh.plotting import figure
from bokeh.transform import dodge


class PlotBase:

    def __init__(self, insts):
        """
        Base class for general plot functions

        :param insts: (list) a list of the instruments in the plot
        """
        self.plt = None
        self.plt_ht = 500
        self.plt_width = 900
        self.colors = self.color_palette(len(insts))

    def color_palette(self, num):
        """
        Determine the colors to be used by the different instruments

        :param num: The number of instruments / colors

        :return: (list) palette of colors
        """
        if num < 3:
            return Category10[10][:num]
        elif num <= 10:
            return Category10[num]
        else:
            return Category20[num]

    def get_plot(self):
        """
        return the portions required for the bokeh plots

        :return: (plot) to be embedded into html
        """
        from bokeh.embed import file_html
        from bokeh.resources import CDN
        if self.plt:
            return file_html(self.plt, CDN, "TimePlot")

        return None


class TimeBarPlot(PlotBase):

    def __init__(self, results, title):
        """
        Categorical bar plot.

        :param results: (dict / lists) the database results
        :param title: (str) the plot title
        """
        self.results = results
        self.title = title

        self.insts = list(results.keys())
        super().__init__(self.insts)

        cds, x_data = self.get_columndata()
        self.create_plot(cds, x_data)

    def get_columndata(self):
        time_set = set()
        for inst in self.insts:
            time_set |= set(self.results[inst])

        data = {}
        for inst in self.insts:
            sums_dict = self.results[inst]
            data[inst] = []
            for key in time_set:
                if key in sums_dict:
                    data[inst].append(sums_dict[key])
                else:
                    data[inst].append(0)

        data['time_set'] = self.insts

        x_data = [(str(tm), inst) for tm in time_set for inst in self.insts]
        num = ()
        for inst in self.insts:
            num += tuple(data[inst])

        return ColumnDataSource(data=dict(x=x_data, num=num)), x_data

    def create_plot(self, cds, x_data):
        plt = figure(x_range=FactorRange(*x_data), plot_height=self.plt_ht,
                     plot_width=self.plt_width, title=self.title,
                     tooltips="$x: @$num%",
                     tools="pan,box_zoom,reset,hover")

        plt.vbar(x='x', top='num', width=0.9, source=cds, line_color="white",
                 fill_color=factor_cmap('x', palette=self.colors,
                                        factors=self.insts, start=1, end=2))

        plt.y_range.start = 0
        plt.x_range.range_padding = 0.1
        plt.xaxis.major_label_orientation = 1

        self.plt = plt


class OverlayTimePlot(PlotBase):

    def __init__(self, results, title, xrange=240, units=None):
        """
        Plot with all integer values between 0 and the largest time value
        in the results.

        :param results: (dict) the database query results
        :param title: (str) the plot title
        """

        self.data = {}
        self.results = results
        self.title = title
        self.max_xrange = xrange
        self.last_val = 0

        self.insts = list(results.keys())
        self.n_insts = len(self.insts)

        super().__init__(self.insts)
        if self.n_insts != 0:
            cds = self.get_columndata()
            self.create_plot(cds, units=units)

    def get_columndata(self):
        """
        Create a columndata source to be used with the Bokeh plots

        :return:
        """
        time_set = set()
        for inst in self.insts:
            time_set |= set(self.results[inst])
            self.last_val = max(self.last_val, max(self.results[inst], key=int))

        self.last_val += 1

        self.data = {}
        for inst in self.insts:
            sums_dict = self.results[inst]
            self.data[inst] = []
            for i in range(0, self.last_val):
                if i in sums_dict:
                    self.data[inst].append(sums_dict[i])
                else:
                    self.data[inst].append(0)

        self.data['time_set'] = list(range(0, self.last_val))

        return ColumnDataSource(data=self.data)

    def define_tooltip(self):
        tool_tips = []
        for i in range(0, self.n_insts):
            inst_name = self.insts[i]
            inst_tup = (inst_name, "@" + inst_name)
            tool_tips.append(inst_tup)

        tool_tips.append(("TIME ", "@time_set (s)"))

        return tool_tips

    def create_plot(self, cds, units=None):
        """
        Create a plot of the number of files with a given time.

        :param cds: columndata source of the data
        """
        if not units:
            units = 's'
        self.max_xrange = int(min(self.last_val, self.max_xrange))
        tool_tips = self.define_tooltip()

        plt = figure(x_range=(0, self.max_xrange), plot_height=self.plt_ht,
                     plot_width=self.plt_width, title=self.title,
                     tooltips=tool_tips,
                     tools="pan,box_zoom,xwheel_zoom,reset,hover")

        bar_width = 1.0 / self.n_insts

        for i in range(0, self.n_insts):
            offset = bar_width * (i % self.n_insts)
            plt.vbar(x=dodge('time_set', offset, range=plt.x_range),
                     top=self.insts[i], width=bar_width, source=cds,
                     color=self.colors[i], legend_label=self.insts[i])

        if self.max_xrange > 10:
            tick_incr = int(self.max_xrange / 10)
        else:
            tick_incr = 1
        ticks = list(range(0,self.last_val+tick_incr, tick_incr))
        plt.xaxis.ticker = ticks
        plt.xaxis.axis_label = f"Time ({units})"
        plt.yaxis.axis_label = "Number of Files"

        self.plt = plt


class TimeBudgetPie(PlotBase):
    def __init__(self, results):
        """

        """
        self.name = title
        self.insts = list(results.keys())
        super().__init__(self.insts)

        times = self.read_times()

        data = pd.Series(times).reset_index(name='time').rename(columns={'index': 'typ'})
        data['angle'] = data['time'] / data['time'].sum() * 2 * pi
        data['percentage'] = data['time'] / data['time'].sum() * 100.
        data['total_tm'] = data['time'].astype(str).str[7:15]
        data['color'] = self.set_colors(times)

        self.plt = figure(tools="pan,box_zoom,reset,save,hover", plot_width=900,
                     plot_height=600, title="Time Budget",
                     tooltips="@typ: @total_tm hrs, @percentage{0.2f}%")

        self.plt.wedge(x=0, y=1, radius=0.6, source=data, fill_color='color',
                       start_angle=cumsum('angle', include_zero=True),
                       end_angle=cumsum('angle'), line_color="white",
                       legend='typ')

        self.plt.axis.axis_label = None
        self.plt.axis.visible = False
        self.plt.grid.grid_line_color = None

    def set_colors(self, times):
        """
        Define the colors to be used.

        :param times: (dict) the times by category

        :return: (list) a list of colors to use.
        """
        if times:
            len_times = len(times)
        else:
            len_times = 1

        if times and len_times < 3:
            colors = []
            for i in range(0,len_times):
                colors.append(Category20[3][i])
            return colors
        elif times and len_times > 2:
            return Category20[len_times]
        else:
            return Category20[20]

    def read_times(self):
        """
        Determine the times for each category

        :return: (dict) the times by category
        """
        times = {}
        tallies = self._activity_tally()

        if not tallies:
            return None

        for typ, name in self.fields.items():
            time = self.get_time_total(tallies, typ)

            if time:
                if name in times:
                    times[name] += time
                else:
                    times[name] = time

        return times

    def get_time_total(self, tallies, typ):
        """
        Find the total time for the category

        :param tallies: (list / dict) the list of tallies by category
        :param typ: (str) the tally key for the category

        :return: (datetime) the total time
        """
        time = timedelta(hours=0)
        for tally in tallies:
            time += timedelta(seconds=tally[typ])

        return time


