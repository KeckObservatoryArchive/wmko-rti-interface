from bokeh.models import ColumnDataSource, FactorRange
from bokeh.transform import factor_cmap
from bokeh.plotting import figure
from bokeh.palettes import Category10, Category20

from bokeh.io import output_file, show
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

    def __init__(self, results, title):
        """
        Plot with all integer values between 0 and the largest time value
        in the results.

        :param results: (dict) the database query results
        :param title: (str) the plot title
        """

        self.data = {}
        self.results = results
        self.title = title
        self.max_xrange = 240
        self.last_val = 0

        self.insts = list(results.keys())
        self.n_insts = len(self.insts)

        super().__init__(self.insts)
        if self.n_insts != 0:
            cds = self.get_columndata()
            self.create_plot(cds)

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

    def create_plot(self, cds):
        """
        Create a plot of the number of files with a given time.

        :param cds: columndata source of the data
        """
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
        plt.xaxis.axis_label = "Time (s)"
        plt.yaxis.axis_label = "Number of Files"

        self.plt = plt