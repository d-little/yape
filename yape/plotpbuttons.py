import pandas as pd
import numpy
import sqlite3
import matplotlib

matplotlib.use("Agg")
import matplotlib.colors as colors
import matplotlib.dates as mdates
from matplotlib.dates import (
    DayLocator,
    HourLocator,
    DateFormatter,
    drange,
    IndexDateFormatter,
)
from matplotlib.ticker import FormatStrFormatter
from matplotlib.ticker import ScalarFormatter
from matplotlib.ticker import AutoMinorLocator
import pytz
import re
from pathlib import Path
import mpl_toolkits.mplot3d
import matplotlib.pyplot as plt
from datetime import datetime
import logging


def dispatch_plot(df, column, outfile, config):
    genericplot(df, column, outfile, config)


def parse_tuple(string):
    try:
        s = eval(string)
        if type(s) == tuple:
            return s
        return
    except:
        return


def genericplot(df, column, outfile, config):
    timeframe = config["timeframe"]
    outfile = outfile.replace(":", ".")
    logging.info("creating " + outfile)
    dim = (16, 6)
    markersize = 1
    style = "-"
    try:
        dim = parse_tuple("(" + config["plotting"]["dim"] + ")")
    except KeyError:
        pass
    try:
        markersize = float(config["plotting"]["markersize"])
    except KeyError:
        pass
    try:
        style = config["plotting"]["style"]
    except KeyError:
        pass
    fig, ax = plt.subplots(figsize=dim, dpi=80, facecolor="w", edgecolor="dimgrey")

    if timeframe is not None and timeframe != "":
        ax.xaxis.set_minor_locator(AutoMinorLocator(n=20))
    else:
        ax.xaxis.set_minor_locator(mdates.HourLocator())
    ax.xaxis.set_major_locator(mdates.DayLocator())
    ax.xaxis.set_minor_formatter(mdates.DateFormatter("%H:%M:%S"))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("\n\n %Y-%m-%d"))

    ax.get_yaxis().set_major_formatter(
        plt.FuncFormatter(lambda x, loc: "{:,}".format(float(x)))
    )
    if timeframe is not None:
        df[column][timeframe.split(",")[0] : timeframe.split(",")[1]].plot(ax=ax)
    else:
        df[column].plot(ax=ax, style=style, markersize=markersize)

    ax.set_ylim(ymin=0)  # Always zero start
    # ax.set_ylim(ymax=0.005)
    # ax.yaxis.set_major_formatter(FormatStrFormatter('%.4g'))
    ax.yaxis.set_major_formatter(ScalarFormatter(useOffset=None))
    ax.get_yaxis().get_major_formatter().set_scientific(False)
    plt.grid(which="both", axis="both")
    plt.title(column, fontsize=10)
    plt.xlabel("Time", fontsize=10)
    plt.tick_params(labelsize=8)
    if timeframe != "":
        plt.setp(ax.xaxis.get_minorticklabels(), rotation=70)

    plt.savefig(outfile, bbox_inches="tight")

    plt.close()


# need this as utility, since pandas timestamps are not compaitble with sqlite3 timestamps
# there's a possible other solution by using using converters in sqlite, but I haven't explored that yet
def fix_index(df):
    df.index = pd.to_datetime(df["datetime"])
    df = df.drop(["datetime"], axis=1)
    df.index.name = "datetime"
    return df


def plot_subset_split(db, config, subsetname, split_on):
    fileprefix = config["fileprefix"]
    timeframe = config["timeframe"]
    basename = config["basefilename"]
    plotDisks = config["plotDisks"]

    if not check_data(db, subsetname):
        return None
    c = db.cursor()
    c.execute("select distinct " + split_on + ' from "' + subsetname + '"')
    rows = c.fetchall()
    for column in rows:
        # If specified only plot selected disks for iostat - saves time and space
        if len(plotDisks) > 0 and subsetname == "iostat" and column[0] not in plotDisks:
            logging.info("Skipping plot subsection: " + column[0])
        else:
            logging.info("Including plot subsection: " + column[0])
            c.execute(
                'select * from "' + subsetname + '" where ' + split_on + "=?",
                [column[0]],
            )
            data = pd.read_sql_query(
                'select * from "'
                + subsetname
                + '" where '
                + split_on
                + '="'
                + column[0]
                + '"',
                db,
            )
            if len(data["datetime"][0].split()) == 1:
                # another evil hack for iostat on some redhats (no complete timestamps)
                # the datetime field only has '09/13/18' instead of '09/13/18 14:39:49'
                # -> take timestamps from mgstat
                data = data.drop("datetime", axis=1)
                size = data.shape[0]
                # one of those evil OS without datetime in vmstat
                # evil hack: take index from mgstat (we should have that in every pbuttons) and map that
                # is going to horribly fail when the number of rows doesn't add up ---> TODO for later
                dcolumn = pd.read_sql_query("select datetime from mgstat", db)
                ##since mgstat has only one entry per timestamp, but iostat has one entry per timestamp per device
                ##we need to duplicate the rows appropriately which is data.shape[0]/dcolumn.shape[0]) times
                # dcolumn=dcolumn.loc[dcolumn.index.repeat(size/dcolumn.shape[0])].reset_index(drop=True)

                data.index = pd.to_datetime(dcolumn["datetime"][:size])
                data.index.name = "datetime"
            else:
                data = fix_index(data)
            data = data.drop([split_on], axis=1)
            for key in data.columns.values:
                if timeframe is not None:
                    file = Path(
                        basename /
                        fileprefix /
                        subsetname /
                        "." /
                        column[0] /
                        "." /
                        key.replace("/", "_") /
                        "." /
                        timeframe /
                        ".png"
                    )
                else:
                    file = Path(
                        basename /
                        fileprefix /
                        subsetname /
                        "." /
                        column[0] /
                        "." /
                        key.replace("/", "_") /
                        ".png"
                    )
                dispatch_plot(data, key, file, config)


def plot_subset(db, config, subsetname):
    fileprefix = config["fileprefix"]
    timeframe = config["timeframe"]
    basename = config["basefilename"]
    if not check_data(db, subsetname):
        return None
    data = pd.read_sql_query('select * from "' + subsetname + '"', db)
    if "datetime" not in data.columns.values:
        size = data.shape[0]
        # one of those evil OS without datetime in vmstat
        # evil hack: take index from mgstat (we should have that in every pbuttons) and map that
        # is going to horribly fail when the number of rows doesn't add up ---> TODO for later
        dcolumn = pd.read_sql_query("select datetime from mgstat", db)
        data.index = pd.to_datetime(dcolumn["datetime"][:size])
        data.index.name = "datetime"
    else:
        data = fix_index(data)
    for key in data.columns.values:

        if timeframe is not None:
            file = Path(
                basename /
                fileprefix /
                subsetname /
                "." /
                key.replace("\\", "_").replace("/", "_") /
                "." /
                timeframe /
                ".png".replace("%", "_")
            )
        else:
            file = Path(
                basename /
                fileprefix /
                subsetname /
                "." /
                key.replace("\\", "_").replace("/", "_") /
                ".png".replace("%", "_")
            )
        dispatch_plot(data, key, file, config)


def check_data(db, name):
    cur = db.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", [name])
    if len(cur.fetchall()) == 0:
        logging.warning("no data for:" + name)
        return False
    return True


def mgstat(db, config):
    plot_subset(db, config, "mgstat")


def perfmon(db, config):
    plot_subset(db, config, "perfmon")


def vmstat(db, config):
    plot_subset(db, config, "vmstat")


def iostat(db, config):
    plot_subset_split(db, config, "iostat", "Device")


def monitor_disk(db, config):
    plot_subset_split(db, config, "monitor_disk", "device")


def sard(db, config):
    plot_subset_split(db, config, "sard", "device")


def saru(db, config):
    plot_subset_split(db, config, "sar-u", "cpu")
