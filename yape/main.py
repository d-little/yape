import argparse

import sys
import csv

import sqlite3

import logging
import tempfile
import zipfile
import yaml
from pathlib import Path

from yape.parsepbuttons import parsepbuttons
from yape.plotpbuttons import mgstat, vmstat, iostat, perfmon, sard, monitor_disk, saru
from pkg_resources import get_distribution, DistributionNotFound


def getVersion():
    v = ""
    try:
        v = get_distribution("yape").version
    except DistributionNotFound:
        v = ""
    pass
    return v

def read_config(yamlfile:Path=None, config:set=None) -> set:
    ''' Returns an updated config from provided yaml file '''
    if yamlfile is None:
        yamlfile = Path(Path.home() / "yape.yml")
    if yamlfile.is_file():
        with open(yamlfile, "r") as ymlfile:
            cfg = yaml.load(ymlfile)
            logging.debug(cfg)
            config = config.update(cfg)
    else:
        logging.debug('No additional yaml configuration found.')
    return config

def fileout(db, config:{}, section) -> None:
    fileprefix = config["fileprefix"]
    basefilename = config["basefilename"]
    c = db.cursor()
    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", [section])
    if len(c.fetchall()) == 0:
        return None
    file = Path(basefilename, fileprefix + section + ".csv")
    print("exporting " + section + " to " + file)
    c.execute('select * from "' + section + '"')
    columns = [i[0] for i in c.description]

    with open(file, "w") as f:
        csvWriter = csv.writer(f)
        csvWriter.writerow(columns)
        csvWriter.writerows(c)
    return None


def ensure_dir(file_path: Path) -> None:
    ''' Ensures that the full file structure of the incoming file_path exists. If it does not, it is created. '''
    directory = file_path.parent
    #There's not really a need to check if it exists, just try create it.
    directory.mkdir(parents=True, exist_ok=True)

def fileout_splitcols(db, config:{}, section, split_on) -> None:
    fileprefix = config["fileprefix"]
    basefilename = Path(config["basefilename"])
    c = db.cursor()
    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", [section])
    if len(c.fetchall()) == 0:
        return None
    c.execute("select distinct " + split_on + ' from "' + section + '"')
    rows = c.fetchall()
    for column in rows:
        c.execute(
            'select * from "' + section + '" where ' + split_on + "=?", [column[0]]
        )
        file = Path(basefilename / fileprefix / section)
        file = file.with_suffix(column[0] + ".csv")
        print("exporting " + section + "-" + column[0] + " to " + file)
        columns = [i[0] for i in c.description]
        with open(file, "w") as f:
            csvWriter = csv.writer(f)
            csvWriter.writerow(columns)
            csvWriter.writerows(c)


def parse_args(args):
    parser = argparse.ArgumentParser(description="Yape")
    parser.add_argument(
        "-v",
        "--version",
        dest="version",
        help="display version information",
        action="version",
        version="%(prog)s " + getVersion(),
    )
    parser.add_argument(
        "pButtons_file_name", 
        type=Path,
        help="path to pButtons file to use"
    )
    parser.add_argument(
        "--filedb",
        type=Path,
        help="use specific file as DB, useful to be able to used afterwards or as standalone datasource.",
    )
    parser.add_argument(
        "--skip-parse",
        dest="skipparse",
        help="disable parsing; requires filedb to be specified to supply data",
        action="store_true",
    )
    parser.add_argument(
        "-c",
        dest="csv",
        help="will output the parsed tables as csv files. useful for further processing. will currently create: mgstat, vmstat, sar-u. sar-d and iostat will be output per device",
        action="store_true",
    )
    parser.add_argument(
        "--mgstat", dest="graphmgstat", help="plot mgstat data", action="store_true"
    )
    parser.add_argument(
        "--vmstat", dest="graphvmstat", help="plot vmstat data", action="store_true"
    )
    parser.add_argument(
        "--iostat", dest="graphiostat", help="plot iostat data", action="store_true"
    )
    parser.add_argument(
        "--sard", dest="graphsard", help="plot sar-d data", action="store_true"
    )
    parser.add_argument(
        "--saru", dest="graphsaru", help="plot sar-u data", action="store_true"
    )
    parser.add_argument(
        "--monitor_disk",
        dest="monitor_disk",
        help="plot disk data from monitor (vms)",
        action="store_true",
    )
    parser.add_argument(
        "--perfmon", dest="graphperfmon", help="plot perfmon data", action="store_true"
    )
    parser.add_argument(
        "--timeframe",
        dest="timeframe",
        help='specify a timeframe for the plots, i.e. --timeframe "2018-05-16 00:01:16,2018-05-16 17:04:15"',
    )
    parser.add_argument(
        "--prefix",
        dest="prefix",
        help="specify output file prefix (this is for the filename itself, to specify a directory, use -o)",
    )
    parser.add_argument(
        "--plotDisks", dest="plotDisks", help="restrict list of disks to plot"
    )

    parser.add_argument(
        "--log",
        dest="loglevel",
        help="set log level:DEBUG,INFO,WARNING,ERROR,CRITICAL. The default is INFO",
    )

    parser.add_argument(
        "-a", "--all", dest="all", help="graph everything", action="store_true"
    )
    parser.add_argument(
        "-q", "--quiet", dest="quiet", help="no stdout output", action="store_true"
    )
    parser.add_argument(
        "-o",
        "--out",
        dest="out",
        type=Path,
        help="specify base output directory, defaulting to <pbuttons_name>/",
    )
    parser.add_argument(
        "--config",
        dest="configfile",
        help="specify the location of a config file. ~/.yape.yml is used by default.",
    )
    return parser.parse_args(args)

def is_compressed(file: Path) -> bool:
    ''' Return True if file is a supported compressed file, else False '''
    # I would like to replace this with python_magic in the future, check the magic number instead.
    valid_suffix = [ '.zip', '.gz' ]
    filetype = file.suffix
    if filetype not in set(valid_suffix):
        # Not compressed, but exists
        return False
    return True

def decompress(compressedfile:Path, destination:Path) -> bool:
    ''' Decompresses the given compressedfile and stores files in destination. 
    Returns True if file successfully decompressed, else False
    eg: 
      - /path/to/pbuttons.zip returns: /temporary/path/pbuttons.html
      - c:\\mydir\\pbuttons.html.gz returns: c:\\temporary\\path\\pbuttons.html
      - c:\\mydir\\uncompressed_pbuttons.html returns c:\\mydir\\uncompressed_pbuttons.html
    '''
    try:
        if not compressedfile.exists:
            raise ValueError("Passed compressedfile does not exist: {}", compressedfile)
        filetype = compressedfile.suffix
        if filetype == '.zip':
            with open(compressedfile, "rb") as f:
                zf = zipfile.ZipFile(f)
                zf.extractall(destination)
        elif filetype == '.gz':
            import gzip ## move to top of script, here for now for testing
            import shutil ## ^^^
            # We dont want to uncompress the pbuttons in memory, stream it out.
            # https://codereview.stackexchange.com/questions/156005/improving-gzip-function-for-huge-files
            tgtpath = destination / Path(compressedfile.stem)
            with gzip.open(compressedfile, 'rb') as f_in, open(tgtpath, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        else:
            raise Exception('Unhandled compressed filetype.  This should not occur.')
    except OSError as e:
        sys.exit("Could not process compressed pButtons file because: {}".format(str(e)))
    return True

def yape2(args=None):
    if args == None:
        args = parse_args(sys.argv[1:])
    try:
        if args.loglevel is not None:
            loglevel = getattr(logging, args.loglevel.upper(), None)
            if not isinstance(loglevel, int):
                raise ValueError("Invalid log level: %s" % args.loglevel)
            logging.basicConfig(level=loglevel)
        else:
            logging.basicConfig(level=getattr(logging, "INFO", None))
        if args.quiet:
            logger = logging.getLogger()
            logger.disabled = True
        if args.skipparse:
            if args.filedb is None:
                logging.error("filedb required with skip-parse set")
                return -1
        if args.filedb is not None:
            db = sqlite3.connect(args.filedb)
        else:
            db = sqlite3.connect(":memory:")
            db.execute("pragma journal_mode=wal")
            db.execute("pragma synchronous=0")

        if args.prefix is not None:
            fileprefix = args.prefix
        else:
            fileprefix = ""

        if not args.skipparse:
            pButtons_file = args.pButtons_file_name
            if is_compressed(pButtons_file):
                # If the file is compressed, it's unrealistic to assume we wil have enough memory to
                #  hold the extracted pbuttons file. So we extract it to a temp directory and work on it there
                with tempfile.TemporaryDirectory(prefix="yape_") as dest:
                    destination = Path(dest)
                    decompress(pButtons_file, destination)
                    # Find the HTML file in destination
                    htmlfiles = list(destination.rglob("*.html"))
                    # We could check len(htmlfiles) here, if it's > 1, we've extracted more than 1 html file.
                    # For now, just use the first one in the list
                    htmlfile = htmlfiles[0]
                    parsepbuttons(htmlfile, db)
            elif pButtons_file.suffix == ".html":
                parsepbuttons(pButtons_file, db)
            else:
                raise Exception('Unhandled compressed filetype.  This should not occur.')

        if args.out is not None:
            basefilename = args.out
        else:
            basefilename = args.pButtons_file_name.name

        if args.plotDisks is not None:
            plotDisks = args.plotDisks
        else:
            plotDisks = ""

        # a place to hold global configurations/settings
        # makes it easier to extend functionality to carry
        # command line parameters to subfunctions...
        config = {}
        config["quiet"] = args.quiet

        # doing config file read here, because we want the quiet flag to be overwritable in the config
        # but not the below config settings
        config = read_config(args.configfile, config)
        logging.debug(config)
        config["fileprefix"] = fileprefix
        config["plotDisks"] = plotDisks
        config["timeframe"] = args.timeframe
        config["basefilename"] = basefilename

        if args.csv:
            ensure_dir(basefilename)
            fileout(db, config, "mgstat")
            fileout(db, config, "vmstat")
            fileout_splitcols(db, config, "iostat", "Device")
            fileout_splitcols(db, config, "sar-d", "DEV")
            fileout(db, config, "perfmon")
            fileout(db, config, "sar-u")

        # plotting
        if args.graphsard or args.all:
            ensure_dir(basefilename)
            sard(db, config)

        if args.graphsaru or args.all:
            ensure_dir(basefilename)
            saru(db, config)

        if args.graphmgstat or args.all:
            ensure_dir(basefilename)
            mgstat(db, config)

        if args.graphvmstat or args.all:
            ensure_dir(basefilename)
            vmstat(db, config)

        if args.monitor_disk or args.all:
            ensure_dir(basefilename)
            monitor_disk(db, config)

        if args.graphiostat or args.all:
            ensure_dir(basefilename)
            iostat(db, config)

        if args.graphperfmon or args.all:
            ensure_dir(basefilename)
            perfmon(db, config)

    except OSError as e:
        print("Could not process pButtons file because: {}".format(str(e)))


if __name__ == "__main__":
    # execute only if run as the entry point into the program
    yape2()
