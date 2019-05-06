from yape.main import fileout, fileout_splitcols, parse_args, yape2
from yape.parsepbuttons import parsepbuttons

from pathlib import Path
import traceback
import logging

TEST_DIR = Path("testdata")
TEST_RESULTS = Path("testresults")
# just to understand how tests work
class TestParser:
    def test_is_string(self):
        s = "this is a test"
        assert isinstance(s, str)

    def test_args_parse(self):
        params = ["--filedb", "some.db", "some.html"]
        args = parse_args(params)
        assert args.filedb == Path("some.db")
        assert args.pButtons_file_name == Path("some.html")
        params = ["-q", "-a", "some.html"]
        args = parse_args(params)
        assert args.quiet
        assert args.all
        params = [
            "--mgstat",
            "--vmstat",
            "--iostat",
            "--sard",
            "--monitor_disk",
            "--perfmon",
            "some.html",
        ]
        args = parse_args(params)
        assert args.graphmgstat
        assert args.graphvmstat
        assert args.graphiostat
        assert args.graphsard
        assert args.monitor_disk
        assert args.graphperfmon

    # pretty much a full stack test of parsing and plotting for all pbuttons in the testdata dir
    def test_db_parse(self):
        testingcfg = TEST_DIR / "config.test.yml"
        #onlyfiles = [ x for x in TEST_DIR.iterdir() if x.is_file() ]
        #for file in onlyfiles:
        for file in TEST_DIR.glob('**/*.html'):
            logging.debug("Working on file:" + file)
            #if not file.suffix == "html":
            #    continue
            logging.debug(file)
            basename = file.name
            db_file = basename.with_suffix('.db')
            params = [
                "-o",
                TEST_RESULTS / basename,
                "-a",
                "--config",
                testingcfg,
                "--filedb",
                db_file,
                file,
            ]
            logging.debug(params)
            args = parse_args(params)
            try:
                yape2(args)
            except:
                logging.debug("error while parsing:" + file)
                logging.debug(traceback.format_exc())
                assert False, "exception while parsing: " + file

