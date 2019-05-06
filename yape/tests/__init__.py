from pathlib import Path
import shutil

TEST_RESULTS = Path("testresults")

def setup(self):
    if TEST_RESULTS.exists():
        shutil.rmtree(TEST_RESULTS)
    TEST_RESULTS.mkdir(parents=True, exist_ok=True)
