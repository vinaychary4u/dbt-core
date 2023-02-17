from unittest import TestCase
from pathlib import Path
import sys
import subprocess

class TestUnitTests(TestCase):
    """Runs each unit test in process-level isolation"""
    
    def test__test_unit_tests_work_in_isolation(self):
        test_dir = Path(__file__).parent
        all_test_files = test_dir.glob("**/*.py")
        skip_files = (
            __file__, 
            str(test_dir / "mock_adapter.py"),
            str(test_dir / "utils.py"),
            str(test_dir / "test_context.py"), # is.. trouble
            str(test_dir / "test_contracts_graph_parsed.py"), # is.. trouble
            str(test_dir / "test_parse_manifest.py"), # has no actual tests, just parent classes
        )
        test_files = [tf for tf in all_test_files if str(tf) not in skip_files] 
        for test_file in test_files:
            subprocess.run(f"{sys.executable} -m pytest {test_file}", shell=True, check=True)            

