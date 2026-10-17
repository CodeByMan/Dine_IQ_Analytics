import os, sys, json
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "data_generator"))
import validate_dataset

def test_all_validation_flags_pass():
    assert validate_dataset.main() == 0
    rep = json.load(open(os.path.join(ROOT, "reports", "validation_report.json")))
    assert all(v["status"] == "PASS" for v in rep["flags"].values())
