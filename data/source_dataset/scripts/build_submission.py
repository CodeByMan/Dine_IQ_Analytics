#!/usr/bin/env python3
"""One-command reproducible build of all required dataset-phase artifacts."""
from pathlib import Path
import subprocess, sys
ROOT=Path(__file__).resolve().parents[1]
STEPS=['generate_dataset.py','complete_srs_requirements.py','remediate_dataset.py','finalize_dataset_docs.py','build_dictionary.py','build_realism_coverage.py','build_dataset_statistics.py','create_parquet.py','validate_srs_compliance.py']
for step in STEPS:
    print(f'==> {step}',flush=True)
    subprocess.run([sys.executable,str(ROOT/'scripts'/step)],cwd=ROOT,check=True)
print('FINAL BUILD STATUS: PASS')
