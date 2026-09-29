#!/usr/bin/env python3
"""Restore the complete SRS fixture/documentation inventory after remediation."""
from pathlib import Path
import runpy
ROOT=Path(__file__).resolve().parents[1]
namespace=runpy.run_path(str(ROOT/'scripts'/'complete_srs_requirements.py'))
namespace['create_quality_fixtures']()
namespace['create_promotion_trap_fixture']()
namespace['write_docs']()
print('PASS: isolated SRS quality/hidden-data fixtures and rules documentation finalized')
