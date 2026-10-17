from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterable, Mapping


TRACE_FIELDS = (
    "Requirement ID", "Requirement", "Required files", "Implementation", "Status",
    "Verification", "Evidence", "Remaining work",
)


def write_stage_traceability(
    project_root: Path,
    stage: str,
    requirements: Mapping[str, str],
    required_files: Iterable[str],
    *,
    verified: bool,
    verification: str,
    evidence: str,
    remaining_work: str = "",
) -> Path:
    """Persist exact SRS section references and stage-specific evidence.

    A row is marked VERIFIED only when the caller has observed a successful
    stage execution; merely listing a file cannot produce that status.
    """
    path = project_root / "documentation" / "requirement_traceability" / f"{stage}_traceability.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    files = ";".join(required_files)
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=TRACE_FIELDS)
        writer.writeheader()
        for reference, description in requirements.items():
            writer.writerow({
                "Requirement ID": reference,
                "Requirement": description,
                "Required files": files,
                "Implementation": f"{stage} pipeline stage",
                "Status": "VERIFIED" if verified else "IMPLEMENTED_UNVERIFIED",
                "Verification": verification,
                "Evidence": evidence,
                "Remaining work": remaining_work,
            })
    return path
