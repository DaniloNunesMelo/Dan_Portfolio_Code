"""Spark job entrypoint for SparkSubmitOperator.

This thin wrapper ensures ``e3_contracts_to_transactions`` is importable when
spark-submit launches this script in a new Python process, then delegates
entirely to the existing ``main()`` entry point.

Usage (via SparkSubmitOperator / spark-submit directly):
    spark-submit jobs/e3_pipeline.py \\
        --contracts /data/contracts.csv \\
        --claims    /data/claims.csv    \\
        --output    /output/TRANSACTIONS.csv \\
        --config    /config/parameters.yaml
"""

from __future__ import annotations

import sys
from pathlib import Path

# Insert the project root so the package is importable whether spark-submit
# is invoked from the project root or from any other working directory.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from e3_contracts_to_transactions.main import main  # noqa: E402

if __name__ == "__main__":
    main()
