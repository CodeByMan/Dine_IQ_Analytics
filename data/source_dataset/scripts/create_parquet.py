from pathlib import Path
import pandas as pd
try: import pyarrow
except ImportError as e: raise SystemExit('Install pyarrow: python -m pip install pyarrow') from e
root=Path(__file__).resolve().parents[1]; src=root/'data'; out=root/'parquet'; out.mkdir(exist_ok=True)
for p in src.glob('*.csv'):
    pd.read_csv(p).to_parquet(out/f'{p.stem}.parquet',index=False,compression='snappy')
    print('wrote raw table',p.stem)
# DS-EX-05 requires a large processed Parquet artifact, not only raw-table mirrors.
processed=root/'processed_data'
for p in processed.glob('*.csv'):
    pd.read_csv(p).to_parquet(p.with_suffix('.parquet'),index=False,compression='snappy')
    print('wrote processed dataset',p.stem)
