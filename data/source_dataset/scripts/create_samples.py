from pathlib import Path
import pandas as pd
root=Path(__file__).resolve().parents[1]; out=root/'samples'; out.mkdir(exist_ok=True)
for p in sorted((root/'data').glob('*.csv')):
    pd.read_csv(p,nrows=25).to_csv(out/f'{p.stem}_sample.csv',index=False)
print('Created 25-row sample for each table in samples/')
