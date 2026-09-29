#!/usr/bin/env python3
"""Strict streaming validator for demand forecast split files."""
from pathlib import Path
import csv, json
from datetime import date
ROOT=Path(__file__).resolve().parents[1]; DIR=ROOT/'splits'/'demand_forecast'
EXPECTED=['Date','Item_ID','Location_ID','Is_Available','Target_Quantity']
def inspect(path):
    with path.open('rb') as raw:
        if b'\x00' in raw.read(1024*1024): raise ValueError(f'{path.name}: NUL bytes in header region')
    first=None; last=None; previous=None; n=0
    with path.open('r',encoding='utf-8',newline='') as f:
        reader=csv.reader(f); header=next(reader)
        if header!=EXPECTED: raise ValueError(f'{path.name}: header {header!r} != {EXPECTED!r}')
        for lineno,row in enumerate(reader,2):
            if len(row)!=len(EXPECTED): raise ValueError(f'{path.name}:{lineno}: expected 5 fields, got {len(row)}')
            d=date.fromisoformat(row[0]); key=(d,int(row[1]),int(row[2]))
            if previous is not None and key<=previous: raise ValueError(f'{path.name}:{lineno}: duplicate or unsorted date/item/location key')
            if int(row[3]) not in (0,1) or int(row[4])<0: raise ValueError(f'{path.name}:{lineno}: invalid availability/target')
            previous=key; first=first or d; last=d; n+=1
    if not n: raise ValueError(f'{path.name}: empty split')
    return first,last,n
def main():
    meta=json.loads((DIR/'split_metadata.json').read_text(encoding='utf-8')); b=meta['Boundaries']; names=['train','validation','test']
    got={name:inspect(DIR/f'{name}.csv') for name in names}
    expected=[(date.fromisoformat(meta['Date_Range'][0]),date.fromisoformat(b['train_end'])),
              (date.fromisoformat(b['validation_start']),date.fromisoformat(b['validation_end'])),
              (date.fromisoformat(b['test_start']),date.fromisoformat(meta['Date_Range'][1]))]
    for name,actual,bound in zip(names,got.values(),expected):
        if actual[:2]!=bound: raise ValueError(f'{name}: actual boundaries {actual[:2]} != metadata {bound}')
    if not (got['train'][1]<got['validation'][0] and got['validation'][1]<got['test'][0]): raise ValueError('date windows overlap')
    print('Train/validation/test CSV schema, row widths, NUL bytes, keys, ranges, and chronology: PASS')
    for name,(lo,hi,n) in got.items(): print(f'{name}: {n:,} rows; {lo} to {hi}')
    print('STATUS: PASS')
if __name__=='__main__':
    try: main()
    except Exception as e: print(f'STATUS: FAIL — {e}'); raise SystemExit(2)
