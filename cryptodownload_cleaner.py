import pandas as pd
from pathlib import Path
import os
import argparse
import tqdm

parser = argparse.ArgumentParser()
parser.add_argument('input_dir')
parser.add_argument('output_dir')
args = parser.parse_args()
print(args.input_dir, args.output_dir)


raw_dir = Path(args.input_dir)
tmp_dir = Path('tmp')
output_dir = Path(args.output_dir)

tmp_dir.mkdir(exist_ok=True)
output_dir.mkdir(exist_ok=True, parents=True)

def remove_watermark(input_path, output_path):
    watermark = 'https://www.CryptoDataDownload.com'
    os.system(f'grep -v "{watermark}" {input_path} > {output_path}')


def drop_duplicate_unix_time(input_path, output_path):
    df = pd.read_csv(input_path)
    df['unix'] = df['unix'].astype(int)

    millisec_unix_df = df[df['unix'] >= 10**12]
    millisec_unix_df['time'] = pd.to_datetime(millisec_unix_df['unix'], unit='ms')

    sec_unix_df = df[df['unix'] < 10**12]
    sec_unix_df['time'] = pd.to_datetime(sec_unix_df['unix'], unit='s')

    df = pd.concat([millisec_unix_df, sec_unix_df])
    df = df.drop_duplicates(subset='time', keep='first')
    df.sort_values('time', ascending=False, inplace=True)
    df.drop(columns=['date', 'unix'], inplace=True)
    df = df[['time'] + list(df.columns.difference(['time']))]
    df.to_csv(output_path, index=False)

input_paths = raw_dir.glob('*.csv')
loop = tqdm.tqdm(input_paths)
for input_path in loop:
    loop.set_description(str(input_path))
    
    tmp_path = tmp_dir / input_path.name
    output_path = output_dir / input_path.name
    
    remove_watermark(input_path, tmp_path)
    drop_duplicate_unix_time(tmp_path, output_path)