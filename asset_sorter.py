#%%
from pathlib import Path
import pandas as pd
import tqdm
#%%
dataset_dir = Path('dataset/ftx')

assets = []
loop = tqdm.tqdm(sorted(dataset_dir.glob('*.csv')))
for asset_path in loop:
    df = pd.read_csv(asset_path)
    pair_name = asset_path.stem
    if pair_name[-4:] == 'USDT':
        base = pair_name[: -4]
        quote = 'USDT'
    elif pair_name[-3:] == 'USD':
        base = pair_name[: -3]
        quote = 'USD'
    average_volume = df['volume'].head(24*30).mean()
    
    assets.append({
        'base': base,
        'name': f'{base}/{quote}',
        'count': len(df),
        'volume': average_volume
    })
asset_df = pd.DataFrame(assets)
#%%
asset_df
#%%
max_count = asset_df.groupby('base')['count'].max()
asset_df = asset_df[asset_df['base'].isin(max_count.index) & asset_df['count'].isin(max_count)]
#%%
asset_df.sort_values('volume', ascending=False, inplace=True)
#%%
asset_df
#%%
with open('asset_list_ftx.txt', 'w') as f:
    f.writelines(asset_df['name'].apply(lambda name: name+'\n'))

# %%
