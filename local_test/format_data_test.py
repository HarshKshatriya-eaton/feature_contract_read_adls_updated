import pandas as pd

from utils.format_data import Format

format = Format()

df = pd.DataFrame(['ABC', 'test', 'val', None], columns=['col'])

print(df)

fo = {
    "actual_datasoure_name": "col",
    "output_format": "text : lower"
}
fo1 = {
    "col1": {
        "actual_datasoure_name": "col",
        "output_format": "text : capitalize each word"
    }
}
df_out = format.format_output(df, fo1)
print(df_out)