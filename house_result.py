import pandas as pd

df_main = pd.read_excel("hous_hunter_urls.xlsx")
df_companies = pd.read_excel("house_info.xlsx")

df_right = pd.concat([df_companies])
df_right = df_right.drop_duplicates(subset=["ID"], keep='first')

df_all = pd.merge(df_main, df_right, on="ID", how="left")

def row_has_error(row):
    for val in row:
        if isinstance(val, str) and "error" in val.lower():
            return True
    return False

df_clean = df_all[~df_all.apply(row_has_error, axis=1)]

df_clean.to_excel("house_result.xlsx", index=False)
print(f"Итоговый файл: house_result.xlsx (строк: {len(df_clean)})")
