
def get_npk_id(number):
  entries = []
  base = 12345
  for i in range(0, number):
    if len(entries) == 0:
      entries.append(base)
    else:
      entries.append(entries[-1] +1)
  return entries

def fetch_df(df, shortened):
  if shortened == True:
    return df.head()
  else:
    return df