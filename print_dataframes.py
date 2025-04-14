from load_data import load_data

data = load_data()

df_name_basics = data ['name_basics']
df_title_akas = data['title_akas']
df_title_basics = data['title_basics']
df_title_crew = data['title_crew']
df_title_episode = data['title_episode']
df_title_principals = data['title_principals']
df_title_ratings = data['title_ratings']

print("name_basics")
df_name_basics.show(5)
print("title_akas")
df_title_akas.show(5)
print("title_basics")
df_title_basics.show(5)
print("title_crew")
df_title_crew.show(5)
print("title_episode")
df_title_episode.show(5)
print("title_principals")
df_title_principals.show(5)
print("title_ratings")
df_title_ratings.show(5)
