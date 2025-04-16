from load_data import load_data
from ira_iskovych import run_ira_queries
from maks import run_maks_queries
from sofiia import run_sofiia_questions
from andrii_queries import  run_andrii_queries

dfs = load_data()
print('='*86+'\n\t\t\t\t\t\tSOFIIA SUBTELNA BUSINESS QUESTIONS\n'+'='*86)
run_sofiia_questions(dfs)
print('='*86+'\n\t\t\t\t\t\tANDRII KUZIV BUSINESS QUESTIONS\n'+'='*86)
run_andrii_queries()
print('='*86+'\n\t\t\t\t\t\tMAKSYM PAVLISH BUSINESS QUESTIONS\n'+'='*86)
run_maks_queries(dfs)
print('='*86+'\n\t\t\t\t\t\tIRA ISKOVYCH BUSINESS QUESTIONS\n'+'='*86)
run_ira_queries(dfs)
