import pandas as pd
import numpy as np
import requests
import json
import os

def query_bsi_dataset(secret_id, start_period="2020-01-01"):
    headers = {
        'X-IBM-Client-Id': secret_id,
        'accept': "application/json"
        }

    #BSI series codes
    bsi_series_codes_all = ""
    for i in range(57, 71):
        code = "EIBSIM000" + str(i)
        if i != 70:
            code += ","
        bsi_series_codes_all += code

    #Query API
    url = f"https://apigw1.bot.or.th/bot/public/observations/?series_code={bsi_series_codes_all}&start_period={start_period}"
    r = requests.get(url, headers=headers)
    content = json.loads(r.content)

    #Combine to dataframe
    df_bsi_all = pd.DataFrame()
    for j in range(len(content['result']['series'])):
        df_desc = pd.DataFrame(content['result']['series'][j])
        df_obs = pd.DataFrame(content['result']['series'][j]['observations'])
        df_desc[['period_start', 'value']] = df_obs
        df_desc.drop(columns='observations', inplace=True)
        df_bsi_all = df_bsi_all.append(df_desc, ignore_index=True, sort=False)

    #Clean data
    df_bsi_all.value = df_bsi_all.value.astype('float')
    df_bsi_all['series_category'] = np.where(df_bsi_all['series_name_eng'].str.contains('next 3 months'), "BSI next 3 months", "BSI current")
    df_bsi_all['series_sub_component_eng'] = df_bsi_all['series_name_eng']\
        .str.replace('Business Sentiment Index', '')\
        .str.replace('of', '')\
        .str.replace('next 3 months', '')
    df_bsi_all['series_sub_component_th'] = df_bsi_all['series_name_th']\
        .str.replace('ดัชนีความเชื่อมั่นทางธุรกิจ', '')\
        .str.replace('3 เดือนข้างหน้า', '')\
    
    df_bsi_all['series_sub_component_eng'] = df_bsi_all['series_sub_component_eng'].str.strip()
    df_bsi_all['series_sub_component_th'] = df_bsi_all['series_sub_component_th'].str.strip()

    df_bsi_all['series_sub_component_eng'] = np.where(df_bsi_all['series_sub_component_eng'] == '', 'BSI', df_bsi_all['series_sub_component_eng'])
    df_bsi_all['series_sub_component_th'] = np.where(df_bsi_all['series_sub_component_th'] == '', 'BSI', df_bsi_all['series_sub_component_th'])
    
    #Export to csv
    df_bsi_all.to_csv('data/bsi.csv', encoding='utf-8-sig')
    return df_bsi_all

#Contribution to index
def gen_bsictg(df):
    df_cont = df[(df['series_category'] == 'BSI current') & (df['series_sub_component_th'] != 'BSI')]
    df_cont = df_cont.pivot_table(columns='series_sub_component_th', index='period_start', values='value', aggfunc='median')
    df_cont = ((df_cont - 50) * 1/6)

    bsi_cur = df[(df['series_category'] == 'BSI current') & (df['series_sub_component_th'] == 'BSI')][['period_start', 'value']].set_index('period_start')
    df_cont = df_cont.join(bsi_cur)
    df_cont.rename(columns={'value': 'BSI'}, inplace=True)
    df_cont.index = pd.to_datetime(df_cont.index)
    df_cont = df_cont.round(2)
    
    #Export to csv
    df_cont.to_csv('data/bsictg.csv', encoding='utf-8-sig')

if __name__ == "__main__":
    os.mkdir('data')
    df = query_bsi_dataset(os.environ['SECRET_ID'], start_period="2020-01-01")
    gen_bsictg(df)