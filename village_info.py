# -*- coding: utf-8 -*-
"""
data preparation for modeling: village population and income information

"""

import pandas as pd
import numpy as np

pop = pd.DataFrame()
pop_ntp = pd.DataFrame()
lists = ['2017','2018','2019','2020','2021'] 

#### 台北市資料
# 讀取台北市人口資料
for list in lists:
    df = pd.read_excel(f"./data/tpe/{list}_population.ods", engine="odf", header=None)
    df = df.iloc[2:,:]
    df.insert(1, 'year', list)
    pop = pd.concat([pop,df], ignore_index=True)

pop.columns = ['village', 'year', 'village_count', 'village_count', 
                       'neighbor','neighbor2', 'household', 'pop_total', 
                       'pop_M', 'pop_F']

# 加入行政區與縣市欄位
pop.insert(1,'district','NaN')
list=pop[pop['village'].str.contains('區')].index.tolist()

for i in range(len(list)-1):
    pop.loc[(list[i]+1):(list[i+1]-1),['district']]=pop.loc[list[i],['village']].values
    pop.loc[(list[-1]+1):pop.shape[0],['district']]=pop.loc[list[-1],['village']].values

pop.insert(2,'city','TPE')
  

# 篩選所需資料
pop = pop.drop(pop[pop['village'].str.contains('區')].index) # 刪除區資料
pop = pop.drop(pop[pop['village'].str.contains('計')].index) # 刪除總計資料
pop = pop[['village','district', 'city', 'year', 'household', 'pop_total', 'pop_M', 'pop_F']] # 挑選欄位
pop = pop.reset_index(drop=True)
pop['filter'] = pop['district'] + pop['village']

# 檢視資料
pop.isnull().sum()
pop.groupby('year')['village'].count() # 確認每年里個數
pop.drop_duplicates(subset='filter', keep=False) # 確認每年里名稱一致

#### 新北市資料
# 讀取新北市人口資料
for list in lists:
    tmp = pd.read_csv(f"./data/ntp/{list}_population_ntp.csv", encoding='utf-8')
    tmp = tmp.iloc[0:,[3,4,6,7,8,9]]
    tmp.insert(2, 'city', 'NTP')
    tmp.insert(3, 'year', list)
    pop_ntp = pop_ntp.append(tmp, ignore_index=True)
pop_ntp.columns=['district','village','city','year','household','pop_M','pop_F','pop_total']
pop_ntp = pop_ntp.dropna(subset=['district']) # 刪除總計
pop_ntp.reindex(columns=['village','district','city','year','household','pop_total','pop_M','pop_F'])
pop_ntp = pop_ntp.reset_index(drop=True)   
pop_ntp['filter'] = pop_ntp['district'] + pop_ntp['village']

# 檢視資料
pop_ntp.isnull().sum()
pop_ntp.groupby('year')['village'].count() # 確認每年里個數
pop_ntp.drop_duplicates(subset='filter', keep=False) # 確認每年里名稱一致
pop_ntp[pop_ntp['filter'].str.contains('坪林區石')] # 2017年坪林區石𥕢里未造字
pop_ntp[pop_ntp['filter'].str.contains('蘆洲區正義里')] # 似乎有空白，造成不對齊
print(pop_ntp[pop_ntp['village'].str.contains('\s')]) # 2018~2021 蘆洲區的里有空白

# 處理不一致資料
pop_ntp['village'] = pop_ntp['village'].str.replace('石曹','𥕢')
pop_ntp['filter'] = pop_ntp['filter'].str.replace('石曹','𥕢') 
pop_ntp['village'] = pop_ntp['village'].str.strip()
pop_ntp['filter'] = pop_ntp['filter'].str.strip()


#### 合併雙北人口資料
pop = pd.concat([pop,pop_ntp],axis=0, ignore_index=True)


#### 合併里面積資料
# 讀取面積資料(from QGIS, 平方公尺)
area = pd.read_csv('./data/village_area.csv', encoding='utf-8')
mask = (area['COUNTYNAME']=='臺北市') | (area['COUNTYNAME']=='新北市')
area = area[mask]
area = area[['COUNTYNAME','TOWNNAME','VILLNAME','area']]
area = area.reset_index(drop=True)
area.isnull().sum()
area[area['VILLNAME'].isnull()]
area = area.dropna() 
area['filter'] = area['TOWNNAME'] + area['VILLNAME']
area = area[['filter','area']]

# 人口合併里面積
pop_merge = pd.merge(pop, area, how='left', on='filter')

# 檢視合併結果
pop_merge.groupby('year').count()
pop_merge.isnull().sum()
pop_merge[pop_merge['area'].isnull()].groupby('filter').count()
area[area['filter'].str.contains('坪林區石')]
area[area['filter'].str.contains('板橋區公')]
area[area['filter'].str.contains('萬華區糖')]

# 處理錯誤
area['filter'] = area['filter'].str.replace('坪林區石\[曹\]里','坪林區石𥕢里')
area['filter'] = area['filter'].str.replace('公舘里','公館里')
pop['village'] = pop['village'].str.replace('?','廍')
pop['filter'] = pop['filter'].str.replace('?','廍')

# 重新合併與檢視
pop_merge = pd.merge(pop, area, how='left', on='filter')
pop_merge.isnull().sum()

#### 計算人口密度 (人數/每平方公里)
# 檢視並轉換資料格式
pop_merge.dtypes
pop_merge[['household', 'pop_total','pop_M','pop_F']] = pop_merge[['household', 'pop_total','pop_M','pop_F']].astype(float)
pop_merge.dtypes

# 計算人口密度
pop_merge['density'] = pop_merge.eval('pop_total / area * 1000000')


# 彙整各年人口密度
summary = pop_merge.query('year == "2021"')
summary = summary[['village', 'district', 'city','filter','density']]
summary.columns = ['village', 'district', 'city', 'filter', 'density_2021']
summary = summary.reset_index(drop=True)

years = ['2020','2019','2018','2017']
for year in years:
    tmp = pop_merge[pop_merge['year'] == year].loc[:,['density','filter']]
    tmp.columns  = [f'density_{year}', 'filter']
    summary = pd.merge(summary, tmp, how='left', on='filter')


# 計算平均三年的人口成長率
summary['ave_pop_growth'] = summary.eval('(density_2021 / density_2020 + density_2020 / density_2019 + density_2019 / density_2018 - 3) / 3 * 100')

#### 合併里年家戶收入
# 讀取收入資料
income = pd.read_csv('./data/2019 income.csv', encoding='utf-8')
income = income.iloc[:,[0,1,2,4]]
income.columns = ['city','district', 'village','2019_income']
income = income[(income['city']== '臺北市')|(income['city']== '新北市')]

# 選取所需資料
income = income.drop(income[income['village'].str.contains('其他')].index)
income = income.drop(income[income['village'].str.contains('合計')].index)
income = income.reset_index(drop=True)
income['filter'] = income['district'] + income['village']
income = income[['filter', '2019_income']]

# 合併人口資訊與所得資訊
summary = pd.merge(summary, income, on='filter', how='left')

# 檢視合併資訊
summary.isnull().sum()
summary[summary['2019_income'].isnull()]
tmp_income = pd.merge(income, summary, on='filter', how='left')
tmp_income[tmp_income['village'].isnull()]

# 修正資料
vil_before = ['富臺里', '羣賢里', '羣英里', '舊庄里',  '五峰里', '峰廷里', '󿾵寮里', '爪峰里', '濓洞里','濓新里']
vil_after = summary[summary['2019_income'].isnull()]['village'].tolist()
for i in range(len(vil_before)):
    income['filter'] = income['filter'].str.replace( vil_before[i],vil_after[i])

#重新合併資料
summary = summary.drop('2019_income', axis=1)
summary = pd.merge(summary, income, on='filter', how='left')
summary.isnull().sum()
summary.dtypes

#### 存檔
summary.to_csv('./data/population.csv')
