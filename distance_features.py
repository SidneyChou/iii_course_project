# -*- coding: utf-8 -*-
"""
data preparation for modeling: cafe shops and renting shops information

"""

import pandas as pd
import numpy as np
import geopandas as gpd

# 讀取檔案
rent = pd.read_csv('./data/renting_shop.csv',encoding='utf-8')
point = pd.read_csv('./data/feat_points.csv',encoding='utf-8')
cafe = pd.read_csv('./data/cafe.csv',encoding='utf-8')

# 經緯度轉成 geometry資料型態 (原座標系統:  EPSG:4326 == WGS84)
g_rent = gpd.GeoDataFrame(rent, crs='epsg:4326', geometry=gpd.points_from_xy(rent.long, rent.lat))
g_total = gpd.GeoDataFrame(point, crs='epsg:4326', geometry=gpd.points_from_xy(point['long'], point['lat']))
g_cafe = gpd.GeoDataFrame(cafe, crs='epsg:4326', geometry=gpd.points_from_xy(cafe['long'], cafe['lat']))

# 轉換成台灣座標系統，位置會比較準確 (EPSG:3826 == TWD97)
g_rent = g_rent.to_crs('EPSG:3826')
g_total = g_total.to_crs('EPSG:3826')
g_cafe = g_cafe.to_crs('EPSG:3826')

#### 咖啡店資料
# 計算距離 (EPSG:3826 算出的距離為公尺)
distance = g_total.geometry.apply(lambda g: round(g_cafe.distance(g),0))

# 彙整距離表
cafe_distance = pd.DataFrame(distance[0])
cafe_distance.columns = ['distance']
cafe_distance['cafe_id'] = 0
cafe_distance['point_id'] = distance.index
cafe_distance['point_type'] = point['type2']

for i in range(1, distance.shape[1]):
    tmp = pd.DataFrame(distance[i])
    tmp.columns = ['distance']
    tmp['cafe_id'] = i
    tmp['point_id'] = distance.index
    tmp['point_type'] = point['type2']
    cafe_distance = pd.concat([cafe_distance, tmp], ignore_index=True)    

cafe_distance = cafe_distance [['cafe_id', 'point_id', 'point_type', 'distance']]
cafe_distance = pd.read_csv('./data/cafe_distance.csv',encoding='utf-8') # 匯出檔案

# 建立特徵表
# 增加人口與收入資訊
cafe = cafe.loc[:,['area','rating','comment', 'brand', 'name', 'addr', 'filter', 'long', 'lat', 'village_rent','density_2021', 'ave_pop_growth', '2019_income']]

# 建立特徵表: 計算特定距離內的點數量
d_list = [500, 400, 300, 200, 100]
for d in d_list:
    types = ['cafe','small_cafe', 'breakfast', 'beverage','fastfood', 'supermarket','MRT', 'bus_stop', 'parking_space', 'CVS', 'school', 'bank', 'train']
    for i in range(len(types)):
        cafe_tmp = cafe_distance[cafe_distance['point_type'] == types[i]]
        for j in range(cafe.shape[0]):
            tmp = cafe_tmp[cafe_tmp['cafe_id']==j]
            cafe.loc[j,types[i]] = tmp[tmp['distance']<d].shape[0]
    cafe.to_csv(f'./data/cafe_features_{d}m.csv', index=False)

# 檢視特徵表
distance = [100,200,300,400,500]
for d in distance:
    features = pd.read_csv(f'./data/cafe_features_{d}m.csv',encoding='utf-8')
    print(features.shape)
    print(features.isnull().sum())

# 刪除空值
for d in distance:
    features = pd.read_csv(f'./data/cafe_features_{d}m.csv',encoding='utf-8')
    features.insert(0,'id',features.index)
    features.isnull().sum()
    features[features['density_2021'].isnull()]
    features = features.drop(features[features['density_2021'].isnull()].index, axis=0)
    features = features.drop(features[features['rating']==0].index, axis=0)
    features = features.reset_index(drop=True)
    features.to_csv(f'./data/cafe_features_{d}m.csv', index=False)

#### 出租店資料
# 距離計算
distance_rent = g_total.geometry.apply(lambda g: round(g_rent.distance(g),0)) 

# 彙整距離表
idx = distance_rent.index
types = point['type2']
rent_distance = pd.DataFrame(distance_rent.iloc[:,0])
rent_distance.columns = ['distance']
rent_distance['rent_id'] = 0
rent_distance['point_id'] = idx
rent_distance['point_type'] = types

for i in range(1, distance_rent.shape[1]):# 
    tmp = pd.DataFrame(distance_rent.iloc[:,i])
    tmp.columns = ['distance']
    tmp['rent_id'] = i
    tmp['point_id'] = idx
    tmp['point_type'] = types
    rent_distance = pd.concat([rent_distance, tmp], ignore_index=True)
    
rent_distance = rent_distance[['rent_id', 'point_id', 'point_type', 'distance']]
rent_distance.to_csv('./data/rent_distance.csv', index=False)

# 建立特徵表
# 增加人口與收入資訊
rent = rent.loc[:,['id','city','address','filter','price','area','lat','long', 'density_2021','ave_pop_growth','2019_income']]
rent['avg_price'] = rent['price'] / rent['area']
rent['avg_price'] = rent['avg_price'].astype(int)
rent = rent[['id','city','address','filter','price','area','lat','long','avg_price','density_2021','ave_pop_growth','2019_income']]

# 建立特徵表: 計算特定距離內的點數量
d_list = [500, 400, 300, 200, 100]
for d in d_list:
    types = ['cafe','small_cafe', 'breakfast', 'beverage','fastfood', 'supermarket','MRT', 'bus_stop', 'parking_space', 'CVS', 'school', 'bank', 'train']
    for i in range(len(types)):
        rent_tmp = rent_distance[rent_distance['point_type'] == types[i]]
        for j in range(rent.shape[0]):
            tmp = rent_tmp[rent_tmp['rent_id']==j]
            rent.loc[j,types[i]] = tmp[tmp['distance']<d].shape[0]
    rent.to_csv(f'./data/rent_features_{d}m_new.csv', index=False)

# 檢視特徵表
distance = [100,200,300,400,500]
for d in distance:
    features = pd.read_csv(f'./data/rent_features_{d}m.csv',encoding='utf-8')
    print(features.shape)
    print(features.isnull().sum())

# 刪除空值
for d in distance:
    features = pd.read_csv(f'./data/rent_features_{d}m.csv',encoding='utf-8')
    features.insert(0,'id',features.index)
    features.isnull().sum()
    features[features['density_2021'].isnull()]
    features = features.drop(features[features['density_2021'].isnull()].index, axis=0)
    features = features.reset_index(drop=True)
    features.to_csv(f'./data/rent_features_{d}m.csv', index=False)
