# 資策會_巨量資料分析就業養成班
## 期末專題: cafe選址樂 
- 目的：依據租屋網可租店面地點，提供咖啡店開業者合適的開店地點
- 地點範圍：雙北
- 成果網站：https://leaflet-cafe-map.herokuapp.com/cafemap.html

## Git展示內容
- 報告投影片：資策會專題報告_cafe選址樂.pdf
- 我所負責的程式：資料準備與資料清理
  * 彙整里人口與家戶所得資訊：village_info.py
  * 計算各點距離並彙整模型所需資料表：distance_features.py
- data：程式所需資料

## 專案流程
- 訂定問題
  * 在咖啡店快速展店的趨勢下，是否能自動化提供可展店的地點與外部商圈資訊?
  * 是否能以商圈資訊與替代營收指標，建立選址的推薦模型(推薦開店、不推薦開店)?
- 資料蒐集
  * 商圈資訊：
    + 市場規模：人口密度與成長率
    + 品質：商圈家戶所得
    + 商業氣息： 其他業別店家數
    + 競爭程度：同業店數
  * 營收替代指標：Google商家評論數
- 資料準備與清理
  * 彙整里人口與家戶所得資訊
  * 彙整咖啡店與可租店面地點資訊
  * 計算各點距離
  * 彙整模型所需資料表
- 資料分析
  * 維度轉換：LDA vs. NCA，PCA vs. KPCA
- 模型建立
  * 監督式分類：XGBoost、RandomForest、SVM
  * 類別：高評論(推薦)、低評論(不推薦)
  * 模型評估指標：高的高評論數 precision rate(避免可租店面被誤判為推薦展店時,開店後，造成損失)
- 應用：將蒐集到的外部商圈環境資料結合公司內部資料，作為展店營收預測工具
