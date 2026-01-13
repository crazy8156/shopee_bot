import streamlit as st
import pandas as pd

# 1. 設定網頁標題
st.set_page_config(page_title="蝦皮財務中樞", layout="wide")

st.title("📊 蝦皮全能架構師 - 財務分析後台")
st.write("目前系統運作正常，請上傳您的 Excel 報表。")

# 2. 建立 Excel 上傳區
uploaded_file = st.file_uploader("請上傳成本結構表 (Excel)", type=["xlsx", "xls"])

# 3. 讀取並顯示資料 (測試功能)
if uploaded_file is not None:
    try:
        # 讀取 Excel
        df = pd.read_excel(uploaded_file)
        st.success("✅ 檔案讀取成功！")
        
        # 顯示前幾筆資料
        st.subheader("數據預覽：")
        st.dataframe(df.head())
        
        # 顯示簡單統計
        st.write(f"總筆數: {len(df)}")
        
    except Exception as e:
        st.error(f"檔案讀取失敗: {e}")