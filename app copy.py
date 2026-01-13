import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import msoffcrypto
import io
import datetime
import time

# ==========================================
# 1. 核心參數設定
# ==========================================
COST_SHEET_NAME = "商品編碼表"       # (新表)
LEGACY_SHEET_NAME = "蝦皮成本比對表2026" # (舊表)
DB_SHEET_NAME = "蝦皮訂單總表"       # 銷售紀錄
MEMORY_SHEET_NAME = "歸戶記憶庫"

SPECIAL_PRODUCTS = ["7777下單信用卡專區", "chatgpt續約區", "ChatGPT", "美圖秀秀", "補運費", "補差價", "專屬賣場", "客製化", "1元賣場"] 

EXCEL_PWD = "287667"   
ADMIN_PWD = "888888"   

st.set_page_config(page_title="蝦皮全自動財務系統 v8.7", layout="wide")

# ==========================================
# 2. 工具函式
# ==========================================
def get_taiwan_time():
    return datetime.datetime.utcnow() + datetime.timedelta(hours=8)

def clean_id(val):
    if pd.isna(val) or val == "": return ""
    s = str(val).strip()
    if "e" in s.lower():
        try: s = "{:.0f}".format(float(s))
        except: pass
    return s.replace(".0", "")

@st.cache_resource
def get_gspread_client():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('service_account.json', scope)
    client = gspread.authorize(creds)
    return client

# === 記憶庫 ===
def get_memory_rules(client):
    try:
        try: sheet = client.open(COST_SHEET_NAME).worksheet(MEMORY_SHEET_NAME)
        except: 
            sh = client.open(COST_SHEET_NAME)
            sheet = sh.add_worksheet(title=MEMORY_SHEET_NAME, rows=100, cols=3)
            sheet.append_row(["蝦皮商品名稱", "真實SKU名稱", "真實成本"])
            return {}
        data = sheet.get_all_values()
        if len(data) <= 1: return {}
        rules = {}
        for row in data[1:]:
            if len(row) >= 3:
                rules[row[0]] = {'sku': row[1], 'cost': float(row[2])}
        return rules
    except: return {}

def save_memory_rule(client, shopee_name, real_sku, real_cost):
    try:
        try: sheet = client.open(COST_SHEET_NAME).worksheet(MEMORY_SHEET_NAME)
        except: sheet = client.open(COST_SHEET_NAME).add_worksheet(title=MEMORY_SHEET_NAME, rows=100, cols=3)
        existing = sheet.col_values(1)
        if shopee_name not in existing:
            sheet.append_row([shopee_name, real_sku, real_cost])
            return True
    except: pass
    return False

# ==========================================
# 3. 資料讀取
# ==========================================
def get_cost_sheet_raw():
    try:
        client = get_gspread_client()
        sheet = client.open(COST_SHEET_NAME).sheet1
        data = sheet.get_all_values()
        df = pd.DataFrame(data[1:], columns=data[0])
        df['原始行號'] = range(2, len(df) + 2)
        if '商品' in df.columns and '商品名稱' not in df.columns:
            df.rename(columns={'商品': '商品名稱'}, inplace=True)
        return df
    except: return None

@st.cache_data(ttl=60)
def load_cloud_cost_table():
    try:
        client = get_gspread_client()
        sheet = client.open(COST_SHEET_NAME).sheet1
        data = sheet.get_all_values()
        if len(data) <= 1: return None, sheet
        
        # === 強韌標題判斷 ===
        if "商品" in str(data[0]) or "成本" in str(data[0]):
            df = pd.DataFrame(data[1:], columns=data[0])
        else:
            expected = ['商品名稱', '蝦皮商品編碼', '成本']
            if len(data[0]) > 3: expected += [f"Col_{i}" for i in range(4, len(data[0])+1)]
            df = pd.DataFrame(data, columns=expected[:len(data[0])])
            st.warning("⚠️ 偵測到表頭缺失，已自動補全。")

        df.columns = df.columns.str.strip()
        if '商品' in df.columns: df.rename(columns={'商品': '商品名稱'}, inplace=True)
            
        if '蝦皮商品編碼' not in df.columns or '成本' not in df.columns:
            st.error(f"❌ 『{COST_SHEET_NAME}』缺少關鍵欄位。偵測到：{list(df.columns)}")
            return None, sheet

        df['蝦皮商品編碼'] = df['蝦皮商品編碼'].apply(clean_id)
        df['成本'] = pd.to_numeric(df['成本'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
        df['Menu_Label'] = df['商品名稱'] + " | 成本$" + df['成本'].astype(str)
        df['has_cost'] = df['成本'] > 0
        df = df.sort_values(by=['蝦皮商品編碼', 'has_cost'], ascending=[True, True])
        df = df.drop_duplicates(subset=['蝦皮商品編碼'], keep='last')
        
        return df, sheet
    except Exception as e:
        st.error(f"❌ 讀取『{COST_SHEET_NAME}』失敗：{e}")
        return None, None

def process_mass_update_file(uploaded_file):
    try:
        try: import python_calamine; engine = 'calamine'
        except: engine = 'openpyxl'
        try: df = pd.read_excel(uploaded_file, header=2, engine=engine)
        except: return None
        df = df.dropna(subset=['商品ID'])
        df['key'] = df['商品ID'].apply(clean_id) + "_" + df['商品選項ID'].apply(clean_id)
        df['Full_Name'] = df['商品名稱'].astype(str)
        if '商品規格名稱' in df.columns:
             df['Full_Name'] += " [" + df['商品規格名稱'].astype(str).fillna('') + "]"
        return df[['Full_Name', 'key']]
    except: return None

def load_sales_report(uploaded_file):
    try:
        file_content = uploaded_file.getvalue()
        try: df = pd.read_excel(io.BytesIO(file_content), engine='openpyxl')
        except:
            decrypted = io.BytesIO()
            office_file = msoffcrypto.OfficeFile(io.BytesIO(file_content))
            office_file.load_key(password=EXCEL_PWD)
            office_file.decrypt(decrypted)
            decrypted.seek(0)
            df = pd.read_excel(decrypted)
        
        df.columns = df.columns.astype(str).str.strip().str.replace('\n', '')
        mapping = {'蝦皮商品編碼 (商品ID_規格ID)': '蝦皮商品編碼', '商品總價': '售價', '訂單小計 (撥款金額)': '進蝦皮錢包', '買家支付運費': '運費'}
        for col in df.columns:
            if "撥款金額" in col or "進蝦皮錢包" in col: mapping[col] = "進蝦皮錢包"
            if "商品編碼" in col and "規格" in col: mapping[col] = "蝦皮商品編碼"
        df.rename(columns=mapping, inplace=True)
        if '蝦皮商品編碼' in df.columns: df['蝦皮商品編碼'] = df['蝦皮商品編碼'].apply(clean_id)
        df = df.drop_duplicates()
        return df
    except Exception as e: st.error(f"Excel 解析失敗: {e}"); return None

# ==========================================
# 4. 寫入邏輯 (V8.7: 增量更新 - 絕對不覆蓋舊資料)
# ==========================================
def sync_new_products(new_products_df, sheet, progress_bar):
    current_data = sheet.get_all_values()
    if len(current_data) > 1:
        current_ids = set([clean_id(row[1]) for row in current_data[1:]])
    else:
        current_ids = set()
        if not current_data: sheet.append_row(['商品名稱', '蝦皮商品編碼', '成本'])
    rows_to_add = []
    for _, row in new_products_df.iterrows():
        if row['key'] not in current_ids and row['key'] != "_":
            rows_to_add.append([row['Full_Name'], row['key'], 0])
            current_ids.add(row['key'])
    if rows_to_add: sheet.append_rows(rows_to_add); return len(rows_to_add)
    return 0

def auto_fill_costs_from_legacy(progress_bar):
    """ V8.5: 聰明搜尋舊表 """
    client = get_gspread_client()
    progress_bar.progress(10, text=f"搜尋舊表『{LEGACY_SHEET_NAME}』...")
    try:
        sh = client.open(LEGACY_SHEET_NAME)
        worksheets = sh.worksheets()
        target_ws = None; df_old = None
        for ws in worksheets:
            data = ws.get_all_values()
            if len(data) > 2: 
                row1 = str(data[0])
                if "編碼" in row1 or "ID" in row1 or "成本" in row1:
                    target_ws = ws
                    df_old = pd.DataFrame(data[1:], columns=data[0])
                    break
        if df_old is None: return f"❌ 舊表無資料"

        df_old.columns = df_old.columns.str.strip()
        col_id = None; col_cost = None
        for c in ['蝦皮商品編碼', '商品編碼', '商品ID', '編碼', 'ID']:
            if c in df_old.columns: col_id = c; break
        for c in ['成本', 'Cost', 'cost', '進貨成本', '進價']:
            if c in df_old.columns: col_cost = c; break
        if not col_id or not col_cost: return f"❌ 欄位對應失敗"
        
        cost_map = {}
        for _, row in df_old.iterrows():
            code = clean_id(row[col_id])
            try: cost = float(str(row[col_cost]).replace(',', ''))
            except: cost = 0
            if cost > 0: cost_map[code] = cost
    except Exception as e: return f"❌ 讀取舊表失敗：{e}"

    progress_bar.progress(40, text=f"讀取新表『{COST_SHEET_NAME}』...")
    try:
        new_sheet = client.open(COST_SHEET_NAME).sheet1
        new_data = new_sheet.get_all_values()
        if "商品" in str(new_data[0]) or "成本" in str(new_data[0]): df_new = pd.DataFrame(new_data[1:], columns=new_data[0])
        else:
             expected = ['商品名稱', '蝦皮商品編碼', '成本']
             if len(new_data[0]) > 3: expected += [f"Col_{i}" for i in range(4, len(new_data[0])+1)]
             df_new = pd.DataFrame(new_data, columns=expected[:len(new_data[0])])
        
        df_new.columns = df_new.columns.str.strip()
        new_col_id = '蝦皮商品編碼' if '蝦皮商品編碼' in df_new.columns else None
        new_col_cost = '成本' if '成本' in df_new.columns else None
        if not new_col_id or not new_col_cost: return f"❌ 新表欄位失敗"
    except Exception as e: return f"❌ 讀取新表失敗：{e}"

    progress_bar.progress(60, text="寫入成本資料...")
    updated_count = 0
    for i, row in df_new.iterrows():
        code = clean_id(row[new_col_id])
        current_cost = 0
        try: current_cost = float(str(row[new_col_cost]).replace(',', ''))
        except: pass
        if current_cost == 0 and code in cost_map:
            df_new.at[i, new_col_cost] = cost_map[code]
            updated_count += 1

    if updated_count > 0:
        updated_values = [df_new.columns.tolist()] + df_new.astype(str).values.tolist()
        new_sheet.clear(); new_sheet.update(updated_values)
        return f"✅ 成功救援 {updated_count} 筆成本資料！"
    else: return "✅ 無需更新"

def process_orders(df_sales, df_cost, progress_bar):
    # 1. 基礎檢查與清理
    required_cols = ['訂單編號', '商品名稱']
    for col in required_cols:
        if col not in df_sales.columns: return f"❌ 失敗：報表找不到『{col}』。"

    progress_bar.progress(10, text="資料清理...")
    if '訂單狀態' in df_sales.columns:
        df_sales = df_sales[df_sales['訂單狀態'].astype(str).str.strip() != '不成立']
    
    # 2. 計算成本與利潤 (這部分先針對上傳的資料算，稍後會過濾)
    progress_bar.progress(30, text="新訂單計算...")
    df_cost_slim = df_cost[['蝦皮商品編碼', '成本']]
    df_merged = pd.merge(df_sales, df_cost_slim, on='蝦皮商品編碼', how='left')
    
    cols_to_clean = ['售價', '成交手續費', '金流與系統處理費', '其他服務費', '數量', '成本', '進蝦皮錢包']
    for c in cols_to_clean:
        if c in df_merged.columns:
            df_merged[c] = pd.to_numeric(df_merged[c].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
    
    if '蝦皮付費總金額' not in df_merged.columns:
        df_merged['蝦皮付費總金額'] = df_merged['成交手續費'] + df_merged['金流與系統處理費'] + df_merged['其他服務費']
    if '進蝦皮錢包' not in df_merged.columns or df_merged['進蝦皮錢包'].sum() == 0:
        df_merged['進蝦皮錢包'] = df_merged['售價'] - df_merged['蝦皮付費總金額']

    df_merged['總利潤'] = df_merged['進蝦皮錢包'] - (df_merged['成本'] * df_merged['數量'])
    
    # 3. 讀取現有資料庫
    progress_bar.progress(50, text=f"比對 {DB_SHEET_NAME}...")
    client = get_gspread_client()
    try: db_sheet = client.open(DB_SHEET_NAME).sheet1
    except: return f"❌ 找不到資料庫：{DB_SHEET_NAME}"
    
    # 準備格式
    headers = ['訂單編號', '訂單成立日期', '商品名稱', '商品選項名稱', '數量', '售價', '成交手續費', '金流與系統處理費', '其他服務費', '蝦皮付費總金額', '進蝦皮錢包', '成本', '總利潤', '蝦皮商品編碼', '資料備份時間', '備註']
    
    df_upload_ready = df_merged.copy()
    df_upload_ready['資料備份時間'] = get_taiwan_time().strftime("%Y-%m-%d %H:%M:%S")
    df_upload_ready['備註'] = "" 
    
    # 套用記憶庫 (僅針對上傳的新資料)
    memory_rules = get_memory_rules(client)
    if '商品名稱' in df_upload_ready.columns:
        mask_special = df_upload_ready['商品名稱'].astype(str).apply(lambda x: any(sp in x for sp in SPECIAL_PRODUCTS))
        df_upload_ready.loc[mask_special, '備註'] = "待人工確認"
        df_upload_ready.loc[mask_special, '總利潤'] = 0
        
        for idx, row in df_upload_ready[mask_special].iterrows():
            p_name = str(row['商品名稱']).strip()
            if p_name in memory_rules:
                rule = memory_rules[p_name]
                real_cost = rule['cost']
                income = float(row['進蝦皮錢包'])
                real_profit = income - real_cost 
                df_upload_ready.at[idx, '成本'] = real_cost
                df_upload_ready.at[idx, '總利潤'] = real_profit
                df_upload_ready.at[idx, '備註'] = f"已歸戶(自動): {rule['sku']}"

    for h in headers:
        if h not in df_upload_ready.columns: df_upload_ready[h] = ""
    df_upload_ready = df_upload_ready[headers].fillna('').astype(str)
    
    # 4. 關鍵邏輯：增量更新 (不覆蓋)
    existing_data = db_sheet.get_all_values()
    
    if len(existing_data) <= 1:
        # 資料庫是空的，直接寫入
        db_sheet.clear(); db_sheet.append_row(headers); db_sheet.append_rows(df_upload_ready.values.tolist())
        return f"✅ 初始化完成！新增 {len(df_upload_ready)} 筆。"
    else:
        # 資料庫有東西
        df_existing = pd.DataFrame(existing_data[1:], columns=existing_data[0])
        
        # 抓出已經存在的訂單 ID
        existing_ids = set(df_existing['訂單編號'].astype(str).str.strip())
        
        # 過濾上傳的資料：只保留資料庫裡「沒有」的
        df_new_orders = df_upload_ready[~df_upload_ready['訂單編號'].astype(str).str.strip().isin(existing_ids)]
        
        skipped_count = len(df_upload_ready) - len(df_new_orders)
        
        if not df_new_orders.empty:
            progress_bar.progress(80, text=f"新增 {len(df_new_orders)} 筆新資料...")
            db_sheet.append_rows(df_new_orders.values.tolist())
            progress_bar.progress(100, text="完成")
            return f"✅ 成功！新增 {len(df_new_orders)} 筆新訂單 (跳過 {skipped_count} 筆舊資料)。"
        else:
            progress_bar.progress(100, text="無新資料")
            return f"✅ 沒事做！上傳的 {skipped_count} 筆訂單資料庫裡都有了 (已自動保留您的舊紀錄)。"

def update_special_order(order_sn, real_sku_name, real_cost, df_db, db_sheet):
    """ 歸戶邏輯 """
    idx = df_db.index[df_db['訂單編號'] == order_sn].tolist()
    if not idx: return False
    idx = idx[0]
    
    income = float(str(df_db.at[idx, '進蝦皮錢包']).replace(',', ''))
    real_profit = income - real_cost
    
    df_db.at[idx, '成本'] = real_cost
    df_db.at[idx, '總利潤'] = real_profit
    df_db.at[idx, '備註'] = f"已歸戶: {real_sku_name}"
    
    updated_data = [df_db.columns.tolist()] + df_db.astype(str).values.tolist()
    db_sheet.clear()
    db_sheet.update(updated_data)
    return True

# ==========================================
# 5. 主程式
# ==========================================
st.sidebar.title("🚀 蝦皮財務系統 v8.7")
mode = st.sidebar.radio("模式", ["📊 前台戰情室", "⚙️ 後台管理", "🔍 成本神探 (抓錯用)"])

if mode == "🔍 成本神探 (抓錯用)":
    st.title("🔍 成本神探")
    target_id = st.text_input("輸入蝦皮商品編碼")
    if target_id:
        with st.spinner(f"正在掃描『{COST_SHEET_NAME}』..."):
            df_raw = get_cost_sheet_raw()
            if df_raw is not None:
                df_raw['Clean_ID'] = df_raw['蝦皮商品編碼'].apply(clean_id)
                target_clean = clean_id(target_id)
                matches = df_raw[df_raw['Clean_ID'] == target_clean]
                if not matches.empty: st.error(f"出現 {len(matches)} 次："); st.dataframe(matches)
                else: st.warning("找不到此編碼。")

elif mode == "📊 前台戰情室":
    st.title("📊 蝦皮營業額戰情室")
    if st.sidebar.button("🔄 刷新"): st.cache_data.clear(); st.rerun()
    
    client = get_gspread_client()
    try:
        sheet = client.open(DB_SHEET_NAME).sheet1
        data = sheet.get_all_values()
        if len(data) > 1:
            df_all = pd.DataFrame(data[1:], columns=data[0])
            for c in ['售價', '成本', '數量', '總利潤', '進蝦皮錢包']:
                if c in df_all.columns: df_all[c] = pd.to_numeric(df_all[c].astype(str).str.replace(',',''), errors='coerce').fillna(0)
        else: st.warning("無資料"); st.stop()
    except: st.error("讀取失敗"); st.stop()

    if df_all is not None:
        if '備註' not in df_all.columns: df_all['備註'] = ""
        if '訂單成立日期' in df_all.columns:
            df_all['訂單成立日期'] = pd.to_datetime(df_all['訂單成立日期'], errors='coerce')
            df_all['日期標籤'] = df_all['訂單成立日期'].dt.strftime('%Y-%m-%d')
        else: st.error("缺日期欄位"); st.stop()

        dates = sorted(df_all['日期標籤'].dropna().unique(), reverse=True)
        sel_date = st.selectbox("📅 選擇日期", dates) if dates else None
        
        if sel_date:
            df_day = df_all[df_all['日期標籤'] == sel_date]
            mask_special = (
                df_day['商品名稱'].astype(str).apply(lambda x: any(sp in x for sp in SPECIAL_PRODUCTS)) & 
                (~df_day['備註'].astype(str).str.contains("已歸戶"))
            )
            df_special = df_day[mask_special]
            df_normal = df_day[~df_day.index.isin(df_special.index)]
            
            total_rev = df_normal['售價'].sum()
            total_cost = (df_normal['成本'] * df_normal['數量']).sum()
            total_gp = df_normal['總利潤'].sum()
            margin = (total_gp / total_rev * 100) if total_rev > 0 else 0
            
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("💰 營收 (含歸戶)", f"${total_rev:,.0f}")
            k2.metric("📉 成本", f"${total_cost:,.0f}")
            k3.metric("💸 毛利", f"${total_gp:,.0f}")
            k4.metric("📊 毛利率", f"{margin:.1f}%")
            
            st.divider()
            if not df_special.empty:
                st.warning(f"⚠️ 有 {len(df_special)} 筆訂單未歸戶"); st.dataframe(df_special[['訂單編號', '商品名稱', '售價', '備註']])
            
            st.subheader("📦 銷售明細")
            cols = ['訂單編號', '商品名稱', '數量', '售價', '成本', '總利潤', '備註']
            final = [c for c in cols if c in df_normal.columns]
            st.dataframe(df_normal[final], use_container_width=True)

elif mode == "⚙️ 後台管理":
    st.title("⚙️ 後台管理")
    if st.text_input("密碼", type="password") == ADMIN_PWD:
        tab1, tab2, tab3 = st.tabs(["📥 報表上傳", "🔗 特殊訂單歸戶", "🛠️ 商品同步"])

        with tab1:
            st.subheader("Step 1: 上傳 Order.all")
            df_cost, _ = load_cloud_cost_table()
            if df_cost is not None:
                st.success(f"✅ 成功讀取 {len(df_cost)} 筆成本資料")
                
                sales_file = st.file_uploader("選擇 Excel", type=['xlsx'])
                if sales_file and st.button("🚀 執行"):
                    bar = st.progress(0, "開始...")
                    df_sales = load_sales_report(sales_file)
                    if df_sales is not None:
                        res = process_orders(df_sales, df_cost, bar)
                        st.success(res); st.cache_data.clear()

        with tab2:
            st.subheader("Step 2: 特殊訂單歸戶")
            st.info("💡 勾選「記住」，下次自動處理！")
            
            client = get_gspread_client()
            try:
                db_sheet = client.open(DB_SHEET_NAME).sheet1
                data = db_sheet.get_all_values()
                if len(data) > 1: df_db = pd.DataFrame(data[1:], columns=data[0])
                else: st.warning("無資料"); st.stop()
            except: st.error("讀取失敗"); st.stop()
            
            if '備註' not in df_db.columns: df_db['備註'] = ""
            mask = (
                df_db['商品名稱'].astype(str).apply(lambda x: any(sp in x for sp in SPECIAL_PRODUCTS)) & 
                (~df_db['備註'].astype(str).str.contains("已歸戶"))
            )
            pending = df_db[mask]
            
            if pending.empty: st.success("✅ 全部歸戶完成！")
            else:
                st.write(f"待處理：{len(pending)} 筆")
                df_cost_ref, _ = load_cloud_cost_table()
                if df_cost_ref is not None:
                    cost_dict = pd.Series(df_cost_ref.成本.values, index=df_cost_ref.Menu_Label).to_dict()
                    options = ["請選擇..."] + list(cost_dict.keys())
                    
                    for idx, row in pending.iterrows():
                        with st.container():
                            c1, c2, c3 = st.columns([2, 2, 1])
                            c1.text(f"{row['商品名稱']}\n{row['訂單編號']} (${row['售價']})")
                            
                            sel = c2.selectbox("真實商品", options, key=f"s_{row['訂單編號']}", label_visibility="collapsed")
                            remember_me = c2.checkbox("記住對應", key=f"chk_{row['訂單編號']}")
                            
                            if c3.button("歸戶", key=f"b_{row['訂單編號']}"):
                                if sel != "請選擇...":
                                    real_cost = cost_dict[sel]
                                    real_name = sel.split(" |")[0]
                                    with st.spinner("更新中..."):
                                        update_special_order(row['訂單編號'], real_name, real_cost, df_db, db_sheet)
                                        if remember_me:
                                            if "7777" in row['商品名稱']: st.warning("⚠️ 拒絕記住 7777！")
                                            else:
                                                save_memory_rule(client, row['商品名稱'], real_name, real_cost)
                                                st.toast("🧠 已記住規則！")
                                        st.toast("✅ 成功"); time.sleep(1); st.rerun()
                                else: st.error("請選擇")
                            st.divider()

        with tab3:
            st.subheader("Step 3: 商品資料維護")
            
            st.markdown("##### 1. 新增商品 (從蝦皮匯出檔)")
            mass_file = st.file_uploader("上傳 mass_update.xlsx", type=['xlsx'])
            if mass_file and st.button("同步至商品編碼表"):
                bar = st.progress(0, "...")
                df_new = process_mass_update_file(mass_file)
                if df_new is not None:
                    client = get_gspread_client()
                    sheet = client.open(COST_SHEET_NAME).sheet1
                    cnt = sync_new_products(df_new, sheet, bar)
                    st.success(f"新增 {cnt} 筆")
            
            st.divider()
            
            st.markdown("##### 2. 成本資料救援")
            st.info("若您的商品編碼表目前成本為 0，可按此鈕去抓取『蝦皮成本比對表2026』的舊資料。")
            if st.button("🔄 從舊表 (2026) 匯入成本"):
                bar2 = st.progress(0, "連線中...")
                res = auto_fill_costs_from_legacy(bar2)
                if "❌" in res: st.error(res)
                else: st.success(res)