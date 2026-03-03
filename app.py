import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import msoffcrypto
import io
from datetime import datetime, timedelta, timezone
import time
import plotly.express as px
import plotly.graph_objects as go

# ==========================================
# 1. 核心參數設定
# ==========================================
COST_SHEET_NAME = "商品編碼表"       # (新表)
LEGACY_SHEET_NAME = "蝦皮成本比對表2026" # (舊表)
DB_SHEET_NAME = "蝦皮訂單總表"       # 銷售紀錄
MEMORY_SHEET_NAME = "歸戶記憶庫"
AD_COST_SHEET_NAME = "廣告費用紀錄"

SPECIAL_PRODUCTS = ["7777下單信用卡專區", "chatgpt續約區", "ChatGPT", "美圖秀秀", "補運費", "補差價", "專屬賣場", "客製化", "1元賣場"] 

EXCEL_PWD = "287667"   
ADMIN_PWD = "888888"   

st.set_page_config(
    page_title="蝦皮全自動財務系統 v8.8", 
    page_icon="🦐",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==========================================
# 0. UI 美化設定 (Custom CSS)
# ==========================================
def inject_custom_css():
    st.markdown("""
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&display=swap');
        
        /* 全域字體設定 */
        html, body, [class*="css"] {
            font-family: 'Inter', 'Microsoft JhengHei', system-ui, -apple-system, sans-serif;
        }
        
        /* 標題漸層效果 */
        h1 {
            background: -webkit-linear-gradient(45deg, #FF512F, #DD2476);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            font-weight: 800 !important;
            padding-bottom: 10px;
        }

        /* 側邊欄優化 */
        section[data-testid="stSidebar"] {
            background-color: #f8f9fa;
        }
        
        /* 指標卡片 (Metric Cards) */
        .metric-card {
            background: white;
            padding: 20px;
            border-radius: 12px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.05);
            border: 1px solid #e0e0e0;
            text-align: center;
            transition: transform 0.2s;
        }
        .metric-card:hover {
            transform: translateY(-2px);
            box-shadow: 0 10px 15px rgba(0,0,0,0.1);
        }
        .metric-label {
            color: #6c757d;
            font-size: 0.85rem;
            text-transform: uppercase;
            letter-spacing: 1px;
            margin-bottom: 5px;
            font-weight: 600;
        }
        .metric-value {
            color: #2c3e50;
            font-size: 1.8rem;
            font-weight: 800;
            margin: 0;
        }
        .metric-sub {
            font-size: 0.8rem;
            color: #28a745;
            margin-top: 5px;
        }
        
        /* 表格優化 */
        [data-testid="stDataFrame"] {
            border: 1px solid #eee;
            border-radius: 8px;
            overflow: hidden;
        }
        
        /* 按鈕優化 */
        .stButton button {
            border-radius: 8px;
            font-weight: 600;
            border: none;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            transition: all 0.2s;
        }
        .stButton button:hover {
            box-shadow: 0 4px 8px rgba(0,0,0,0.15);
            transform: translateY(-1px);
        }
        </style>
    """, unsafe_allow_html=True)

inject_custom_css()

# ==========================================
# 2. 工具函式
# ==========================================
def get_taiwan_time():
    return datetime.now(timezone.utc) + timedelta(hours=8)

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
    key_file = 'service_account.json'
    
    import os
    if not os.path.exists(key_file):
        st.error(f"❌ 嚴重錯誤：找不到金鑰檔案 `{key_file}`")
        st.info("請確認您已在 Render 的 'Secret Files' 中新增此檔案，且名稱正確無誤。")
        raise FileNotFoundError(f"Missing {key_file}")
        
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name(key_file, scope)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        # 嘗試讀取 JSON 內容來除錯
        try:
            import json
            with open(key_file, 'r') as f:
                creds_data = json.load(f)
                pid = creds_data.get('project_id', 'Unknown')
                st.error(f"❌ 認證失敗 (Project ID: {pid})")
        except:
            st.error(f"❌ 認證失敗 (無法讀取 Project ID)")
            
        st.error(f"詳細錯誤訊息：{e}")
        st.code(str(e))
        raise e

# === 廣告費用庫 ===
def get_ad_costs_df(client):
    try:
        try: sheet = client.open(COST_SHEET_NAME).worksheet(AD_COST_SHEET_NAME)
        except: 
            sh = client.open(COST_SHEET_NAME)
            sheet = sh.add_worksheet(title=AD_COST_SHEET_NAME, rows=500, cols=3)
            sheet.append_row(["日期", "廣告費用", "登錄時間"])
            return pd.DataFrame(columns=["日期", "廣告費用", "登錄時間"])
        
        data = sheet.get_all_values()
        if len(data) <= 1: return pd.DataFrame(columns=["日期", "廣告費用", "登錄時間"])
        df = pd.DataFrame(data[1:], columns=data[0])
        # Clean up
        df['廣告費用'] = pd.to_numeric(df['廣告費用'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
        df['日期'] = pd.to_datetime(df['日期'], format='%Y-%m-%d', errors='coerce').dt.date
        return df
    except: return pd.DataFrame(columns=["日期", "廣告費用", "登錄時間"])

def save_ad_cost(client, target_date, cost_value):
    try:
        try: sheet = client.open(COST_SHEET_NAME).worksheet(AD_COST_SHEET_NAME)
        except: 
            sheet = client.open(COST_SHEET_NAME).add_worksheet(title=AD_COST_SHEET_NAME, rows=500, cols=3)
            sheet.append_row(["日期", "廣告費用", "登錄時間"])
        
        data = sheet.get_all_values()
        target_date_str = target_date.strftime("%Y-%m-%d")
        now_str = get_taiwan_time().strftime("%Y-%m-%d %H:%M:%S")
        
        # 尋找是否已有該日期的紀錄
        row_idx = None
        for i, row in enumerate(data):
            if i > 0 and len(row) > 0 and row[0] == target_date_str:
                row_idx = i + 1
                break
                
        if row_idx:
            sheet.update_cell(row_idx, 2, cost_value)
            sheet.update_cell(row_idx, 3, now_str)
        else:
            sheet.append_row([target_date_str, cost_value, now_str])
        return True
    except Exception as e:
        print(f"Error saving ad cost: {e}")
        return False

# === 記憶庫 ===
def get_memory_rules(client):
    try:
        try: sheet = client.open(COST_SHEET_NAME).worksheet(MEMORY_SHEET_NAME)
        except: 
            sh = client.open(COST_SHEET_NAME)
            sheet = sh.add_worksheet(title=MEMORY_SHEET_NAME, rows=100, cols=4)
            sheet.append_row(["蝦皮商品名稱", "蝦皮規格名稱", "真實SKU名稱", "真實成本"])
            return {}
        
        data = sheet.get_all_values()
        if len(data) <= 1: return {}
        rules = {}
        for row in data[1:]:
            # 支援舊版(3欄) 與 新版(4欄)
            if len(row) >= 4:
                # Key: (商品名稱, 規格名稱)
                key = (row[0].strip(), row[1].strip())
                rules[key] = {'sku': row[2], 'cost': float(row[3])}
            elif len(row) == 3:
                # 舊版資料，將規格視為空字串，或只對應名稱
                key = (row[0].strip(), "")
                rules[key] = {'sku': row[1], 'cost': float(row[2])}
        return rules
    except: return {}

def save_memory_rule(client, shopee_name, shopee_option, real_sku, real_cost):
    try:
        try: sheet = client.open(COST_SHEET_NAME).worksheet(MEMORY_SHEET_NAME)
        except: sheet = client.open(COST_SHEET_NAME).add_worksheet(title=MEMORY_SHEET_NAME, rows=100, cols=4)
        
        shopee_name = str(shopee_name).strip()
        shopee_option = str(shopee_option).strip()
        
        # 檢查是否已存在 (避免重複)
        data = sheet.get_all_values()
        exists = False
        for row in data:
            if len(row) >= 4:
                if row[0].strip() == shopee_name and row[1].strip() == shopee_option:
                    exists = True; break
            elif len(row) == 3:
                if row[0].strip() == shopee_name and shopee_option == "":
                    exists = True; break
        
        if not exists:
            # 寫入格式: 名稱, 規格, 真實SKU, 真實成本
            sheet.append_row([shopee_name, shopee_option, real_sku, real_cost])
            return True
    except: pass
    return False

def update_master_cost_sheet(client, real_sku_name, new_cost):
    """
    更新主成本表 (Cost Sheet) 中的成本
    由於 Menu_Label 是 "Name | Cost"，我們主要透過 Name 來比對。
    此功能會搜尋商品名稱並更新其成本欄位。
    """
    try:
        sheet = client.open(COST_SHEET_NAME).sheet1
        # 讀取所有資料 (注意：如果資料量非常大，這樣全讀可能會慢，但在普通規模下這是最安全的)
        data = sheet.get_all_values()
        if not data: return False
        
        headers = data[0]
        try: 
            # 嘗試找尋正確的欄位 index
            name_idx = headers.index('商品名稱') if '商品名稱' in headers else headers.index('商品')
            cost_idx = headers.index('成本')
        except: return False
        
        # real_sku_name 從介面傳來是 "商品名稱 | 成本$XXX" 或 "商品名稱"
        if " | 成本$" in real_sku_name:
            target_name = real_sku_name.split(' | 成本$')[0].strip()
        else:
            target_name = real_sku_name.strip()
            
        cell_to_update = None
        
        # 尋找目標行 (從資料的第2行開始，對應 sheet row 2)
        # sheet.update_cell 接受 (row, col) 其中 row 是從1開始
        for i, row in enumerate(data):
            if i == 0: continue # Skip header
            
            # 確保不會 index out of range
            if len(row) > name_idx:
                row_name_val = str(row[name_idx]).strip()
                # 簡單字串比對
                if row_name_val == target_name:
                    cell_to_update = (i + 1, cost_idx + 1)
                    break
        
        if cell_to_update:
            sheet.update_cell(cell_to_update[0], cell_to_update[1], new_cost)
            return True
        return False
        
    except Exception as e:
        print(f"Error updating master cost: {e}")
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
            if "撥款金額" in col or "進蝦皮錢包" in col or "進帳" in col: mapping[col] = "進蝦皮錢包"
            if "商品編碼" in col and "規格" in col: mapping[col] = "蝦皮商品編碼"
            if "規格名稱" in col: mapping[col] = "商品選項名稱" # 新增映射
            if "買家" in col and "備註" in col: mapping[col] = "買家備註"
        df.rename(columns=mapping, inplace=True)
        if '蝦皮商品編碼' in df.columns: df['蝦皮商品編碼'] = df['蝦皮商品編碼'].apply(clean_id)
        df = df.drop_duplicates()
        return df
    except Exception as e: st.error(f"Excel 解析失敗: {e}"); return None

# ==========================================
# 4. 寫入邏輯
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
        progress_bar.progress(100, text="完成！")
        return f"✅ 成功救援 {updated_count} 筆成本資料！"
    else: 
        progress_bar.progress(100, text="完成！")
        return "✅ 無需更新"

def normalize_name(name):
    """
    將名稱進行標準化處理，移除空白、全形轉半形、統一大小寫
    """
    if not isinstance(name, str): return str(name)
    name = name.strip().lower()
    # 移除所有空白 (包含全形空白)
    name = name.replace(" ", "").replace("　", "")
    # 標點符號標準化
    name = name.replace("，", ",").replace("（", "(").replace("）", ")").replace("【", "[").replace("】", "]")
    return name

def process_orders(df_sales, df_cost, progress_bar):
    required_cols = ['訂單編號', '商品名稱']
    for col in required_cols:
        if col not in df_sales.columns: return f"❌ 失敗：報表找不到『{col}』。"

    progress_bar.progress(10, text="資料清理...")
    if '訂單狀態' in df_sales.columns:
        df_sales = df_sales[df_sales['訂單狀態'].astype(str).str.strip() != '不成立']
    
    progress_bar.progress(30, text="計算利潤...")
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

    # Fix: User requested to modify profit calculation formula to subtract TOTAL cost directly, without multiplying by quantity.
    # This implies the imported cost is treated as total cost per line item or user preference.
    df_merged['總利潤'] = df_merged['進蝦皮錢包'] - df_merged['成本']
    
    progress_bar.progress(50, text=f"比對 {DB_SHEET_NAME}...")
    client = get_gspread_client()
    try: db_sheet = client.open(DB_SHEET_NAME).sheet1
    except: return f"❌ 找不到資料庫：{DB_SHEET_NAME}"
    
    headers = ['訂單編號', '訂單成立日期', '商品名稱', '商品選項名稱', '數量', '售價', '成交手續費', '金流與系統處理費', '其他服務費', '蝦皮付費總金額', '進蝦皮錢包', '成本', '總利潤', '蝦皮商品編碼', '買家備註', '資料備份時間', '備註']
    
    df_upload_ready = df_merged.copy()
    df_upload_ready['資料備份時間'] = get_taiwan_time().strftime("%Y-%m-%d %H:%M:%S")
    df_upload_ready['備註'] = "" 
    
    memory_rules = get_memory_rules(client)
    if '商品名稱' in df_upload_ready.columns:
        mask_special = df_upload_ready['商品名稱'].astype(str).apply(lambda x: any(sp in x for sp in SPECIAL_PRODUCTS))
        df_upload_ready.loc[mask_special, '備註'] = "待人工確認"
        df_upload_ready.loc[mask_special, '總利潤'] = 0
        
        # 建立成本查詢表 (for Smart Match)
        name_cost_map = {}
        normalized_cost_map = {} # 新增：標準化查詢表
        
        if not df_cost.empty and '商品名稱' in df_cost.columns and '成本' in df_cost.columns:
            for _, r in df_cost.iterrows():
                raw_name = str(r['商品名稱']).strip()
                cost_val = float(r['成本'])
                name_cost_map[raw_name] = cost_val
                
                # 建立模糊比對鍵值
                norm_name = normalize_name(raw_name)
                normalized_cost_map[norm_name] = {'cost': cost_val, 'sku': raw_name}

        for idx, row in df_upload_ready[mask_special].iterrows():
            p_name = str(row['商品名稱']).strip()
            p_opt = str(row['商品選項名稱']).strip()
            
            found_cost = None
            found_sku = None
            source_type = ""
            
            # 優先嘗試完全匹配 (名稱 + 規格)
            if (p_name, p_opt) in memory_rules:
                rule = memory_rules[(p_name, p_opt)]
                found_cost = rule['cost']
                found_sku = rule['sku']
                source_type = "記憶"
            # 嘗試反向兼容 (只匹配名稱，且記憶庫中規格為空)
            elif (p_name, "") in memory_rules:
                rule = memory_rules[(p_name, "")]
                found_cost = rule['cost']
                found_sku = rule['sku']
                source_type = "記憶"
            
            # === 智能匹配 (Smart Match) ===
            # 如果記憶庫沒找到，嘗試直接從成本表 (df_cost) 找對應名稱
            else:
                # 嘗試組合: "商品名稱 [規格名稱]", "商品名稱"
                candidates = []
                if p_opt: candidates.append(f"{p_name} [{p_opt}]")
                candidates.append(p_name)
                
                for cand in candidates:
                    # 方法 A: 精確比對
                    if cand in name_cost_map:
                        found_cost = name_cost_map[cand]
                        found_sku = cand 
                        source_type = "智能"
                        break
                    
                    # 方法 B: 模糊比對 (忽略空白、標點)
                    cand_norm = normalize_name(cand)
                    if cand_norm in normalized_cost_map:
                        found_cost = normalized_cost_map[cand_norm]['cost']
                        found_sku = normalized_cost_map[cand_norm]['sku']
                        source_type = "智能(模糊)"
                        break
            
            if found_cost is not None:
                real_cost = found_cost
                income = float(row['進蝦皮錢包'])
                # real_profit = income - real_cost # Original line
                real_profit = income - real_cost # Modified formula applied above to all, but here we overwrite special orders
                
                df_upload_ready.at[idx, '成本'] = real_cost
                df_upload_ready.at[idx, '總利潤'] = real_profit
                df_upload_ready.at[idx, '備註'] = f"已歸戶({source_type}): {found_sku}"

    for h in headers:
        if h not in df_upload_ready.columns: df_upload_ready[h] = ""
    df_upload_ready = df_upload_ready[headers].fillna('').astype(str)
    
    # === Smart Merge Logic ===
    existing_data = db_sheet.get_all_values()
    
    if len(existing_data) <= 1:
        # Initial Write
        db_sheet.clear(); db_sheet.append_row(headers); db_sheet.append_rows(df_upload_ready.values.tolist())
        return f"✅ 初始化完成！新增 {len(df_upload_ready)} 筆。"
    else:
        # Load existing data
        df_existing = pd.DataFrame(existing_data[1:], columns=existing_data[0])
        
        # Clean existing columns headers to avoid mismatch
        df_existing.columns = df_existing.columns.astype(str).str.strip().str.replace('\n', '')
        
        # Ensure new headers like '買家備註' are injected if the database is using an old format
        for h in headers:
            if h not in df_existing.columns:
                df_existing[h] = ""
        
        # Ensure Order IDs are strings for comparison
        df_existing['訂單編號'] = df_existing['訂單編號'].astype(str).str.strip()
        df_upload_ready['訂單編號'] = df_upload_ready['訂單編號'].astype(str).str.strip()
        
        # Create a dictionary of existing orders for fast lookup: {ID: Row}
        existing_dict = df_existing.set_index('訂單編號').to_dict('index')
        
        new_records = []
        sync_logs = []  # To capture what is being synced
        updated_count = 0
        skipped_count = 0
        
        for idx, row in df_upload_ready.iterrows():
            order_id = row['訂單編號']
            
            if order_id in existing_dict:
                # Order exists
                old_row = existing_dict[order_id]
                old_note = str(old_row.get('備註', ''))
                
                if "已歸戶" in old_note:
                    # Case 1: Already consolidated -> PROTECT, but SYNC Date/Status
                    skipped_count += 1
                    target_idx = df_existing.index[df_existing['訂單編號'] == order_id]
                    if not target_idx.empty:
                        # Sync crucial fields if they exist in schema
                        sync_fields = ['訂單成立日期', '訂單狀態', '商品名稱', '買家備註']
                        for field in sync_fields:
                            if field in df_existing.columns and field in row:
                                val_str = str(row[field])
                                df_existing.at[target_idx[0], field] = val_str
                                if field == '訂單成立日期':
                                    sync_logs.append(f"🔄 [Sync] {order_id} 日期更新: {val_str}")
                else:
                    # Case 2: Not consolidated -> UPDATE
                    target_idx = df_existing.index[df_existing['訂單編號'] == order_id]
                    if not target_idx.empty:
                        df_existing.loc[target_idx[0]] = row
                        updated_count += 1
            else:
                # Case 3: New Order -> ADD
                new_records.append(row)
        
        # DEBUG: Show counts and details
        with st.expander("🕵️ Upload Debug Info (上傳診斷)", expanded=True):
            st.write(f"📂 讀取到的 Excel 列數: {len(df_sales)}")
            st.write(f"🧹 清理後準備寫入的列數: {len(df_upload_ready)}")
            st.write("📋 準備寫入的前 3 筆 ID:", df_upload_ready['訂單編號'].head(3).tolist())
            
            st.write(f"🗄️ 資料庫現有筆數: {len(df_existing)}")
            st.write(f"📊 判定結果 - 新增: {len(new_records)}, 更新: {updated_count}, 略過: {skipped_count}")
            
            if skipped_count > 0:
                st.warning(f"⚠️ 發現 {skipped_count} 筆重複資料被略過 (因為已歸戶)")
                # Find first skipped example
                for idx, row in df_upload_ready.iterrows():
                    oid = row['訂單編號']
                    if oid in existing_dict:
                        old_note = str(existing_dict[oid].get('備註', ''))
                        if "已歸戶" in old_note:
                            st.write(f"範例略過 ID: {oid} (備註: {old_note})")
                            break
            
            if len(sync_logs) > 0:
                st.write("🔄 同步日誌 (Sync Logs):")
                for log in sync_logs[:5]: # Show first 5 logs
                    st.text(log)
                if len(sync_logs) > 5: st.text(f"... 以及其他 {len(sync_logs)-5} 筆")
            else:
                st.write("⚠️ 無日期同步記錄 (可能是欄位名稱不符或資料已一致)")
                st.write(f"系統檢查到的欄位: {df_existing.columns.tolist()[:10]}...") # Debug columns
            
            if updated_count > 0:
                st.info(f"ℹ️ 更新了 {updated_count} 筆既有資料")
                
            if len(new_records) == 0:
                st.error("❌ 警告：判定為 0 筆新資料！請檢查上方 '準備寫入的前 3 筆 ID' 是否真的已存在於資料庫。")

        # Combine Existing (Updated) + New Records
        
        # Combine Existing (Updated) + New Records
        if new_records:
            df_new = pd.DataFrame(new_records)
            df_final = pd.concat([df_existing, df_new], ignore_index=True)
        else:
            df_final = df_existing

        # Write back to sheet (Overwrite everything to ensure updates are reflected)
        # Using clear and update is safer for consistency than appending mixed
        progress_bar.progress(90, text="正在同步資料庫...")
        
        # Convert to list of lists
        final_data = [df_final.columns.tolist()] + df_final.astype(str).values.tolist()
        db_sheet.clear()
        try:
            # Try new argument name first (gspread v6)
            db_sheet.update(range_name='A1', values=final_data)
        except:
            # Fallback for older gspread
            db_sheet.update('A1', final_data)
        
        # === Read-Back Verification ===
        st.write("🔎 正在驗證寫入結果...")
        # Check specific order if synced
        if len(sync_logs) > 0:
            # Extract first synced ID from logs
            first_synced_id = sync_logs[0].split('] ')[1].split(' ')[0]
            # Re-read sheet
            check_data = db_sheet.get_all_records()
            check_df = pd.DataFrame(check_data)
            # Find the row
            check_row = check_df[check_df['訂單編號'].astype(str) == first_synced_id]
            if not check_row.empty:
                saved_date = check_row.iloc[0]['訂單成立日期']
                st.success(f"✅ 寫入驗證成功！資料庫內 ID: {first_synced_id} 的日期已變更為: {saved_date}")
            else:
                st.error(f"❌ 寫入驗證失敗：無法在資料庫中找到剛剛同步的 ID {first_synced_id}")
        
        progress_bar.progress(100, text="完成")
        st.cache_data.clear() # Force clear cache to ensure frontend sees new data immediately
        return f"✅ 同步完成！新增 {len(new_records)} 筆，更新 {updated_count} 筆，保留 {skipped_count} 筆已歸戶資料。"

def update_special_order(order_sn, real_sku_name, real_cost, df_db, db_sheet):
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
st.sidebar.markdown("### 🚀 功能選單")
if "sb_mode" not in st.session_state: st.session_state["sb_mode"] = "📊 前台戰情室"
mode = st.sidebar.radio("功能選單", ["📊 前台戰情室", "⚙️ 後台管理", "🔍 成本神探"], key="sb_mode", label_visibility="collapsed")
st.sidebar.markdown("---")
st.sidebar.caption("Ver 10.7.4 (Pro) | Update: 2026-01-19 11:15")

if mode == "🔍 成本神探":
    st.title("🔍 成本神探")
    st.info("此功能用於快速檢查成本表的商品編碼狀態。")
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
    
    if st.sidebar.button("🔄 刷新資料"):
        st.cache_data.clear(); st.rerun()

    client = get_gspread_client()
    try:
        sheet = client.open(DB_SHEET_NAME).sheet1
        data = sheet.get_all_values()
        if len(data) > 1:
            df_all = pd.DataFrame(data[1:], columns=data[0])
            for c in ['售價', '成本', '數量', '總利潤', '進蝦皮錢包']:
                if c in df_all.columns: df_all[c] = pd.to_numeric(df_all[c].astype(str).str.replace(',',''), errors='coerce').fillna(0)
        else: st.warning("資料庫目前為空"); st.stop()
    except gspread.exceptions.SpreadsheetNotFound:
        st.error(f"❌ 找不到 Google Sheet：『{DB_SHEET_NAME}』")
        st.info("請確認：\n1. 是否已建立名為『蝦皮訂單總表』的試算表\n2. 是否已將試算表共用給機器人信箱")
        st.stop()
    except Exception as e:
        st.error(f"讀取 Google Sheet 失敗。\n錯誤訊息：{e}")
        st.stop()

    if df_all is not None:
        if '備註' not in df_all.columns: df_all['備註'] = ""
        if '訂單成立日期' in df_all.columns:
            df_all['訂單成立日期'] = pd.to_datetime(df_all['訂單成立日期'], errors='coerce')
            
            # Check for parsing failures
            invalid_count = df_all['訂單成立日期'].isna().sum()
            if invalid_count > 0:
                st.warning(f"⚠️ 偵測到 {invalid_count} 筆資料日期格式錯誤 (無法解析)，已自動濾除。此問題可能導致最新訂單無法顯示。")
                
            df_all.dropna(subset=['訂單成立日期'], inplace=True) # Clean invalid dates
            df_all['日期標籤'] = df_all['訂單成立日期'].dt.strftime('%Y-%m-%d')
        else: st.error("資料庫缺少『訂單成立日期』欄位"); st.stop()

        # === 全新升級：日期篩選器 ===
        st.markdown("### 📅 日期篩選器")
        col_quick, col_date_range = st.columns([1, 2])

        with col_quick:
            st.markdown("**快速選擇**")
            quick_col1, quick_col2 = st.columns(2)
            with quick_col1:
                if st.button("今日", use_container_width=True):
                    st.session_state['date_start'] = get_taiwan_time().date()
                    st.session_state['date_end'] = get_taiwan_time().date()
                if st.button("昨日", use_container_width=True):
                    yesterday = get_taiwan_time().date() - timedelta(days=1)
                    st.session_state['date_start'] = yesterday
                    st.session_state['date_end'] = yesterday
            with quick_col2:
                if st.button("本月", use_container_width=True):
                    today = get_taiwan_time().date()
                    st.session_state['date_start'] = today.replace(day=1)
                    st.session_state['date_end'] = today
                if st.button("上月", use_container_width=True):
                    today = get_taiwan_time().date()
                    # Calculate first day of this month, then substract 1 day to get last month end
                    last_month_end = today.replace(day=1) - timedelta(days=1)
                    last_month_start = last_month_end.replace(day=1)
                    st.session_state['date_start'] = last_month_start
                    st.session_state['date_end'] = last_month_end

        with col_date_range:
            st.markdown("**自訂範圍**")
            col_start, col_end = st.columns(2)
            # Default to today if not set, or min/max from data
            min_date = df_all['訂單成立日期'].min().date() if not df_all['訂單成立日期'].isnull().all() else get_taiwan_time().date()
            max_date = df_all['訂單成立日期'].max().date() if not df_all['訂單成立日期'].isnull().all() else get_taiwan_time().date()
            
            # Initialize session state if not present
            if 'date_start' not in st.session_state: st.session_state['date_start'] = min_date
            if 'date_end' not in st.session_state: st.session_state['date_end'] = max_date

            with col_start:
                start_date = st.date_input("起始日期", value=st.session_state['date_start'])
            with col_end:
                end_date = st.date_input("結束日期", value=st.session_state['date_end'])

        # 資料篩選
        df_filtered = df_all[
            (df_all['訂單成立日期'].dt.date >= start_date) & 
            (df_all['訂單成立日期'].dt.date <= end_date)
        ]
        
        # Debug Mode (Moved Here)
        with st.expander("🕵️ Debug Mode (資料診斷)", expanded=True):
            c_dbg1, c_dbg2 = st.columns(2)
            with c_dbg1:
                st.write(f"📊 原始資料: {len(df_all)} 筆")
                st.write(f"📅 資料庫最新日期: {df_all['訂單成立日期'].max()}")
            with c_dbg2:
                st.write(f"🔍 篩選後資料: {len(df_filtered)} 筆")
                st.write(f"📆 目前篩選範圍: {start_date} ~ {end_date}")
            
            if df_filtered.empty and not df_all.empty:
                last_date = df_all['訂單成立日期'].max().date()
                if last_date < start_date:
                    st.warning(f"⚠️ 您的資料庫最新訂單只到 `{last_date}`，但您選了 `{start_date}` 之後的日期。請嘗試選擇「昨日」或「本月」。")
        
        if df_filtered.empty:
            st.warning(f"⚠️ 該日期區間 ({start_date} ~ {end_date}) 無資料")
        else:
            df_day = df_filtered # Use filtered data as the main dataset
            
            # 分離特殊與正常訂單
            mask_special = (
                df_day['商品名稱'].astype(str).apply(lambda x: any(sp in x for sp in SPECIAL_PRODUCTS)) & 
                (~df_day['備註'].astype(str).str.contains("已歸戶"))
            )
            df_special = df_day[mask_special]
            df_normal = df_day[~df_day.index.isin(df_special.index)]
            
            # 計算核心指標
            total_rev = df_normal['售價'].sum()
            total_cost = (df_normal['成本'] * df_normal['數量']).sum()
            
            # 讀取廣告費用
            ad_df = get_ad_costs_df(client)
            period_ad_cost = 0
            if not ad_df.empty:
                mask = (ad_df['日期'] >= start_date) & (ad_df['日期'] <= end_date)
                period_ad_cost = ad_df.loc[mask, '廣告費用'].sum()
            
            total_gp = df_normal['總利潤'].sum() - period_ad_cost
            margin = (total_gp / total_rev * 100) if total_rev > 0 else 0
            
            # --- 視覺化指標卡片 ---
            cols = st.columns(5)
            metrics = [
                ("💰 當日營收", f"${total_rev:,.0f}", ""),
                ("📉 商品成本", f"${total_cost:,.0f}", ""),
                ("📢 廣告費用", f"${period_ad_cost:,.0f}", ""),
                ("💸 淨毛利", f"${total_gp:,.0f}", "核心獲利"),
                ("📊 毛利率", f"{margin:.1f}%", "Profit Margin")
            ]
            
            for col, (label, val, sub) in zip(cols, metrics):
                with col:
                    st.markdown(f"""
                    <div class="metric-card">
                        <div class="metric-label">{label}</div>
                        <div class="metric-value">{val}</div>
                        <div class="metric-sub">{sub}</div>
                    </div>
                    """, unsafe_allow_html=True)
            
            st.markdown("---")
            
            # === 視覺化圖表區塊 ===
            st.markdown("### 📊 營運數據透視")
            v_tab1, v_tab2, v_tab3 = st.tabs(["📈 營業額趨勢", "🍰 商品結構分析", "🏆 熱賣排行榜"])
            
            with v_tab1:
                # 折線圖：每日營業額 & 利潤
                daily_stats = df_day.groupby('日期標籤').agg({
                    '售價': 'sum',
                    '總利潤': 'sum'
                }).reset_index()
                daily_stats.columns = ['日期', '營業額', '利潤']
                
                fig = go.Figure()
                fig.add_trace(go.Scatter(x=daily_stats['日期'], y=daily_stats['營業額'], mode='lines+markers', name='營業額', line=dict(color='#FF6B6B', width=3)))
                fig.add_trace(go.Scatter(x=daily_stats['日期'], y=daily_stats['利潤'], mode='lines+markers', name='利潤', line=dict(color='#4ECDC4', width=3)))
                fig.update_layout(title="每日營收與獲利趨勢", height=400, hovermode="x unified")
                st.plotly_chart(fig, use_container_width=True)
                
            with v_tab2:
                # 圓餅圖：商品銷售佔比
                prod_stats = df_day.groupby('商品名稱')['售價'].sum().reset_index().sort_values('售價', ascending=False)
                # 取前5名，其他合併
                if len(prod_stats) > 5:
                    top5 = prod_stats.head(5)
                    others_val = prod_stats.iloc[5:]['售價'].sum()
                    others_df = pd.DataFrame([{'商品名稱': '其他商品', '售價': others_val}])
                    pie_df = pd.concat([top5, others_df])
                else:
                    pie_df = prod_stats
                
                fig_pie = px.pie(pie_df, values='售價', names='商品名稱', title='各商品銷售額佔比', color_discrete_sequence=px.colors.qualitative.Pastel)
                fig_pie.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig_pie, use_container_width=True)
                
            with v_tab3:
                # 長條圖：Top 10 熱賣
                top10_stats = df_day.groupby('商品名稱').agg({'售價': 'sum', '總利潤': 'sum', '數量':'sum'}).reset_index().sort_values('售價', ascending=False).head(10)
                fig_bar = px.bar(top10_stats, x='售價', y='商品名稱', orientation='h', title='Top 10 熱賣商品 (按營業額)', text='售價', color='總利潤')
                fig_bar.update_layout(yaxis={'categoryorder':'total ascending'}, height=500)
                st.plotly_chart(fig_bar, use_container_width=True)

            # === 利潤警示系統 ===
            st.markdown("---")
            st.markdown("### ⚠️ 異常訂單警示")
            
            # 定義規則
            low_margin_orders = df_day[(df_day['總利潤'] / df_day['售價'] < 0.1) & (df_day['總利潤'] > 0)]
            loss_orders = df_day[df_day['總利潤'] < 0]
            high_value_orders = df_day[df_day['售價'] > 5000]
            
            ac1, ac2, ac3 = st.columns(3)
            ac1.metric("🟡 低利潤率 (<10%)", f"{len(low_margin_orders)} 筆", delta_color="off")
            ac2.metric("🔴 虧損訂單 (<0)", f"{len(loss_orders)} 筆", delta_color="inverse")
            ac3.metric("🔵 高單價 (>5000)", f"{len(high_value_orders)} 筆", delta_color="off")
            
            if not low_margin_orders.empty:
                with st.expander(f"🟡 查看 {len(low_margin_orders)} 筆低利潤訂單"):
                    st.dataframe(low_margin_orders[['訂單成立日期','訂單編號','商品名稱','數量','售價','進蝦皮錢包','成本','總利潤']], use_container_width=True)
            
            if not loss_orders.empty:
                with st.expander(f"🔴 查看 {len(loss_orders)} 筆虧損訂單"):
                    st.dataframe(loss_orders[['訂單成立日期','訂單編號','商品名稱','數量','售價','進蝦皮錢包','成本','總利潤']], use_container_width=True)

            if not high_value_orders.empty:
                with st.expander(f"🔵 查看 {len(high_value_orders)} 筆高額訂單"):
                    st.dataframe(high_value_orders[['訂單成立日期','訂單編號','商品名稱','售價','總利潤']], use_container_width=True)
            
            st.markdown("---")
            
            # --- 特殊訂單警示 ---
            if not df_special.empty:
                st.error(f"⚠️ 發現 {len(df_special)} 筆訂單尚未歸戶 (不會計入毛利)")
                # 載入成本表供選擇
                df_cost_ref, _ = load_cloud_cost_table()
                cost_dict = {}
                item_options = ["請選擇商品..."]
                if df_cost_ref is not None:
                    cost_dict = pd.Series(df_cost_ref.成本.values, index=df_cost_ref.Menu_Label).to_dict()
                    item_options = ["請選擇商品..."] + list(cost_dict.keys())

                st.markdown("👇 **您可以直接在下方選擇商品進行快速歸戶：**")
                
                if st.button("🚀 全部一鍵歸戶 (Batch Confirm)", type="primary", use_container_width=True):
                    success_count = 0
                    fail_count = 0
                    
                    progress_bar = st.progress(0, text="正在批次處理中...")
                    
                    for i, (idx, row) in enumerate(df_special.iterrows()):
                        order_sn = row['訂單編號']
                        # 從 session_state 獲取當前選擇的值
                        sel_key = f"dash_sel_{order_sn}"
                        
                        # 檢查是否有選擇商品
                        if sel_key in st.session_state:
                            real_item = st.session_state[sel_key]
                            
                            if real_item != "請選擇商品...":
                                # 嘗試獲取成本 (需組裝 key)
                                cost_key = f"dash_cost_{order_sn}_{str(real_item)}"
                                final_cost = 0
                                if cost_key in st.session_state:
                                    final_cost = st.session_state[cost_key]
                                
                                # 執行歸戶
                                try:
                                    real_sku_name = real_item.split(" |")[0].strip()
                                    if update_special_order(order_sn, real_sku_name, final_cost, df_all, sheet):
                                        # 自動記憶
                                        if "7777" not in str(row['商品名稱']):
                                            save_memory_rule(client, row['商品名稱'], row.get('商品選項名稱', ''), real_sku_name, final_cost)
                                        success_count += 1
                                    else:
                                        fail_count += 1
                                except Exception as e:
                                    print(f"Batch Error: {e}")
                                    fail_count += 1
                        
                        # Update progress
                        progress_bar.progress((i + 1) / len(df_special), text=f"處理中... ({i + 1}/{len(df_special)})")
                    
                    progress_bar.empty()
                    if success_count > 0:
                        st.success(f"✅ 成功歸戶 {success_count} 筆訂單！")
                        if fail_count > 0:
                            st.warning(f"⚠️ {fail_count} 筆處理失敗")
                        time.sleep(1.5)
                        st.rerun()
                    else:
                        st.warning("⚠️ 沒有檢測到可歸戶的訂單，請先選擇商品！")

                # 標題列
                h1, h2, h3, h4 = st.columns([1.5, 2, 1, 1])
                h1.markdown("**訂單資訊**")
                h2.markdown("**選擇對應商品**")
                h3.markdown("**確認成本**")
                h4.markdown("**執行**")
                st.markdown("---")

                for i, row in df_special.iterrows():
                    with st.container():
                        c1, c2, c3, c4 = st.columns([1.5, 2, 1, 1])
                        
                        # 1. 訂單資訊
                        with c1:
                            st.caption(f"{row['訂單成立日期']} | ${row['售價']}")
                            st.markdown(f"**{row['商品名稱']}**")
                            st.caption(f"ID: {row['訂單編號']}")
                        
                        # 2. 選擇商品
                        with c2:
                            # 嘗試自動匹配 (從 memory rule 或 fuzzy logic) -> 這裡先簡單 default nothing
                            # Smart Match Logic for UI defaults could be complex, keeping it simple first
                            real_item = st.selectbox(
                                "選擇對應商品", 
                                item_options, 
                                key=f"dash_sel_{row['訂單編號']}", 
                                label_visibility="collapsed"
                            )
                        
                        # 3. 成本輸入
                        with c3:
                            # Auto-fill cost logic
                            default_cost = 0
                            if real_item != "請選擇商品..." and real_item in cost_dict:
                                default_cost = int(cost_dict[real_item])
                            
                            # Use dynamic key to force update when selectbox changes
                            final_cost = st.number_input(
                                "成本", 
                                value=default_cost, 
                                step=1, 
                                format="%d",  
                                key=f"dash_cost_{row['訂單編號']}_{str(real_item)}",
                                label_visibility="collapsed"
                            )

                        # 4. 按鈕
                        with c4:
                            if st.button("✅ 歸戶", key=f"dash_btn_{row['訂單編號']}", use_container_width=True):
                                if real_item == "請選擇商品...":
                                    st.toast("⚠️ 請先選擇商品")
                                else:
                                    # 執行歸戶邏輯
                                    try:
                                        # 解析 SKU
                                        real_sku_name = real_item.split(" |")[0].strip()
                                        
                                        # 更新資料庫
                                        if update_special_order(row['訂單編號'], real_sku_name, final_cost, df_all, sheet): # Fix: pass df_all (dataframe) and sheet (worksheet)
                                            # 自動記憶 (預設開啟)
                                            if "7777" not in str(row['商品名稱']):
                                                save_memory_rule(client, row['商品名稱'], row.get('商品選項名稱', ''), real_sku_name, final_cost)
                                            
                                            # 同步成本表
                                            if final_cost != default_cost or default_cost == 0:
                                                update_master_cost_sheet(client, real_item, final_cost)
                                            
                                            st.success("歸戶成功！")
                                            time.sleep(1)
                                            st.rerun()
                                        else:
                                            st.error("更新失敗")
                                    except Exception as e:
                                        st.error(f"錯誤: {e}")
                        st.markdown("---")
            
            # --- 視覺化圖表區 ---
            c_chart1, c_chart2 = st.columns(2)
            
            with c_chart1:
                st.markdown("##### 🏆 熱銷商品 (依營收)")
                if not df_normal.empty:
                    top_items = df_normal.groupby('商品名稱')['售價'].sum().nlargest(5).sort_values()
                    st.bar_chart(top_items, color="#FF512F")
                else:
                    st.info("無資料")
                    
            with c_chart2:
                st.markdown("##### 💎 高毛利商品 (依利潤)")
                if not df_normal.empty:
                    top_profits = df_normal.groupby('商品名稱')['總利潤'].sum().nlargest(5).sort_values()
                    st.bar_chart(top_profits, color="#DD2476")
                else:
                    st.info("無資料")
            
            st.divider()

            # --- 詳細資料表 ---
            st.subheader("📦 銷售明細表")
            cols_show = ['商品名稱', '數量', '售價', '成本', '總利潤', '訂單編號']
            final_show = [c for c in cols_show if c in df_normal.columns]
            
            st.dataframe(
                df_normal[final_show],
                use_container_width=True,
                column_config={
                    "售價": st.column_config.NumberColumn("售價", format="$%d"),
                    "成本": st.column_config.NumberColumn("成本", format="$%d"),
                    "總利潤": st.column_config.NumberColumn("總利潤", format="$%d"),
                    "數量": st.column_config.NumberColumn("數量", width="small"),
                },
                hide_index=True
            )

elif mode == "⚙️ 後台管理":
    st.title("⚙️ 後台管理中心")
    
    # 自動帶入已記憶的密碼
    def_pwd = st.session_state.get("saved_pwd", "")
    pwd = st.text_input("🔑 請輸入管理員密碼", type="password", value=def_pwd)
    
    if pwd == ADMIN_PWD:
        # 使用更美觀的 Tabs
        st.markdown("###")
        tab1, tab2, tab3, tab4, tab5 = st.tabs(["📥 訂單上傳", "🔗 歸戶系統", "🛠️ 商品維護", "🤝 非蝦皮訂單", "📢 廣告費用管理"])

        with tab1:
            st.info("請上傳蝦皮匯出的 `Order.all.xlsx` 報表，系統會自動計算成本與利潤。")
            
            c1, c2 = st.columns([1, 1])
            with c1:
                # 檢查成本表狀態
                st.markdown("**系統狀態檢測**")
                df_cost, _ = load_cloud_cost_table()
                if df_cost is not None:
                    st.success(f"✅ 成本表連線正常 (共 {len(df_cost)} 筆資料)")
                else:
                    st.error("❌ 無法讀取成本表")

            with c2:
                sales_file = st.file_uploader("拖曳或點擊上傳 Excel", type=['xlsx'])
                
            if sales_file:
                # 檔案名稱防呆機制
                if not sales_file.name.lower().startswith("order.all"):
                    st.error("❌ 檔案錯誤：請上傳檔名以 `Order.all` 開頭的蝦皮原始報表！")
                    st.info("💡 提示：蝦皮匯出的檔名通常為 `Order.all.YYYYMMDD.xlsx`")
                elif st.button("🚀 開始分析訂單", type="primary", use_container_width=True):
                    bar = st.progress(0, "初始化中...")
                    df_sales = load_sales_report(sales_file)
                    if df_sales is not None:
                        res = process_orders(df_sales, df_cost, bar)
                        time.sleep(0.5)
                        if "成功" in res:
                            st.success(res)
                            time.sleep(1.5) # Wait for cache clear signal
                            st.cache_data.clear()
                            st.rerun()
                        else: st.warning(res)
                        st.cache_data.clear() # Redundant safety clear

        with tab2:
            st.markdown("#### 🔗 特殊訂單歸戶 (信用卡/補差價/客製化)")
            
            client = get_gspread_client()
            try:
                db_sheet = client.open(DB_SHEET_NAME).sheet1
                data = db_sheet.get_all_values()
                if len(data) > 1: df_db = pd.DataFrame(data[1:], columns=data[0])
                else: st.warning("目前無訂單資料"); st.stop()
            except: st.error("資料讀取失敗"); st.stop()
            
            if '備註' not in df_db.columns: df_db['備註'] = ""
            if '買家備註' not in df_db.columns: df_db['買家備註'] = ""
            mask = (
                df_db['商品名稱'].astype(str).apply(lambda x: any(sp in x for sp in SPECIAL_PRODUCTS)) & 
                (~df_db['備註'].astype(str).str.contains("已歸戶"))
            )
            pending = df_db[mask]
            
            # Check for pending orders

            if pending.empty:
                st.balloons()
                st.success("🎉 太棒了！目前所有特殊訂單都已完成歸戶。")
            else:
                st.info(f"💡 系統偵測到有 {len(pending)} 筆待處理特殊訂單，您可透過下方篩選器縮小範圍。")
                
                if '訂單成立日期' in pending.columns:
                    pending['訂單成立日期_dt'] = pd.to_datetime(pending['訂單成立日期'], errors='coerce')
                    min_d = pending['訂單成立日期_dt'].min().date() if not pending['訂單成立日期_dt'].isnull().all() else get_taiwan_time().date()
                    max_d = pending['訂單成立日期_dt'].max().date() if not pending['訂單成立日期_dt'].isnull().all() else get_taiwan_time().date()
                else:
                    min_d = max_d = get_taiwan_time().date()
                
                # --- 日期篩選區塊 ---
                st.markdown("##### 📅 篩選待處理訂單")
                sc1, sc2 = st.columns([1, 2])
                with sc1:
                    sq1, sq2 = st.columns(2)
                    with sq1:
                        if st.button("今日", key="btn_sp_today", use_container_width=True):
                            st.session_state['sp_start'] = get_taiwan_time().date()
                            st.session_state['sp_end'] = get_taiwan_time().date()
                        if st.button("昨日", key="btn_sp_yest", use_container_width=True):
                            yest = get_taiwan_time().date() - timedelta(days=1)
                            st.session_state['sp_start'] = yest
                            st.session_state['sp_end'] = yest
                    with sq2:
                        if st.button("本月", key="btn_sp_tmonth", use_container_width=True):
                            today = get_taiwan_time().date()
                            st.session_state['sp_start'] = today.replace(day=1)
                            st.session_state['sp_end'] = today
                        if st.button("全部待處理", key="btn_sp_all", use_container_width=True):
                            st.session_state['sp_start'] = min_d
                            st.session_state['sp_end'] = max_d
                
                if 'sp_start' not in st.session_state: st.session_state['sp_start'] = min_d
                if 'sp_end' not in st.session_state: st.session_state['sp_end'] = max_d
                
                with sc2:
                    sd1, sd2 = st.columns(2)
                    with sd1:
                        sp_start = st.date_input("起始日期", value=st.session_state['sp_start'], key="sp_start_in")
                    with sd2:
                        sp_end = st.date_input("結束日期", value=st.session_state['sp_end'], key="sp_end_in")
                
                st.session_state['sp_start'] = sp_start
                st.session_state['sp_end'] = sp_end

                if '訂單成立日期_dt' in pending.columns:
                    pending_filtered = pending[(pending['訂單成立日期_dt'].dt.date >= sp_start) & (pending['訂單成立日期_dt'].dt.date <= sp_end)]
                else:
                    pending_filtered = pending
                
                if pending_filtered.empty:
                    st.warning(f"⚠️ 該區間 ({sp_start} ~ {sp_end}) 內目前無待歸戶的特殊訂單。")
                else:
                    st.success(f"📌 篩選後共有 {len(pending_filtered)} 筆特殊訂單待歸戶，請直接在下方表格編輯：")
                    df_cost_ref, _ = load_cloud_cost_table()
                    
                    if df_cost_ref is not None:
                        cost_dict = pd.Series(df_cost_ref.成本.values, index=df_cost_ref.Menu_Label).to_dict()
                        options = ["請選擇對應的真實商品..."] + list(cost_dict.keys())
                        
                        # 1. 準備編輯用的 DataFrame
                        show_cols = [c for c in ['訂單成立日期', '訂單編號', '商品名稱', '商品選項名稱', '進蝦皮錢包', '買家備註'] if c in pending_filtered.columns]
                        df_editor = pending_filtered[show_cols].copy()
                        
                        # 新增編輯欄位 (預設值)
                        df_editor['真實商品'] = "請選擇對應的真實商品..."
                        df_editor['成本(若為0則自動帶入)'] = 0
                    
                    # 2. 顯示 Data Editor
                    edited_df = st.data_editor(
                        df_editor,
                        column_config={
                            "訂單成立日期": st.column_config.TextColumn("日期", disabled=True),
                            "訂單編號": st.column_config.TextColumn("訂單編號", disabled=True),
                            "商品名稱": st.column_config.TextColumn("蝦皮商品名稱", disabled=True, width="large"),
                            "商品選項名稱": st.column_config.TextColumn("規格", disabled=True),
                            "進蝦皮錢包": st.column_config.NumberColumn("進帳", disabled=True, format="$%d"),
                            "買家備註": st.column_config.TextColumn("買家備註", disabled=True),
                            "真實商品": st.column_config.SelectboxColumn(
                                "選擇真實商品",
                                help="請選擇對應的進貨成本商品",
                                width="medium",
                                options=options,
                                required=True
                            ),
                            "成本(若為0則自動帶入)": st.column_config.NumberColumn(
                                "確認成本",
                                help="輸入 0 系統會自動從成本表帶入預設成本",
                                min_value=0,
                                step=1,
                                format="$%d"
                            )
                        },
                        hide_index=True,
                        use_container_width=True,
                        num_rows="fixed",
                        key="special_order_editor"
                    )

                    # 3. 批量儲存按鈕
                    if st.button("💾 批量確認歸戶 (Save All)", type="primary", use_container_width=True):
                        success_count = 0
                        fail_count = 0
                        updated_rows = 0
                        
                        progress_bar = st.progress(0, text="正在處理中...")
                        
                        # 找出有被修改過的行 (其實只要檢查 '真實商品' 不為 default 的即可)
                        # Iterate rows to check valid selections
                        total_rows = len(edited_df)
                        
                        for i, (index, row) in enumerate(edited_df.iterrows()):
                            real_item = row['真實商品']
                            input_cost = row['成本(若為0則自動帶入)']
                            order_sn = row['訂單編號']
                            shopee_name = row['商品名稱']
                            shopee_option = row['商品選項名稱']

                            if real_item != "請選擇對應的真實商品...":
                                updated_rows += 1
                                
                                # 決定最終成本
                                final_cost = input_cost
                                if final_cost == 0 and real_item in cost_dict:
                                    final_cost = int(cost_dict[real_item])
                                
                                # 執行歸戶
                                try:
                                    real_sku_name = real_item.split(" |")[0].strip()
                                    
                                    # 讀取 row 的原始資訊 (為了 save_memory_rule)
                                    # 其實上面已經有 shopee_name 了
                                    
                                    if update_special_order(order_sn, real_sku_name, final_cost, df_db, db_sheet):
                                        # 自動記憶
                                        if "7777" not in str(shopee_name):
                                            save_memory_rule(client, shopee_name, shopee_option, real_sku_name, final_cost)
                                            
                                        # 如果使用者手動改了成本，也同步回主表? 
                                        # 這裡邏輯保留：如果 final_cost != default_cost (user changed it), maybe update master
                                        default_cost_ref = cost_dict.get(real_item, 0)
                                        if final_cost != default_cost_ref and final_cost > 0:
                                            update_master_cost_sheet(client, real_item, final_cost)
                                            
                                        success_count += 1
                                    else:
                                        fail_count += 1
                                        print(f"Failed to update {order_sn}")
                                except Exception as e:
                                    fail_count += 1
                                    st.error(f"Error processing {order_sn}: {e}")
                            
                            progress_bar.progress((i + 1) / total_rows)

                        progress_bar.empty()
                        
                        if updated_rows == 0:
                            st.warning("⚠️ 您尚未選擇任何「真實商品」，請在表格中選擇後再儲存。")
                        else:
                            if success_count > 0:
                                st.success(f"✅ 成功歸戶 {success_count} 筆訂單！")
                                if fail_count > 0:
                                    st.error(f"❌ {fail_count} 筆處理失敗")
                                time.sleep(1.5)
                                st.rerun()
                            else:
                                st.error("❌ 更新失敗，請檢查網路或稍後再試。")


            # ========================================================
            # 成本為 $0 的一般訂單補填區塊
            # ========================================================
            st.divider()
            st.markdown("#### 💰 一般訂單成本補填 (成本 = $0)")
            st.info("以下為成本欄位為 $0 且尚未歸戶的**一般**訂單（非特殊區），請選擇真實商品並補填成本。")

            try:
                client_zero = get_gspread_client()
                db_sheet_zero = client_zero.open(DB_SHEET_NAME).sheet1
                data_zero = db_sheet_zero.get_all_values()
                if len(data_zero) > 1:
                    df_db_zero = pd.DataFrame(data_zero[1:], columns=data_zero[0])
                else:
                    df_db_zero = pd.DataFrame()
            except Exception as e:
                st.error(f"讀取資料失敗：{e}")
                df_db_zero = pd.DataFrame()

            if not df_db_zero.empty:
                if '備註' not in df_db_zero.columns:
                    df_db_zero['備註'] = ""
                if '買家備註' not in df_db_zero.columns:
                    df_db_zero['買家備註'] = ""
                if '成本' not in df_db_zero.columns:
                    df_db_zero['成本'] = 0

                df_db_zero['成本'] = pd.to_numeric(
                    df_db_zero['成本'].astype(str).str.replace(',', ''), errors='coerce'
                ).fillna(0)

                mask_zero = (
                    (df_db_zero['成本'] == 0) &
                    (~df_db_zero['備註'].astype(str).str.contains("已歸戶")) &
                    (~df_db_zero['商品名稱'].astype(str).apply(
                        lambda x: any(sp in x for sp in SPECIAL_PRODUCTS)
                    ))
                )
                pending_zero = df_db_zero[mask_zero].copy()

                if pending_zero.empty:
                    st.success("✅ 所有一般訂單的成本均已填寫完成！")
                else:
                    st.info(f"💡 系統偵測到有 {len(pending_zero)} 筆待處理的一般零元訂單，您可透過下方篩選器縮小範圍。")
                    
                    if '訂單成立日期' in pending_zero.columns:
                        pending_zero['訂單成立日期_dt'] = pd.to_datetime(pending_zero['訂單成立日期'], errors='coerce')
                        min_d_z = pending_zero['訂單成立日期_dt'].min().date() if not pending_zero['訂單成立日期_dt'].isnull().all() else get_taiwan_time().date()
                        max_d_z = pending_zero['訂單成立日期_dt'].max().date() if not pending_zero['訂單成立日期_dt'].isnull().all() else get_taiwan_time().date()
                    else:
                        min_d_z = max_d_z = get_taiwan_time().date()
                    
                    # --- 一般訂單日期篩選區塊 ---
                    st.markdown("##### 📅 篩選一般待處理訂單")
                    zc1, zc2 = st.columns([1, 2])
                    with zc1:
                        zq1, zq2 = st.columns(2)
                        with zq1:
                            if st.button("今日", key="btn_z_today", use_container_width=True):
                                st.session_state['z_start'] = get_taiwan_time().date()
                                st.session_state['z_end'] = get_taiwan_time().date()
                            if st.button("昨日", key="btn_z_yest", use_container_width=True):
                                yest_z = get_taiwan_time().date() - timedelta(days=1)
                                st.session_state['z_start'] = yest_z
                                st.session_state['z_end'] = yest_z
                        with zq2:
                            if st.button("本月", key="btn_z_tmonth", use_container_width=True):
                                today_z = get_taiwan_time().date()
                                st.session_state['z_start'] = today_z.replace(day=1)
                                st.session_state['z_end'] = today_z
                            if st.button("全部待處理", key="btn_z_all", use_container_width=True):
                                st.session_state['z_start'] = min_d_z
                                st.session_state['z_end'] = max_d_z
                    
                    if 'z_start' not in st.session_state: st.session_state['z_start'] = min_d_z
                    if 'z_end' not in st.session_state: st.session_state['z_end'] = max_d_z
                    
                    with zc2:
                        zd1, zd2 = st.columns(2)
                        with zd1:
                            z_start = st.date_input("起始日期", value=st.session_state['z_start'], key="z_start_in")
                        with zd2:
                            z_end = st.date_input("結束日期", value=st.session_state['z_end'], key="z_end_in")
                            
                    st.session_state['z_start'] = z_start
                    st.session_state['z_end'] = z_end

                    if '訂單成立日期_dt' in pending_zero.columns:
                        pending_zero_filtered = pending_zero[(pending_zero['訂單成立日期_dt'].dt.date >= z_start) & (pending_zero['訂單成立日期_dt'].dt.date <= z_end)]
                    else:
                        pending_zero_filtered = pending_zero

                    if pending_zero_filtered.empty:
                        st.warning(f"⚠️ 該區間 ({z_start} ~ {z_end}) 內目前無一般零元訂單待補填。")
                    else:
                        st.success(f"📌 篩選後共有 {len(pending_zero_filtered)} 筆一般特殊訂單待補填，請在下方表格編輯：")
                        df_cost_ref_zero, _ = load_cloud_cost_table()
                        if df_cost_ref_zero is not None:
                            cost_dict_zero = pd.Series(
                                df_cost_ref_zero.成本.values,
                                index=df_cost_ref_zero.Menu_Label
                            ).to_dict()
                            options_zero = ["請選擇對應的真實商品..."] + list(cost_dict_zero.keys())

                            show_cols_zero = [c for c in ['訂單成立日期', '訂單編號', '商品名稱', '商品選項名稱', '進蝦皮錢包', '買家備註'] if c in pending_zero_filtered.columns]
                            df_editor_zero = pending_zero_filtered[show_cols_zero].copy()
                            df_editor_zero['真實商品'] = "請選擇對應的真實商品..."
                            df_editor_zero['成本(若為0則自動帶入)'] = 0

                            edited_zero = st.data_editor(
                                df_editor_zero,
                                column_config={
                                    "訂單成立日期": st.column_config.TextColumn("日期", disabled=True),
                                    "訂單編號": st.column_config.TextColumn("訂單編號", disabled=True),
                                    "商品名稱": st.column_config.TextColumn("蝦皮商品名稱", disabled=True, width="large"),
                                    "商品選項名稱": st.column_config.TextColumn("規格", disabled=True),
                                    "進蝦皮錢包": st.column_config.NumberColumn("進帳", disabled=True, format="$%d"),
                                    "買家備註": st.column_config.TextColumn("買家備註", disabled=True),
                                    "真實商品": st.column_config.SelectboxColumn(
                                        "選擇真實商品",
                                        help="請選擇對應的進貨成本商品",
                                        width="medium",
                                        options=options_zero,
                                        required=True
                                    ),
                                    "成本(若為0則自動帶入)": st.column_config.NumberColumn(
                                        "確認成本",
                                        help="輸入 0 系統會自動從成本表帶入預設成本",
                                        min_value=0,
                                        step=1,
                                        format="$%d"
                                    ),
                                },
                                hide_index=True,
                                use_container_width=True,
                                num_rows="fixed",
                                key="zero_cost_editor"
                            )

                            if st.button("💾 批量補填成本 (Save All)", type="primary", use_container_width=True, key="save_zero_cost"):
                            success_z = 0
                            fail_z = 0
                            updated_z = 0
                            bar_z = st.progress(0, text="正在處理...")
                            total_z = len(edited_zero)

                            for i, (idx_z, row_z) in enumerate(edited_zero.iterrows()):
                                real_item_z = row_z['真實商品']
                                input_cost_z = row_z['成本(若為0則自動帶入)']
                                order_sn_z = row_z['訂單編號']
                                shopee_name_z = row_z['商品名稱']
                                shopee_opt_z = row_z.get('商品選項名稱', '')

                                if real_item_z != "請選擇對應的真實商品...":
                                    updated_z += 1
                                    final_cost_z = input_cost_z
                                    if final_cost_z == 0 and real_item_z in cost_dict_zero:
                                        final_cost_z = int(cost_dict_zero[real_item_z])

                                    try:
                                        real_sku_name_z = real_item_z.split(" |")[0].strip()
                                        if update_special_order(order_sn_z, real_sku_name_z, final_cost_z, df_db_zero, db_sheet_zero):
                                            save_memory_rule(client_zero, shopee_name_z, shopee_opt_z, real_sku_name_z, final_cost_z)
                                            default_cost_z = cost_dict_zero.get(real_item_z, 0)
                                            if final_cost_z != default_cost_z and final_cost_z > 0:
                                                update_master_cost_sheet(client_zero, real_item_z, final_cost_z)
                                            success_z += 1
                                        else:
                                            fail_z += 1
                                    except Exception as e:
                                        fail_z += 1
                                        st.error(f"處理 {order_sn_z} 時發生錯誤：{e}")

                                bar_z.progress((i + 1) / total_z)

                            bar_z.empty()

                            if updated_z == 0:
                                st.warning("⚠️ 您尚未選擇任何「真實商品」，請在表格中選擇後再儲存。")
                            else:
                                if success_z > 0:
                                    st.success(f"✅ 成功補填 {success_z} 筆訂單的成本！")
                                if fail_z > 0:
                                    st.error(f"❌ {fail_z} 筆處理失敗")
                                if success_z > 0:
                                    time.sleep(1.5)
                                    st.cache_data.clear()
                                    st.rerun()
                    else:
                        st.error("❌ 無法載入成本表，請確認 Google Sheet 連線。")

        with tab3:
            st.markdown("#### 🛠️ 商品資料批量維護")
            
            with st.expander("📦 批量新增商品 (從 mass_update.xlsx)", expanded=True):
                mass_file = st.file_uploader("上傳 mass_update.xlsx", type=['xlsx'])
                if mass_file:
                    if st.button("開始同步至編碼表"):
                        bar = st.progress(0, "分析中...")
                        df_new = process_mass_update_file(mass_file)
                        if df_new is not None:
                            client = get_gspread_client()
                            sheet = client.open(COST_SHEET_NAME).sheet1
                            cnt = sync_new_products(df_new, sheet, bar)
                            st.success(f"✅ 同步完成！共新增 {cnt} 筆新商品。")
                        else:
                            st.error("檔案解析失敗")
            
            with st.expander("🚑 成本資料救援 (從 2026 舊表)", expanded=False):
                st.warning("⚠️ 此功能僅在「新增商品」後，發現成本都是 0 時使用。")
                if st.button("執行救援任務"):
                    bar2 = st.progress(0, "連線舊資料庫...")
                    res = auto_fill_costs_from_legacy(bar2)
                    st.success(res)
            
        with tab4:
            st.markdown("#### 🤝 非蝦皮訂單手動錄入 (私下轉帳)")
            st.info("此功能用於記錄「非蝦皮平台」的交易（如街口、將來銀行轉帳），手續費將自動設為 $0。")
            
            # 取得成本表資料
            df_cost_ref, _ = load_cloud_cost_table()
            
            if df_cost_ref is not None:
                cost_dict = pd.Series(df_cost_ref.成本.values, index=df_cost_ref.Menu_Label).to_dict()
                item_options = ["請選擇商品..."] + list(cost_dict.keys())
                
                with st.form("manual_order_form", clear_on_submit=True):
                    c1, c2 = st.columns(2)
                    with c1:
                        m_date = st.date_input("🗓️ 訂單日期", value=datetime.now().date())
                        m_item = st.selectbox("📦 選擇商品", item_options)
                    with c2:
                        m_bank = st.radio("🏦 收款銀行", ["街口", "將來銀行"], horizontal=True)
                        m_qty = st.number_input("🔢 數量", min_value=1, value=1, step=1)
                    
                    # 取得預設成本
                    m_default_cost = 0.0
                    if m_item != "請選擇商品..." and m_item in cost_dict:
                        m_default_cost = cost_dict[m_item]
                    
                    c3, c4 = st.columns(2)
                    with c3:
                        m_price = st.number_input("💰 銷售單價 (整筆金額)", min_value=0.0, value=0.0, step=10.0)
                    with c4:
                        m_cost = st.number_input("📉 成本單價 (每件)", min_value=0.0, value=float(m_default_cost), step=1.0, key=f"m_cost_{str(m_item)}")

                    submit_btn = st.form_submit_button("✅ 確認建立非蝦皮訂單", use_container_width=True, type="primary")
                    
                    if submit_btn:
                        if m_item == "請選擇商品...":
                            st.error("❌ 請選擇商品")
                        elif m_price <= 0:
                            st.error("❌ 請輸入售價")
                        else:
                            with st.spinner("正在寫入資料庫..."):
                                # 生成 ID
                                off_id = f"OFF_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                                # 解析商品純名稱
                                pure_name = m_item.split(" |")[0].strip()
                                # 計算利潤 (手續費全額為0)
                                total_income = m_price 
                                total_cost = m_cost * m_qty
                                total_profit = total_income - total_cost
                                
                                # 準備寫入 Row
                                # headers = ['訂單編號', '訂單成立日期', '商品名稱', '商品選項名稱', '數量', '售價', '成交手續費', '金流與系統處理費', '其他服務費', '蝦皮付費總金額', '進蝦皮錢包', '成本', '總利潤', '蝦皮商品編碼', '資料備份時間', '備註']
                                new_row = [
                                    off_id,
                                    m_date.strftime("%Y-%m-%d"),
                                    pure_name,
                                    "轉帳交易", # 規格
                                    m_qty,
                                    m_price,
                                    0, 0, 0, 0, # 各種手續費
                                    total_income,
                                    m_cost,
                                    total_profit,
                                    "OFF-PLATFORM", # 編碼
                                    get_taiwan_time().strftime("%Y-%m-%d %H:%M:%S"),
                                    f"轉帳: {m_bank}"
                                ]
                                
                                try:
                                    client = get_gspread_client()
                                    db_sheet = client.open(DB_SHEET_NAME).sheet1
                                    db_sheet.append_row([str(x) for x in new_row])
                                    st.success(f"🎉 訂單錄入成功！ ID: {off_id}")
                                    st.balloons()
                                    st.cache_data.clear()
                                except Exception as e:
                                    st.error(f"❌ 寫入失敗: {e}")
            else:
                st.error("❌ 無法載入成本表，無法進行手動錄入。")

        with tab5:
            st.markdown("#### 📢 廣告費用輸入與管理")
            st.info("請輸入每天在蝦皮或站外投放廣告所產生的真實費用，這將會合併至前台戰情室計算真淨毛利。")
            
            # Form for input
            client = get_gspread_client()
            ad_df = get_ad_costs_df(client)
            
            with st.form("ad_cost_form", clear_on_submit=False):
                c1, c2 = st.columns(2)
                with c1:
                    ad_date = st.date_input("🗓️ 廣告花費日期", value=get_taiwan_time().date())
                
                # Check exist ad cost
                default_ad_cost = 0
                if not ad_df.empty:
                    exist_row = ad_df[ad_df['日期'] == ad_date]
                    if not exist_row.empty:
                        default_ad_cost = int(exist_row.iloc[0]['廣告費用'])
                        
                with c2:
                    ad_cost_val = st.number_input("💰 廣告花費金額", min_value=0, value=default_ad_cost, step=50, format="%d")
                
                submit_ad = st.form_submit_button("💾 儲存廣告費用", type="primary", use_container_width=True)
                
                if submit_ad:
                    with st.spinner("正在儲存資料..."):
                        if save_ad_cost(client, ad_date, ad_cost_val):
                            st.success(f"✅ 成功儲存 {ad_date.strftime('%Y-%m-%d')} 廣告費用: ${ad_cost_val}")
                            time.sleep(1)
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.error("❌ 儲存失敗，請檢查網路狀態或重試")
            
            st.divider()
            st.markdown("##### 📜 歷史廣告費用紀錄 (近 30 筆)")
            if not ad_df.empty:
                # 依日期遞減排序，取前 30
                ad_df_show = ad_df.sort_values(by="日期", ascending=False).head(30)
                st.dataframe(
                    ad_df_show,
                    hide_index=True,
                    use_container_width=True,
                    column_config={
                        "日期": st.column_config.DateColumn("日期", format="YYYY-MM-DD"),
                        "廣告費用": st.column_config.NumberColumn("廣告費用", format="$%d"),
                        "登錄時間": st.column_config.TextColumn("最後更新時間")
                    }
                )
            else:
                st.info("尚無廣告費用紀錄")

    elif pwd:
        st.error('⛔ 密碼錯誤')
