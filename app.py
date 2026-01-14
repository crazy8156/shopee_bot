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
            if "規格名稱" in col: mapping[col] = "商品選項名稱" # 新增映射
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

    df_merged['總利潤'] = df_merged['進蝦皮錢包'] - (df_merged['成本'] * df_merged['數量'])
    
    progress_bar.progress(50, text=f"比對 {DB_SHEET_NAME}...")
    client = get_gspread_client()
    try: db_sheet = client.open(DB_SHEET_NAME).sheet1
    except: return f"❌ 找不到資料庫：{DB_SHEET_NAME}"
    
    headers = ['訂單編號', '訂單成立日期', '商品名稱', '商品選項名稱', '數量', '售價', '成交手續費', '金流與系統處理費', '其他服務費', '蝦皮付費總金額', '進蝦皮錢包', '成本', '總利潤', '蝦皮商品編碼', '資料備份時間', '備註']
    
    df_upload_ready = df_merged.copy()
    df_upload_ready['資料備份時間'] = get_taiwan_time().strftime("%Y-%m-%d %H:%M:%S")
    df_upload_ready['備註'] = "" 
    
    memory_rules = get_memory_rules(client)
    if '商品名稱' in df_upload_ready.columns:
        mask_special = df_upload_ready['商品名稱'].astype(str).apply(lambda x: any(sp in x for sp in SPECIAL_PRODUCTS))
        df_upload_ready.loc[mask_special, '備註'] = "待人工確認"
        df_upload_ready.loc[mask_special, '總利潤'] = 0
        
        # 建立成本查詢表 (for Smart Match)
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
                real_profit = income - real_cost 
                df_upload_ready.at[idx, '成本'] = real_cost
                df_upload_ready.at[idx, '總利潤'] = real_profit
                df_upload_ready.at[idx, '備註'] = f"已歸戶({source_type}): {found_sku}"

    for h in headers:
        if h not in df_upload_ready.columns: df_upload_ready[h] = ""
    df_upload_ready = df_upload_ready[headers].fillna('').astype(str)
    
    existing_data = db_sheet.get_all_values()
    
    if len(existing_data) <= 1:
        db_sheet.clear(); db_sheet.append_row(headers); db_sheet.append_rows(df_upload_ready.values.tolist())
        return f"✅ 初始化完成！新增 {len(df_upload_ready)} 筆。"
    else:
        df_existing = pd.DataFrame(existing_data[1:], columns=existing_data[0])
        existing_ids = set(df_existing['訂單編號'].astype(str).str.strip())
        df_new_orders = df_upload_ready[~df_upload_ready['訂單編號'].astype(str).str.strip().isin(existing_ids)]
        skipped_count = len(df_upload_ready) - len(df_new_orders)
        
        if not df_new_orders.empty:
            progress_bar.progress(80, text=f"新增 {len(df_new_orders)} 筆新資料...")
            db_sheet.append_rows(df_new_orders.values.tolist())
            progress_bar.progress(100, text="完成")
            return f"✅ 成功！新增 {len(df_new_orders)} 筆新訂單 (跳過 {skipped_count} 筆舊資料)。"
        else:
            progress_bar.progress(100, text="無新資料")
            return f"✅ 沒事做！全部資料已存在 (跳過 {skipped_count} 筆)。"

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
mode = st.sidebar.radio("", ["📊 前台戰情室", "⚙️ 後台管理", "🔍 成本神探"], label_visibility="collapsed")
st.sidebar.markdown("---")
st.sidebar.caption("Ver 9.4 | Update: 2026-01-14 13:20")

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
            df_all['日期標籤'] = df_all['訂單成立日期'].dt.strftime('%Y-%m-%d')
        else: st.error("資料庫缺少『訂單成立日期』欄位"); st.stop()

        # 日期篩選器
        col_date, col_space = st.columns([1, 3])
        with col_date:
            dates = sorted(df_all['日期標籤'].dropna().unique(), reverse=True)
            sel_date = st.selectbox("📅 選擇營業日期", dates) if dates else None
        
        if sel_date:
            df_day = df_all[df_all['日期標籤'] == sel_date]
            
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
            total_gp = df_normal['總利潤'].sum()
            margin = (total_gp / total_rev * 100) if total_rev > 0 else 0
            
            # --- 視覺化指標卡片 ---
            cols = st.columns(4)
            metrics = [
                ("💰 當日營收", f"${total_rev:,.0f}", ""),
                ("📉 商品成本", f"${total_cost:,.0f}", ""),
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
            
            # --- 特殊訂單警示 ---
            if not df_special.empty:
                st.error(f"⚠️ 發現 {len(df_special)} 筆訂單尚未歸戶 (不會計入毛利)")
                st.dataframe(
                    df_special[['訂單編號', '商品名稱', '售價', '備註']],
                    hide_index=True,
                    use_container_width=True
                )
            
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
    
    pwd = st.text_input("🔑 請輸入管理員密碼", type="password")
    
    if pwd == ADMIN_PWD:
        # 使用更美觀的 Tabs
        st.markdown("###")
        tab1, tab2, tab3 = st.tabs(["📥 訂單上傳", "🔗 歸戶系統", "🛠️ 商品維護"])

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
                if st.button("🚀 開始分析訂單", type="primary", use_container_width=True):
                    bar = st.progress(0, "初始化中...")
                    df_sales = load_sales_report(sales_file)
                    if df_sales is not None:
                        res = process_orders(df_sales, df_cost, bar)
                        time.sleep(0.5)
                        if "成功" in res: st.success(res)
                        else: st.warning(res)
                        st.cache_data.clear()

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
            mask = (
                df_db['商品名稱'].astype(str).apply(lambda x: any(sp in x for sp in SPECIAL_PRODUCTS)) & 
                (~df_db['備註'].astype(str).str.contains("已歸戶"))
            )
            pending = df_db[mask]
            
            if pending.empty:
                st.balloons()
                st.success("🎉 太棒了！目前所有特殊訂單都已完成歸戶。")
            else:
                st.warning(f"目前有 {len(pending)} 筆待處理訂單：")
                df_cost_ref, _ = load_cloud_cost_table()
                
                if df_cost_ref is not None:
                    cost_dict = pd.Series(df_cost_ref.成本.values, index=df_cost_ref.Menu_Label).to_dict()
                    options = ["請選擇對應的真實商品..."] + list(cost_dict.keys())
                    
                    for idx, row in pending.iterrows():
                        with st.container():
                            st.markdown(f"""
                            <div style="background:#f8f9fa; padding:15px; border-radius:10px; margin-bottom:10px; border:1px solid #ddd; box-shadow: 0 2px 4px rgba(0,0,0,0.05);">
                                <div style="font-weight:bold; color:#d63384; font-size: 1.05rem; margin-bottom: 8px;">{row['商品名稱']}</div>
                                <div style="background: #e7f5ff; color: #004085; padding: 4px 8px; border-radius: 4px; display: inline-block; font-weight: 600; font-size: 0.9rem; margin-bottom: 8px;">
                                    🔹 規格: {row.get('商品選項名稱', '無規格') if row.get('商品選項名稱') else '無規格'}
                                </div>
                                <div style="font-size:0.85rem; color:#666; margin-top: 4px;">
                                    訂單: <a href="https://seller.shopee.tw/portal/sale?type=all&keyword={row['訂單編號']}" target="_blank" style="text-decoration:none;color:#0d6efd;border-bottom:1px dashed #0d6efd;margin-right:5px;" title="點擊搜尋此訂單">{row['訂單編號']} �</a> 
                                    | 金額: <span style="color: #28a745; font-weight:bold;">${row['售價']}</span>
                                </div>
                            </div>
                            """, unsafe_allow_html=True)
                            
                            # 為了方便複製，提供 Code Block
                            c_copy_tip, c_code = st.columns([1, 2])
                            with c_copy_tip:
                                st.caption("👉 若跳轉後未自動搜尋，請複製號碼：")
                            with c_code:
                                st.code(row['訂單編號'], language=None)
                            
                            c_sel, c_opt, c_act = st.columns([3, 2, 1])
                            
                            with c_sel:
                                real_item = st.selectbox("選擇真實商品", options, key=f"s_{row['訂單編號']}", label_visibility="collapsed")
                            
                            with c_opt:
                                remember_me = st.checkbox("以後自動歸戶", key=f"chk_{row['訂單編號']}")
                                
                            with c_act:
                                if st.button("確認歸戶", key=f"b_{row['訂單編號']}", type="primary"):
                                    if "請選擇" not in real_item:
                                        real_cost = cost_dict[real_item]
                                        real_name = real_item.split(" |")[0]
                                        with st.spinner("寫入中..."):
                                            update_special_order(row['訂單編號'], real_name, real_cost, df_db, db_sheet)
                                            if remember_me:
                                                if "7777" in row['商品名稱']: st.warning("⚠️ 為了安全，無法自動記憶 7777！")
                                                else:
                                                    save_memory_rule(client, row['商品名稱'], row['商品選項名稱'], real_name, real_cost)
                                            st.toast("✅ 歸戶成功！", icon="🎉")
                                            time.sleep(1)
                                            st.rerun()
                                    else:
                                        st.error("請選擇商品")
                        st.markdown("---")

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

    elif pwd:
        st.error("⛔ 密碼錯誤")