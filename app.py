import streamlit as st
import pandas as pd
import io
import json
from datetime import datetime
import time
import gspread
from google.oauth2.service_account import Credentials

# 页面配置
st.set_page_config(
    page_title="门店报表查询系统", 
    page_icon="📊",
    layout="wide"
)

# 系统配置
ADMIN_PASSWORD = "admin123"
PERMISSIONS_SHEET_NAME = "store_permissions"
REPORTS_SHEET_NAME = "store_reports"
SYSTEM_INFO_SHEET_NAME = "system_info"

# CSS样式
st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        padding: 1rem 0;
        margin-bottom: 2rem;
    }
    .store-info {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 1.5rem;
        border-radius: 10px;
        margin: 1rem 0;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    .admin-panel {
        background: linear-gradient(135deg, #ffeaa7 0%, #fab1a0 100%);
        padding: 1.5rem;
        border-radius: 10px;
        border: 2px solid #fdcb6e;
        margin: 1rem 0;
    }
    .receivable-positive {
        background: linear-gradient(135deg, #ff9a9e 0%, #fecfef 100%);
        color: #721c24;
        padding: 2rem;
        border-radius: 15px;
        border: 3px solid #f093fb;
        margin: 1rem 0;
        text-align: center;
    }
    .receivable-negative {
        background: linear-gradient(135deg, #a8edea 0%, #d299c2 100%);
        color: #0c4128;
        padding: 2rem;
        border-radius: 15px;
        border: 3px solid #48cab2;
        margin: 1rem 0;
        text-align: center;
    }
    .dashboard-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 1.5rem;
        border-radius: 12px;
        margin: 0.5rem 0;
        text-align: center;
    }
    </style>
""", unsafe_allow_html=True)

def analyze_receivable_data(df):
    """分析应收未收额数据"""
    result = {}
    
    if len(df.columns) == 0 or len(df) == 0:
        return result
    
    # 目标关键词
    keywords = ['应收-未收额', '应收未收额', '应收-未收', '应收未收', '应收款', '应收账款', '收支差额', '净收入', '盈亏', '利润', '结余']
    
    # 查找合计列
    total_cols = []
    for col in df.columns[1:]:
        if any(word in str(col).lower() for word in ['合计', '总计', '汇总', 'total']):
            total_cols.append(col)
    
    if not total_cols:
        total_cols = df.columns[-3:]  # 取最后3列
    
    # 查找目标行
    for col in total_cols:
        for idx, row in df.iterrows():
            row_name = str(row[df.columns[0]]) if pd.notna(row[df.columns[0]]) else ""
            
            for keyword in keywords:
                if keyword in row_name:
                    try:
                        val = row[col]
                        if pd.notna(val):
                            cleaned = str(val).replace(',', '').replace('¥', '').strip()
                            if cleaned.startswith('(') and cleaned.endswith(')'):
                                cleaned = '-' + cleaned[1:-1]
                            
                            amount = float(cleaned)
                            if amount != 0:
                                result['应收-未收额'] = {
                                    'amount': amount,
                                    'column_name': str(col),
                                    'row_name': row_name,
                                    'row_index': idx
                                }
                                return result
                    except:
                        continue
    
    return result

@st.cache_resource
def get_google_sheets_client():
    """获取Google Sheets客户端"""
    try:
        credentials_info = st.secrets["google_sheets"]
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        credentials = Credentials.from_service_account_info(credentials_info, scopes=scopes)
        return gspread.authorize(credentials)
    except Exception as e:
        st.error(f"连接失败: {str(e)}")
        return None

def get_or_create_spreadsheet(gc, name="门店报表系统数据"):
    """获取或创建表格"""
    try:
        return gc.open(name)
    except:
        return gc.create(name)

def get_or_create_worksheet(spreadsheet, name):
    """获取或创建工作表"""
    try:
        return spreadsheet.worksheet(name)
    except:
        return spreadsheet.add_worksheet(title=name, rows=1000, cols=20)

def save_permissions_to_sheets(df, gc):
    """保存权限数据"""
    try:
        spreadsheet = get_or_create_spreadsheet(gc)
        worksheet = get_or_create_worksheet(spreadsheet, PERMISSIONS_SHEET_NAME)
        
        worksheet.clear()
        time.sleep(1)
        
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        all_data = [['门店名称', '人员编号', '更新时间']]
        
        for _, row in df.iterrows():
            all_data.append([str(row.iloc[0]), str(row.iloc[1]), current_time])
        
        worksheet.update('A1', all_data)
        return True
    except Exception as e:
        st.error(f"保存失败: {str(e)}")
        return False

def load_permissions_from_sheets(gc):
    """加载权限数据"""
    try:
        spreadsheet = get_or_create_spreadsheet(gc)
        worksheet = spreadsheet.worksheet(PERMISSIONS_SHEET_NAME)
        data = worksheet.get_all_values()
        
        if len(data) <= 1:
            return None
        
        df = pd.DataFrame(data[1:], columns=['门店名称', '人员编号', '更新时间'])
        return df[['门店名称', '人员编号']]
    except:
        return None

def save_reports_to_sheets(reports_dict, gc):
    """保存报表数据"""
    try:
        spreadsheet = get_or_create_spreadsheet(gc)
        worksheet = get_or_create_worksheet(spreadsheet, REPORTS_SHEET_NAME)
        
        worksheet.clear()
        time.sleep(1)
        
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        all_data = [['门店名称', '报表数据JSON', '行数', '列数', '更新时间']]
        
        for store_name, df in reports_dict.items():
            try:
                json_data = df.to_json(orient='records', force_ascii=False)
                if len(json_data) > 45000:
                    json_data = df.head(100).to_json(orient='records', force_ascii=False)
                    store_name += " (前100行)"
                
                all_data.append([store_name, json_data, len(df), len(df.columns), current_time])
            except Exception as e:
                st.warning(f"处理 {store_name} 时出错: {str(e)}")
                continue
        
        if len(all_data) > 1:
            worksheet.update('A1', all_data)
        
        return True
    except Exception as e:
        st.error(f"保存报表失败: {str(e)}")
        return False

def load_reports_from_sheets(gc):
    """加载报表数据"""
    try:
        spreadsheet = get_or_create_spreadsheet(gc)
        worksheet = spreadsheet.worksheet(REPORTS_SHEET_NAME)
        data = worksheet.get_all_values()
        
        if len(data) <= 1:
            return {}
        
        reports_dict = {}
        for row in data[1:]:
            if len(row) >= 2:
                store_name = row[0]
                json_data = row[1]
                try:
                    df = pd.read_json(json_data, orient='records')
                    # 跳过第一行，第二行作为表头
                    if len(df) > 2:
                        header_row = df.iloc[1].fillna('').astype(str).tolist()
                        data_rows = df.iloc[2:].copy()
                        
                        # 清理列名
                        cols = []
                        for i, col in enumerate(header_row):
                            col = str(col).strip()
                            if col == '' or col == 'nan':
                                col = f'列{i+1}' if i > 0 else '项目名称'
                            cols.append(col)
                        
                        if len(data_rows.columns) == len(cols):
                            data_rows.columns = cols
                            data_rows = data_rows.reset_index(drop=True).fillna('')
                            reports_dict[store_name] = data_rows
                        else:
                            reports_dict[store_name] = df.fillna('')
                    else:
                        reports_dict[store_name] = df.fillna('')
                except Exception as e:
                    st.warning(f"解析 {store_name} 数据失败: {str(e)}")
                    continue
        
        return reports_dict
    except:
        return {}

def verify_user_permission(store_name, user_id, permissions_data):
    """验证用户权限"""
    if permissions_data is None or len(permissions_data.columns) < 2:
        return False
    
    store_col = permissions_data.columns[0]
    id_col = permissions_data.columns[1]
    
    for _, row in permissions_data.iterrows():
        stored_store = str(row[store_col]).strip()
        stored_id = str(row[id_col]).strip()
        
        if (store_name in stored_store or stored_store in store_name) and stored_id == str(user_id):
            return True
    
    return False

def find_matching_reports(store_name, reports_data):
    """查找匹配的报表"""
    matching = []
    for sheet_name in reports_data.keys():
        if store_name in sheet_name or sheet_name in store_name:
            matching.append(sheet_name)
    return matching

# 初始化会话状态
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
if 'store_name' not in st.session_state:
    st.session_state.store_name = ""
if 'user_id' not in st.session_state:
    st.session_state.user_id = ""
if 'is_admin' not in st.session_state:
    st.session_state.is_admin = False
if 'google_sheets_client' not in st.session_state:
    st.session_state.google_sheets_client = None

# 主标题
st.markdown('<h1 class="main-header">📊 门店报表查询系统</h1>', unsafe_allow_html=True)

# 初始化Google Sheets客户端
if not st.session_state.google_sheets_client:
    with st.spinner("连接云数据库..."):
        gc = get_google_sheets_client()
        if gc:
            st.session_state.google_sheets_client = gc
            st.success("✅ 连接成功！")
        else:
            st.error("❌ 连接失败，请检查配置")
            st.stop()

gc = st.session_state.google_sheets_client

# 侧边栏
with st.sidebar:
    st.title("⚙️ 系统功能")
    
    user_type = st.radio("选择用户类型", ["普通用户", "管理员"])
    
    if user_type == "管理员":
        st.subheader("🔐 管理员登录")
        admin_password = st.text_input("管理员密码", type="password")
        
        if st.button("验证管理员身份"):
            if admin_password == ADMIN_PASSWORD:
                st.session_state.is_admin = True
                st.success("✅ 验证成功！")
                st.rerun()
            else:
                st.error("❌ 密码错误！")
        
        if st.session_state.is_admin:
            st.subheader("📁 文件管理")
            
            # 上传权限表
            permissions_file = st.file_uploader("上传门店权限表", type=['xlsx', 'xls'])
            if permissions_file:
                try:
                    df = pd.read_excel(permissions_file)
                    if len(df.columns) >= 2:
                        if save_permissions_to_sheets(df, gc):
                            st.success(f"✅ 权限表已上传：{len(df)} 个用户")
                            st.balloons()
                        else:
                            st.error("❌ 保存失败")
                    else:
                        st.error("❌ 格式错误：需要至少两列")
                except Exception as e:
                    st.error(f"❌ 读取失败：{str(e)}")
            
            # 上传财务报表
            reports_file = st.file_uploader("上传财务报表", type=['xlsx', 'xls'])
            if reports_file:
                try:
                    excel_file = pd.ExcelFile(reports_file)
                    reports_dict = {}
                    
                    for sheet in excel_file.sheet_names:
                        try:
                            df = pd.read_excel(reports_file, sheet_name=sheet)
                            if not df.empty:
                                reports_dict[sheet] = df
                        except:
                            continue
                    
                    if save_reports_to_sheets(reports_dict, gc):
                        st.success(f"✅ 报表已上传：{len(reports_dict)} 个门店")
                        st.balloons()
                    else:
                        st.error("❌ 保存失败")
                except Exception as e:
                    st.error(f"❌ 读取失败：{str(e)}")
    
    else:
        if st.session_state.logged_in:
            st.subheader("👤 当前登录")
            st.info(f"门店：{st.session_state.store_name}")
            st.info(f"编号：{st.session_state.user_id}")
            
            if st.button("🚪 退出登录"):
                st.session_state.logged_in = False
                st.session_state.store_name = ""
                st.session_state.user_id = ""
                st.rerun()

# 主界面
if user_type == "管理员" and st.session_state.is_admin:
    st.markdown('<div class="admin-panel"><h3>👨‍💼 管理员控制面板</h3><p>数据永久保存在云端</p></div>', unsafe_allow_html=True)
    
    permissions_data = load_permissions_from_sheets(gc)
    reports_data = load_reports_from_sheets(gc)
    
    col1, col2 = st.columns(2)
    with col1:
        perms_count = len(permissions_data) if permissions_data is not None else 0
        st.metric("权限表用户数", perms_count)
    with col2:
        reports_count = len(reports_data)
        st.metric("报表门店数", reports_count)

elif user_type == "管理员" and not st.session_state.is_admin:
    st.info("👈 请在左侧边栏输入管理员密码")

else:
    if not st.session_state.logged_in:
        st.subheader("🔐 用户登录")
        
        permissions_data = load_permissions_from_sheets(gc)
        
        if permissions_data is None:
            st.warning("⚠️ 系统维护中，请联系管理员")
        else:
            stores = sorted(permissions_data[permissions_data.columns[0]].unique().tolist())
            
            with st.form("login_form"):
                selected_store = st.selectbox("选择门店", stores)
                user_id = st.text_input("人员编号")
                submit = st.form_submit_button("🚀 登录")
                
                if submit and selected_store and user_id:
                    if verify_user_permission(selected_store, user_id, permissions_data):
                        st.session_state.logged_in = True
                        st.session_state.store_name = selected_store
                        st.session_state.user_id = user_id
                        st.success("✅ 登录成功！")
                        st.balloons()
                        st.rerun()
                    else:
                        st.error("❌ 门店或编号错误！")
    
    else:
        # 已登录 - 显示报表
        st.markdown(f'<div class="store-info"><h3>🏪 {st.session_state.store_name}</h3><p>操作员：{st.session_state.user_id}</p></div>', unsafe_allow_html=True)
        
        reports_data = load_reports_from_sheets(gc)
        matching_sheets = find_matching_reports(st.session_state.store_name, reports_data)
        
        if matching_sheets:
            if len(matching_sheets) > 1:
                selected_sheet = st.selectbox("选择报表", matching_sheets)
            else:
                selected_sheet = matching_sheets[0]
                st.info(f"📊 已找到报表：{selected_sheet}")
            
            df = reports_data[selected_sheet]
            
            # 财务概览看板
            st.subheader("💰 财务概览看板")
            
            try:
                analysis_results = analyze_receivable_data(df)
                
                if '应收-未收额' in analysis_results:
                    data = analysis_results['应收-未收额']
                    amount = data['amount']
                    
                    col1, col2, col3 = st.columns([1, 2, 1])
                    with col2:
                        if amount > 0:
                            st.markdown(f'''
                                <div class="receivable-positive">
                                    <h1 style="margin: 0; font-size: 3rem;">💳 ¥{amount:,.2f}</h1>
                                    <h3 style="margin: 0.5rem 0;">门店应付款金额</h3>
                                    <p style="margin: 0; font-size: 1rem;">金额为正数，表示门店需要向总部支付的款项</p>
                                </div>
                            ''', unsafe_allow_html=True)
                        
                        elif amount < 0:
                            st.markdown(f'''
                                <div class="receivable-negative">
                                    <h1 style="margin: 0; font-size: 3rem;">💚 ¥{abs(amount):,.2f}</h1>
                                    <h3 style="margin: 0.5rem 0;">门店应收款金额</h3>
                                    <p style="margin: 0; font-size: 1rem;">金额为负数，表示总部需要向门店支付的款项</p>
                                </div>
                            ''', unsafe_allow_html=True)
                        
                        else:
                            st.markdown('''
                                <div style="background: #e8f5e8; color: #2e7d32; padding: 2rem; border-radius: 15px; text-align: center;">
                                    <h1 style="margin: 0; font-size: 3rem;">⚖️ ¥0.00</h1>
                                    <h3 style="margin: 0.5rem 0;">收支平衡</h3>
                                    <p style="margin: 0;">应收未收额为零，账目平衡</p>
                                </div>
                            ''', unsafe_allow_html=True)
                    
                    # 指标卡
                    metric_col1, metric_col2, metric_col3 = st.columns(3)
                    with metric_col1:
                        status = "待支付" if amount > 0 else "待收款" if amount < 0 else "平衡"
                        st.markdown(f'<div class="dashboard-card"><h4>状态</h4><h2>{status}</h2></div>', unsafe_allow_html=True)
                    
                    with metric_col2:
                        st.markdown(f'<div class="dashboard-card"><h4>金额</h4><h2>¥{abs(amount):,.2f}</h2></div>', unsafe_allow_html=True)
                    
                    with metric_col3:
                        st.markdown(f'<div class="dashboard-card"><h4>数据来源</h4><h2>{data["column_name"]}</h2></div>', unsafe_allow_html=True)
                
                else:
                    st.warning("⚠️ 未找到应收-未收额数据")
            
            except Exception as e:
                st.error(f"❌ 分析数据时出错：{str(e)}")
            
            st.divider()
            
            # 报表数据
            st.subheader("📋 报表数据")
            
            search_term = st.text_input("🔍 搜索报表内容")
            
            if search_term:
                mask = df.astype(str).apply(lambda x: x.str.contains(search_term, case=False, na=False)).any(axis=1)
                filtered_df = df[mask]
                st.info(f"找到 {len(filtered_df)} 条记录")
            else:
                filtered_df = df
            
            if len(filtered_df) > 0:
                st.dataframe(filtered_df, use_container_width=True, height=400)
                
                # 下载功能
                col1, col2 = st.columns(2)
                with col1:
                    buffer = io.BytesIO()
                    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                        df.to_excel(writer, index=False)
                    
                    st.download_button(
                        "📥 下载完整报表 (Excel)",
                        buffer.getvalue(),
                        f"{st.session_state.store_name}_报表_{datetime.now().strftime('%Y%m%d')}.xlsx",
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                
                with col2:
                    csv = df.to_csv(index=False, encoding='utf-8-sig')
                    st.download_button(
                        "📥 下载CSV格式",
                        csv,
                        f"{st.session_state.store_name}_报表_{datetime.now().strftime('%Y%m%d')}.csv",
                        "text/csv"
                    )
            else:
                st.warning("没有找到数据")
        
        else:
            st.error(f"❌ 未找到门店 '{st.session_state.store_name}' 的报表")

# 页脚
st.divider()
st.markdown("""
    <div style="text-align: center; color: #888; font-size: 0.8rem; padding: 1rem;">
        <p>🏪 门店报表查询系统 v8.0 - 智能识别+可视化看板版</p>
        <p>💾 云端数据存储 | 🌐 多用户实时访问 | 🤖 智能数据处理</p>
    </div>
""", unsafe_allow_html=True)
