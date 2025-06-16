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
    """分析应收未收额数据 - 专门查找第69行"""
    result = {}
    
    if len(df.columns) == 0 or len(df) == 0:
        return result
    
    # 检查第一行是否是门店名称（通常第一行只有第一个单元格有值）
    first_row = df.iloc[0] if len(df) > 0 else None
    if first_row is not None:
        non_empty_count = sum(1 for val in first_row if pd.notna(val) and str(val).strip() != '')
        # 如果第一行只有1-2个非空值，可能是门店名称行
        if non_empty_count <= 2:
            # 跳过第一行，使用第二行作为新的第一行
            df = df.iloc[1:].reset_index(drop=True)
            result['skipped_store_name_row'] = True
    
    # 查找第69行（如果跳过了第一行，实际是原始数据的第70行）
    target_row_index = 68  # 第69行
    
    if len(df) > target_row_index:
        row = df.iloc[target_row_index]
        first_col_value = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ""
        
        # 检查第一列是否包含"应收-未收额"相关关键词
        keywords = ['应收-未收额', '应收未收额', '应收-未收', '应收未收']
        
        for keyword in keywords:
            if keyword in first_col_value:
                # 查找该行中的数值（从后向前查找，通常合计在后面的列）
                for col_idx in range(len(row)-1, 0, -1):
                    val = row.iloc[col_idx]
                    if pd.notna(val) and str(val).strip() not in ['', 'None', 'nan']:
                        # 清理数值
                        cleaned = str(val).replace(',', '').replace('¥', '').replace('￥', '').strip()
                        
                        # 处理括号表示的负数
                        if cleaned.startswith('(') and cleaned.endswith(')'):
                            cleaned = '-' + cleaned[1:-1]
                        
                        try:
                            amount = float(cleaned)
                            if amount != 0:
                                result['应收-未收额'] = {
                                    'amount': amount,
                                    'column_name': str(df.columns[col_idx]),
                                    'row_name': first_col_value,
                                    'row_index': target_row_index,
                                    'actual_row_number': target_row_index + 1  # 实际行号
                                }
                                return result
                        except ValueError:
                            continue
                break
    
    # 如果第69行没找到，提供备用查找方案
    if '应收-未收额' not in result:
        # 在所有行中查找
        keywords = ['应收-未收额', '应收未收额', '应收-未收', '应收未收']
        
        for idx, row in df.iterrows():
            try:
                row_name = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ""
                
                if not row_name.strip():
                    continue
                
                # 检查是否匹配关键词
                for keyword in keywords:
                    if keyword in row_name:
                        # 查找该行中的数值
                        for col_idx in range(len(row)-1, 0, -1):
                            val = row.iloc[col_idx]
                            if pd.notna(val) and str(val).strip() not in ['', 'None', 'nan']:
                                cleaned = str(val).replace(',', '').replace('¥', '').replace('￥', '').strip()
                                
                                if cleaned.startswith('(') and cleaned.endswith(')'):
                                    cleaned = '-' + cleaned[1:-1]
                                
                                try:
                                    amount = float(cleaned)
                                    if amount != 0:
                                        result['应收-未收额'] = {
                                            'amount': amount,
                                            'column_name': str(df.columns[col_idx]),
                                            'row_name': row_name,
                                            'row_index': idx,
                                            'actual_row_number': idx + 1,
                                            'note': f'在第{idx+1}行找到（非第69行）'
                                        }
                                        return result
                                except ValueError:
                                    continue
                        break
            except Exception:
                continue
    
    # 返回调试信息
    result['debug_info'] = {
        'total_rows': len(df),
        'checked_row_69': len(df) > target_row_index,
        'row_69_content': str(df.iloc[target_row_index].iloc[0]) if len(df) > target_row_index else 'N/A'
    }
    
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
                    
                    # 检查第一行是否是门店名称
                    if len(df) > 0:
                        first_row = df.iloc[0]
                        non_empty_count = sum(1 for val in first_row if pd.notna(val) and str(val).strip() != '')
                        
                        # 如果第一行只有少数非空值，可能是门店名称，跳过它
                        if non_empty_count <= 2 and len(df) > 1:
                            df = df.iloc[1:]
                    
                    # 如果有足够的行，使用第二行作为表头
                    if len(df) > 1:
                        header_row = df.iloc[0].fillna('').astype(str).tolist()
                        data_rows = df.iloc[1:].copy()
                        
                        # 清理列名并处理重复
                        cols = []
                        for i, col in enumerate(header_row):
                            col = str(col).strip()
                            if col == '' or col == 'nan' or col == '0':
                                col = f'列{i+1}' if i > 0 else '项目名称'
                            
                            # 处理重复列名
                            original_col = col
                            counter = 1
                            while col in cols:
                                col = f"{original_col}_{counter}"
                                counter += 1
                            cols.append(col)
                        
                        # 确保列数匹配
                        min_cols = min(len(data_rows.columns), len(cols))
                        cols = cols[:min_cols]
                        data_rows = data_rows.iloc[:, :min_cols]
                        
                        data_rows.columns = cols
                        data_rows = data_rows.reset_index(drop=True).fillna('')
                        reports_dict[store_name] = data_rows
                    else:
                        # 处理少于3行的数据
                        df_clean = df.fillna('')
                        # 设置默认列名避免重复
                        default_cols = []
                        for i in range(len(df_clean.columns)):
                            col_name = f'列{i+1}' if i > 0 else '项目名称'
                            default_cols.append(col_name)
                        df_clean.columns = default_cols
                        reports_dict[store_name] = df_clean
                except Exception as e:
                    st.warning(f"解析 {store_name} 数据失败: {str(e)}")
                    continue
        
        return reports_dict
    except Exception as e:
        st.error(f"加载报表数据失败: {str(e)}")
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
            
            df = reports_data[selected_sheet]
            
            # 检查并处理第一行是否为门店名称
            original_df = df.copy()
            if len(df) > 0:
                first_row = df.iloc[0]
                non_empty_count = sum(1 for val in first_row if pd.notna(val) and str(val).strip() != '')
                
                # 如果第一行只有少数非空值，可能是门店名称，跳过它
                if non_empty_count <= 2 and len(df) > 1:
                    df = df.iloc[1:].reset_index(drop=True)
            
            # 应收-未收额看板
            st.subheader("💰 应收-未收额")
            
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
                                    <p style="margin: 0.5rem 0; font-size: 0.9rem; opacity: 0.8;">数据位置：第{data["actual_row_number"]}行</p>
                                </div>
                            ''', unsafe_allow_html=True)
                        
                        elif amount < 0:
                            st.markdown(f'''
                                <div class="receivable-negative">
                                    <h1 style="margin: 0; font-size: 3rem;">💚 ¥{abs(amount):,.2f}</h1>
                                    <h3 style="margin: 0.5rem 0;">门店应收款金额</h3>
                                    <p style="margin: 0; font-size: 1rem;">金额为负数，表示总部需要向门店支付的款项</p>
                                    <p style="margin: 0.5rem 0; font-size: 0.9rem; opacity: 0.8;">数据位置：第{data["actual_row_number"]}行</p>
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
                
                else:
                    st.warning("⚠️ 未找到应收-未收额数据")
                    
                    # 显示调试信息
                    with st.expander("🔍 查看详情", expanded=True):
                        debug_info = analysis_results.get('debug_info', {})
                        
                        st.markdown("### 📋 数据查找说明")
                        st.write(f"- **报表总行数：** {debug_info.get('total_rows', 0)} 行")
                        
                        if debug_info.get('checked_row_69'):
                            st.write(f"- **第69行内容：** {debug_info.get('row_69_content', 'N/A')}")
                        else:
                            st.write("- **第69行：** 报表行数不足69行")
                        
                        st.markdown("""
                        ### 💡 可能的原因
                        1. 第69行不包含"应收-未收额"相关关键词
                        2. 第69行的数值为空或格式不正确
                        3. 报表格式与预期不符
                        
                        ### 🛠️ 建议
                        - 请检查Excel报表第69行是否包含"应收-未收额"
                        - 确认该行有对应的金额数据
                        - 如需调整查找位置，请联系技术支持
                        """)
            
            except Exception as e:
                st.error(f"❌ 分析数据时出错：{str(e)}")
            
            st.divider()
            
            # 完整报表数据
            st.subheader("📋 完整报表数据")
            
            search_term = st.text_input("🔍 搜索报表内容")
            
            # 数据过滤
            try:
                if search_term:
                    # 安全的搜索实现
                    search_df = df.copy()
                    # 确保所有数据都是字符串
                    for col in search_df.columns:
                        search_df[col] = search_df[col].astype(str).fillna('')
                    
                    mask = search_df.apply(
                        lambda x: x.str.contains(search_term, case=False, na=False, regex=False)
                    ).any(axis=1)
                    filtered_df = df[mask]
                    st.info(f"找到 {len(filtered_df)} 条包含 '{search_term}' 的记录")
                else:
                    filtered_df = df
                
                # 数据统计
                st.info(f"📊 数据统计：共 {len(filtered_df)} 条记录，{len(df.columns)} 列")
                
                # 显示数据表格
                if len(filtered_df) > 0:
                    # 清理数据以确保显示正常
                    display_df = filtered_df.copy()
                    
                    # 确保列名唯一
                    unique_columns = []
                    for i, col in enumerate(display_df.columns):
                        col_name = str(col)
                        if col_name in unique_columns:
                            col_name = f"{col_name}_{i}"
                        unique_columns.append(col_name)
                    display_df.columns = unique_columns
                    
                    # 清理数据内容
                    for col in display_df.columns:
                        display_df[col] = display_df[col].astype(str).fillna('')
                    
                    # 显示数据
                    st.dataframe(display_df, use_container_width=True, height=400)
                
                else:
                    st.warning("没有找到符合条件的数据")
                    
            except Exception as e:
                st.error(f"❌ 数据处理时出错：{str(e)}")
            
            # 下载功能
            st.subheader("📥 数据下载")
            
            col1, col2 = st.columns(2)
            with col1:
                try:
                    buffer = io.BytesIO()
                    # 准备下载数据
                    download_df = df.copy()
                    # 确保列名唯一
                    unique_cols = []
                    for i, col in enumerate(download_df.columns):
                        col_name = str(col)
                        if col_name in unique_cols:
                            col_name = f"{col_name}_{i}"
                        unique_cols.append(col_name)
                    download_df.columns = unique_cols
                    
                    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                        download_df.to_excel(writer, index=False)
                    
                    st.download_button(
                        "📥 下载完整报表 (Excel)",
                        buffer.getvalue(),
                        f"{st.session_state.store_name}_报表_{datetime.now().strftime('%Y%m%d')}.xlsx",
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                except Exception as e:
                    st.error(f"Excel下载准备失败：{str(e)}")
            
            with col2:
                try:
                    # CSV下载
                    csv_df = df.copy()
                    # 处理列名
                    unique_cols = []
                    for i, col in enumerate(csv_df.columns):
                        col_name = str(col)
                        if col_name in unique_cols:
                            col_name = f"{col_name}_{i}"
                        unique_cols.append(col_name)
                    csv_df.columns = unique_cols
                    
                    csv = csv_df.to_csv(index=False, encoding='utf-8-sig')
                    st.download_button(
                        "📥 下载CSV格式",
                        csv,
                        f"{st.session_state.store_name}_报表_{datetime.now().strftime('%Y%m%d')}.csv",
                        "text/csv"
                    )
                except Exception as e:
                    st.error(f"CSV下载准备失败：{str(e)}")
        
        else:
            st.error(f"❌ 未找到门店 '{st.session_state.store_name}' 的报表")
