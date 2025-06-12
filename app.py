import streamlit as st
import pandas as pd
import io
import json
import hashlib
from datetime import datetime, timedelta
import plotly.graph_objects as go
import plotly.express as px
import time
import base64
import gspread
from google.oauth2.service_account import Credentials

# 页面配置
st.set_page_config(
    page_title="门店报表查询系统", 
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 系统配置
ADMIN_PASSWORD = "admin123"  # 建议修改为复杂密码

# Google Sheets配置
PERMISSIONS_SHEET_NAME = "store_permissions"  # 权限表sheet名称
REPORTS_SHEET_NAME = "store_reports"          # 报表数据sheet名称
SYSTEM_INFO_SHEET_NAME = "system_info"       # 系统信息sheet名称

# 自定义CSS样式
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
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    .success-message {
        background-color: #d4edda;
        color: #155724;
        padding: 1rem;
        border-radius: 5px;
        border: 1px solid #c3e6cb;
        margin: 1rem 0;
    }
    .warning-message {
        background-color: #fff3cd;
        color: #856404;
        padding: 1rem;
        border-radius: 5px;
        border: 1px solid #ffeaa7;
        margin: 1rem 0;
    }
    .search-container {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 8px;
        margin: 1rem 0;
    }
    .receivable-positive {
        background-color: #fff3cd;
        color: #856404;
        padding: 1.5rem;
        border-radius: 10px;
        border: 2px solid #ffeaa7;
        margin: 1rem 0;
        text-align: center;
    }
    .receivable-negative {
        background-color: #d4edda;
        color: #155724;
        padding: 1.5rem;
        border-radius: 10px;
        border: 2px solid #c3e6cb;
        margin: 1rem 0;
        text-align: center;
    }
    .setup-guide {
        background-color: #e3f2fd;
        color: #0d47a1;
        padding: 1.5rem;
        border-radius: 10px;
        border: 2px solid #bbdefb;
        margin: 1rem 0;
    }
    </style>
""", unsafe_allow_html=True)

# Google Sheets连接管理
@st.cache_resource
def get_google_sheets_client():
    """获取Google Sheets客户端连接"""
    try:
        # 从Streamlit secrets获取Google服务账号凭据
        credentials_info = st.secrets["google_sheets"]
        
        # 设置权限范围
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        
        # 创建凭据对象
        credentials = Credentials.from_service_account_info(
            credentials_info, 
            scopes=scopes
        )
        
        # 授权并返回客户端
        return gspread.authorize(credentials)
    
    except KeyError:
        st.error("❌ 未找到Google Sheets配置信息")
        st.info("请在Streamlit secrets中配置google_sheets信息")
        return None
    except Exception as e:
        st.error(f"❌ Google Sheets连接失败: {str(e)}")
        return None

def get_or_create_spreadsheet(gc, spreadsheet_name="门店报表系统数据"):
    """获取或创建Google Spreadsheet"""
    try:
        # 尝试打开现有表格
        spreadsheet = gc.open(spreadsheet_name)
        return spreadsheet
    except gspread.SpreadsheetNotFound:
        # 如果不存在，创建新表格
        try:
            spreadsheet = gc.create(spreadsheet_name)
            
            # 与当前用户共享（如果配置了的话）
            try:
                if "user_email" in st.secrets.get("google_sheets", {}):
                    user_email = st.secrets["google_sheets"]["user_email"]
                    spreadsheet.share(user_email, perm_type='user', role='owner')
            except:
                pass
            
            return spreadsheet
        except Exception as e:
            st.error(f"❌ 创建Google Spreadsheet失败: {str(e)}")
            return None

def get_or_create_worksheet(spreadsheet, sheet_name):
    """获取或创建工作表"""
    try:
        # 尝试获取现有工作表
        worksheet = spreadsheet.worksheet(sheet_name)
        return worksheet
    except gspread.WorksheetNotFound:
        # 如果不存在，创建新工作表
        try:
            worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=20)
            return worksheet
        except Exception as e:
            st.error(f"❌ 创建工作表 {sheet_name} 失败: {str(e)}")
            return None

# 权限数据管理
def save_permissions_to_sheets(df, gc):
    """保存权限数据到Google Sheets"""
    try:
        spreadsheet = get_or_create_spreadsheet(gc)
        if not spreadsheet:
            return False
        
        worksheet = get_or_create_worksheet(spreadsheet, PERMISSIONS_SHEET_NAME)
        if not worksheet:
            return False
        
        # 清空现有数据
        worksheet.clear()
        time.sleep(1)  # 避免API频率限制
        
        # 准备所有数据（一次性写入，减少API调用）
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        all_data = [['门店名称', '人员编号', '更新时间']]  # 表头
        
        # 添加所有数据行
        for _, row in df.iterrows():
            data_row = [str(row.iloc[0]), str(row.iloc[1]), current_time]
            all_data.append(data_row)
        
        # 一次性写入所有数据（减少API调用次数）
        worksheet.update('A1', all_data)
        
        # 更新系统信息
        time.sleep(1)  # 再次避免频率限制
        update_system_info(gc, {
            'permissions_updated': current_time,
            'total_users': len(df),
            'total_stores': df.iloc[:, 0].nunique()
        })
        
        return True
    
    except Exception as e:
        if "429" in str(e) or "Quota exceeded" in str(e):
            st.error("⚠️ API请求频率过高，请等待2-3分钟后重试")
            st.info("💡 建议：尝试上传较小的文件，或等待片刻后重新上传")
        else:
            st.error(f"❌ 保存权限数据失败: {str(e)}")
        return False

def load_permissions_from_sheets(gc):
    """从Google Sheets加载权限数据"""
    try:
        spreadsheet = get_or_create_spreadsheet(gc)
        if not spreadsheet:
            return None
        
        try:
            worksheet = spreadsheet.worksheet(PERMISSIONS_SHEET_NAME)
        except gspread.WorksheetNotFound:
            return None
        
        # 获取所有数据
        data = worksheet.get_all_values()
        
        if len(data) <= 1:  # 只有表头或没有数据
            return None
        
        # 创建DataFrame（跳过表头）
        df = pd.DataFrame(data[1:], columns=['门店名称', '人员编号', '更新时间'])
        
        # 只返回门店名称和人员编号列
        return df[['门店名称', '人员编号']]
    
    except Exception as e:
        st.error(f"❌ 加载权限数据失败: {str(e)}")
        return None

# 报表数据管理
def save_reports_to_sheets(reports_dict, gc):
    """保存报表数据到Google Sheets"""
    try:
        spreadsheet = get_or_create_spreadsheet(gc)
        if not spreadsheet:
            return False
        
        worksheet = get_or_create_worksheet(spreadsheet, REPORTS_SHEET_NAME)
        if not worksheet:
            return False
        
        # 清空现有数据
        worksheet.clear()
        time.sleep(1)  # 避免API频率限制
        
        # 准备所有数据（一次性写入）
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        all_data = [['门店名称', '报表数据JSON', '行数', '列数', '更新时间']]  # 表头
        
        # 保存每个门店的报表数据
        for store_name, df in reports_dict.items():
            try:
                # 检查数据大小并智能压缩
                json_data = df.to_json(orient='records', force_ascii=False)
                original_size = len(json_data)
                
                # 如果数据太大，采用分级压缩策略
                if original_size > 45000:  # 留一些余量
                    # 策略1：减少行数
                    if len(df) > 500:
                        sample_df = df.head(500)  # 取前500行
                        json_data = sample_df.to_json(orient='records', force_ascii=False)
                        store_name += f" (前500行,共{len(df)}行)"
                    
                    # 策略2：如果还是太大，进一步减少
                    if len(json_data) > 45000 and len(df) > 200:
                        sample_df = df.head(200)
                        json_data = sample_df.to_json(orient='records', force_ascii=False)
                        store_name += f" (前200行,共{len(df)}行)"
                    
                    # 策略3：最后保险，只取前100行
                    if len(json_data) > 45000:
                        sample_df = df.head(100)
                        json_data = sample_df.to_json(orient='records', force_ascii=False)
                        store_name += f" (前100行,共{len(df)}行)"
                
                # 最终检查
                if len(json_data) > 45000:
                    # 如果还是太大，只保存基本信息
                    json_data = json.dumps({
                        "status": "数据过大",
                        "total_rows": len(df),
                        "total_columns": len(df.columns),
                        "columns": list(df.columns)[:10],  # 只保存前10个列名
                        "sample_data": df.head(5).to_dict('records')  # 只保存前5行作为样本
                    }, ensure_ascii=False)
                    store_name += " (仅基本信息)"
                
                data_row = [
                    store_name,
                    json_data,
                    len(df),
                    len(df.columns),
                    current_time
                ]
                
                all_data.append(data_row)
                
                # 显示处理进度
                if original_size > 45000:
                    st.info(f"📊 {store_name}: 原始大小{original_size//1000}KB，压缩后{len(json_data)//1000}KB")
                
            except Exception as e:
                st.warning(f"⚠️ 处理门店 {store_name} 数据时出错: {str(e)}")
                # 添加错误记录
                error_data = [
                    f"{store_name} (错误)",
                    f"处理失败: {str(e)}",
                    0,
                    0,
                    current_time
                ]
                all_data.append(error_data)
                continue
        
        # 一次性写入所有数据
        if len(all_data) > 1:  # 确保有数据要写入
            worksheet.update('A1', all_data)
            
            # 更新系统信息
            time.sleep(1)  # 避免频率限制
            update_system_info(gc, {
                'reports_updated': current_time,
                'total_reports': len(reports_dict)
            })
        
        return True
    
    except Exception as e:
        if "429" in str(e) or "Quota exceeded" in str(e):
            st.error("⚠️ API请求频率过高，请等待2-3分钟后重试")
            st.info("💡 建议：先上传较小的报表文件测试，成功后再上传完整文件")
        else:
            st.error(f"❌ 保存报表数据失败: {str(e)}")
        return False

def load_reports_from_sheets(gc):
    """从Google Sheets加载报表数据"""
    try:
        spreadsheet = get_or_create_spreadsheet(gc)
        if not spreadsheet:
            return {}
        
        try:
            worksheet = spreadsheet.worksheet(REPORTS_SHEET_NAME)
        except gspread.WorksheetNotFound:
            return {}
        
        # 获取所有数据
        data = worksheet.get_all_values()
        
        if len(data) <= 1:  # 只有表头或没有数据
            return {}
        
        reports_dict = {}
        
        # 解析数据
        for row in data[1:]:  # 跳过表头
            if len(row) >= 2:
                store_name = row[0]
                json_data = row[1]
                
                try:
                    # 将JSON字符串转换回DataFrame
                    df = pd.read_json(json_data, orient='records')
                    reports_dict[store_name] = df
                except Exception as e:
                    st.warning(f"⚠️ 解析门店 {store_name} 数据时出错: {str(e)}")
                    continue
        
        return reports_dict
    
    except Exception as e:
        st.error(f"❌ 加载报表数据失败: {str(e)}")
        return {}

# 系统信息管理
def update_system_info(gc, info_dict):
    """更新系统信息"""
    try:
        spreadsheet = get_or_create_spreadsheet(gc)
        if not spreadsheet:
            return False
        
        worksheet = get_or_create_worksheet(spreadsheet, SYSTEM_INFO_SHEET_NAME)
        if not worksheet:
            return False
        
        # 获取现有数据
        try:
            data = worksheet.get_all_values()
            existing_info = {}
            if len(data) > 1:
                for row in data[1:]:
                    if len(row) >= 2:
                        existing_info[row[0]] = row[1]
        except:
            existing_info = {}
        
        # 更新信息
        existing_info.update(info_dict)
        existing_info['last_update'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # 清空并重写
        worksheet.clear()
        worksheet.append_row(['键', '值', '更新时间'])
        
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for key, value in existing_info.items():
            worksheet.append_row([key, str(value), current_time])
        
        return True
    
    except Exception as e:
        st.error(f"❌ 更新系统信息失败: {str(e)}")
        return False

def get_system_info(gc):
    """获取系统信息"""
    try:
        spreadsheet = get_or_create_spreadsheet(gc)
        if not spreadsheet:
            return {}
        
        try:
            worksheet = spreadsheet.worksheet(SYSTEM_INFO_SHEET_NAME)
            data = worksheet.get_all_values()
            
            if len(data) <= 1:
                return {}
            
            info = {}
            for row in data[1:]:
                if len(row) >= 2:
                    info[row[0]] = row[1]
            
            return info
        except gspread.WorksheetNotFound:
            return {}
    
    except Exception as e:
        return {}

# 核心业务逻辑函数（与原版本相同）
def verify_user_permission(store_name, user_id, permissions_data):
    """验证用户权限"""
    if permissions_data is None or len(permissions_data.columns) < 2:
        return False
    
    store_column = permissions_data.columns[0]
    id_column = permissions_data.columns[1]
    
    # 数据类型转换
    permissions_data[store_column] = permissions_data[store_column].astype(str)
    permissions_data[id_column] = permissions_data[id_column].astype(str)
    
    # 模糊匹配门店名称
    for _, row in permissions_data.iterrows():
        stored_store = str(row[store_column]).strip()
        stored_id = str(row[id_column]).strip()
        
        # 门店名称匹配（支持包含关系）
        if (store_name in stored_store or stored_store in store_name) and stored_id == str(user_id):
            return True
    
    return False

def find_matching_reports(store_name, reports_data):
    """查找匹配的报表"""
    matching_sheets = []
    store_name_clean = store_name.strip()
    
    for sheet_name in reports_data.keys():
        sheet_name_clean = sheet_name.strip()
        # 支持多种匹配方式
        if (store_name_clean in sheet_name_clean or 
            sheet_name_clean in store_name_clean or
            store_name_clean.replace(" ", "") in sheet_name_clean.replace(" ", "") or
            sheet_name_clean.replace(" ", "") in store_name_clean.replace(" ", "")):
            matching_sheets.append(sheet_name)
    
    return matching_sheets

def analyze_receivable_data(df):
    """分析应收未收额数据 - 增强识别和调试功能"""
    analysis_results = {}
    
    if len(df.columns) == 0:
        return analysis_results
    
    first_col = df.columns[0]
    
    # 扩展目标关键词，支持更多格式
    target_keywords = [
        '应收-未收额', '应收未收额', '应收-未收', '应收 未收额',
        '应收未收', '应收-未收金额', '应收未收金额', '未收金额',
        '应收款', '应收账款', '应收余额', '收支差额', '净收入',
        '盈亏', '利润', '净利润', '收支结余', '结余', '差额',
        '总收支', '收支合计', '最终结果', '汇总金额'
    ]
    
    # 智能查找合计列 - 多种策略
    total_columns = []
    
    # 策略1: 明确包含"合计"等关键词的列
    for col in df.columns[1:]:
        col_str = str(col).lower()
        if any(keyword in col_str for keyword in ['合计', '总计', '合并', '汇总', '小计', '总和', '总额']):
            total_columns.append(col)
    
    # 策略2: 查找最后几列中的数值列
    if not total_columns:
        for col in reversed(df.columns[-8:]):  # 检查最后8列
            try:
                non_null_values = df[col].dropna()
                if len(non_null_values) > 0:
                    # 尝试转换一些值看是否是数值
                    test_values = non_null_values.head(10)
                    numeric_count = 0
                    for val in test_values:
                        try:
                            cleaned = str(val).replace(',', '').replace('¥', '').replace('￥', '').strip()
                            if cleaned.replace('.', '').replace('-', '').isdigit() or cleaned == '0':
                                numeric_count += 1
                        except:
                            continue
                    
                    # 如果大部分值都是数字，认为是数值列
                    if numeric_count >= len(test_values) * 0.3:  # 降低阈值
                        total_columns.append(col)
            except:
                continue
    
    # 策略3: 如果还没找到，用所有数值列
    if not total_columns:
        for col in df.columns[1:]:
            try:
                pd.to_numeric(df[col], errors='coerce')
                total_columns.append(col)
            except:
                continue
    
    # 调试信息：记录找到的列
    debug_info = {
        'total_columns_found': [str(col) for col in total_columns],
        'all_columns': [str(col) for col in df.columns],
        'first_20_rows': []
    }
    
    # 获取前20行的第一列数据用于调试
    for idx, row in df.head(20).iterrows():
        row_name = str(row[first_col]) if pd.notna(row[first_col]) else ""
        debug_info['first_20_rows'].append(f"第{idx+2}行: {row_name}")
    
    # 在每个可能的合计列中查找目标指标
    matches_found = []
    
    for total_column in total_columns:
        for idx, row in df.iterrows():
            row_name = str(row[first_col]) if pd.notna(row[first_col]) else ""
            
            # 跳过空行
            if not row_name.strip():
                continue
            
            # 更宽松的匹配策略
            matched = False
            matched_keyword = ""
            
            # 精确匹配
            for keyword in target_keywords:
                if keyword in row_name:
                    matched = True
                    matched_keyword = keyword
                    break
            
            # 模糊匹配 - 去除空格和特殊字符后匹配
            if not matched:
                clean_row_name = row_name.replace(' ', '').replace('-', '').replace('_', '').replace('（', '').replace('）', '')
                for keyword in target_keywords:
                    clean_keyword = keyword.replace(' ', '').replace('-', '').replace('_', '')
                    if clean_keyword in clean_row_name:
                        matched = True
                        matched_keyword = keyword
                        break
            
            # 记录所有匹配尝试
            if matched or any(kw in row_name.lower() for kw in ['收', '支', '额', '款']):
                val = row[total_column] if total_column in row.index else None
                matches_found.append({
                    'row_index': idx,
                    'row_name': row_name,
                    'column': str(total_column),
                    'value': str(val),
                    'matched': matched,
                    'keyword': matched_keyword if matched else '未匹配'
                })
            
            if matched:
                try:
                    val = row[total_column]
                    if pd.notna(val) and str(val).strip() != '' and str(val).lower() != 'none':
                        # 更强的数据清理
                        cleaned_val = str(val).replace(',', '').replace('¥', '').replace('￥', '').replace('(', '').replace(')', '').strip()
                        
                        # 处理负数格式 (1200) -> -1200
                        if cleaned_val.startswith('(') and cleaned_val.endswith(')'):
                            cleaned_val = '-' + cleaned_val[1:-1]
                        
                        # 验证是否为数字
                        try:
                            amount = float(cleaned_val)
                            
                            # 只有非零值才记录
                            if amount != 0:
                                analysis_results['应收-未收额'] = {
                                    'amount': amount,
                                    'column_name': str(total_column),
                                    'row_index': idx,
                                    'row_name': row_name,
                                    'is_negative': amount < 0,
                                    'matched_keyword': matched_keyword,
                                    'debug_info': debug_info,
                                    'matches_found': matches_found
                                }
                                # 找到第一个有效值就返回
                                return analysis_results
                        except ValueError:
                            continue
                except Exception as e:
                    continue
    
    # 如果没有找到，返回调试信息
    analysis_results['debug_info'] = debug_info
    analysis_results['matches_found'] = matches_found
    
    return analysis_results

# 初始化会话状态
def init_session_state():
    """初始化会话状态"""
    defaults = {
        'logged_in': False,
        'store_name': "",
        'user_id': "",
        'login_time': None,
        'is_admin': False,
        'data_loaded': False,
        'setup_complete': False,
        'google_sheets_client': None
    }
    
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

def load_persistent_data():
    """加载持久化数据"""
    if not st.session_state.data_loaded and st.session_state.google_sheets_client:
        st.session_state.data_loaded = True

# 检查Google Sheets配置
def check_google_sheets_setup():
    """检查Google Sheets配置是否完成"""
    try:
        if "google_sheets" not in st.secrets:
            return False
        
        required_fields = ["type", "project_id", "private_key_id", "private_key", "client_email", "client_id"]
        google_config = st.secrets["google_sheets"]
        
        for field in required_fields:
            if field not in google_config:
                return False
        
        return True
    except:
        return False

# 显示设置指南
def show_setup_guide():
    """显示Google Sheets设置指南"""
    st.markdown("""
        <div class="setup-guide">
            <h3>🔧 数据库设置指南</h3>
            <p>请按以下步骤配置云数据库：</p>
        </div>
    """, unsafe_allow_html=True)
    
    with st.expander("📋 详细配置步骤", expanded=True):
        st.markdown("""
        ### 第一步：创建Google Cloud项目
        
        1. 访问 [Google Cloud Console](https://console.cloud.google.com/)
        2. 创建新项目或选择现有项目
        3. 启用以下API：
           - Google Sheets API
           - Google Drive API
        
        ### 第二步：创建服务账号
        
        1. 在Google Cloud Console中，前往 "IAM & Admin" > "Service Accounts"
        2. 点击 "Create Service Account"
        3. 输入服务账号名称，如 "streamlit-sheets-access"
        4. 点击 "Create and Continue"
        5. 跳过权限设置，点击 "Done"
        
        ### 第三步：生成密钥
        
        1. 点击刚创建的服务账号
        2. 切换到 "Keys" 标签页
        3. 点击 "Add Key" > "Create new key"
        4. 选择 "JSON" 格式
        5. 下载JSON密钥文件
        
        ### 第四步：配置Streamlit Secrets
        
        在Streamlit应用的 `.streamlit/secrets.toml` 文件中添加：
        
        ```toml
        [google_sheets]
        type = "service_account"
        project_id = "your-project-id"
        private_key_id = "your-private-key-id"
        private_key = "-----BEGIN PRIVATE KEY-----\\n...\\n-----END PRIVATE KEY-----\\n"
        client_email = "your-service-account@your-project.iam.gserviceaccount.com"
        client_id = "your-client-id"
        auth_uri = "https://accounts.google.com/o/oauth2/auth"
        token_uri = "https://oauth2.googleapis.com/token"
        auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs"
        client_x509_cert_url = "https://www.googleapis.com/robot/v1/metadata/x509/your-service-account%40your-project.iam.gserviceaccount.com"
        
        # 可选：管理员邮箱（用于共享表格）
        user_email = "your-admin-email@gmail.com"
        ```
        
        ### 第五步：安装依赖
        
        在 `requirements.txt` 中添加：
        ```
        gspread
        google-auth
        google-auth-oauthlib
        google-auth-httplib2
        ```
        
        ### 第六步：重新部署应用
        
        配置完成后，重新部署Streamlit应用即可开始使用。
        
        ### 🔒 安全提示
        
        - 服务账号密钥包含敏感信息，请妥善保管
        - 建议定期轮换密钥
        - 不要将密钥文件提交到代码仓库
        """)

# 初始化
init_session_state()

# 主标题
st.markdown('<h1 class="main-header">📊 门店报表查询系统</h1>', unsafe_allow_html=True)

# 检查Google Sheets配置
if not check_google_sheets_setup():
    st.error("❌ 云数据库配置不完整")
    show_setup_guide()
    st.stop()

# 初始化Google Sheets客户端
if not st.session_state.google_sheets_client:
    with st.spinner("🔗 连接云数据库..."):
        gc = get_google_sheets_client()
        if gc:
            st.session_state.google_sheets_client = gc
            st.session_state.setup_complete = True
            st.success("✅ 云数据库连接成功！")
        else:
            st.error("❌ 云数据库连接失败")
            st.stop()

# 加载数据
load_persistent_data()

# 获取系统信息
gc = st.session_state.google_sheets_client
system_info = get_system_info(gc)

# 显示系统状态
if system_info.get('last_update'):
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("总门店数", system_info.get('total_stores', 0))
    with col2:
        st.metric("授权用户数", system_info.get('total_users', 0))
    with col3:
        last_update = system_info.get('last_update', '')
        if last_update:
            try:
                dt = datetime.fromisoformat(last_update)
                st.metric("最后更新", dt.strftime("%m-%d %H:%M"))
            except:
                st.metric("最后更新", last_update[:16])

# 侧边栏
with st.sidebar:
    st.title("⚙️ 系统功能")
    
    # 显示连接状态
    if st.session_state.setup_complete:
        st.success("🔗 云数据库已连接")
    
    # 用户类型选择
    user_type = st.radio(
        "选择用户类型",
        ["普通用户", "管理员"],
        help="管理员可上传文件，普通用户只能查询"
    )
    
    # 管理员功能
    if user_type == "管理员":
        st.divider()
        st.subheader("🔐 管理员登录")
        
        admin_password = st.text_input("管理员密码", type="password")
        
        if st.button("验证管理员身份"):
            if admin_password == ADMIN_PASSWORD:
                st.session_state.is_admin = True
                st.success("✅ 管理员验证成功！")
                st.rerun()
            else:
                st.error("❌ 密码错误！")
                st.session_state.is_admin = False
        
        # 管理员已登录
        if st.session_state.is_admin:
            st.divider()
            st.subheader("📁 文件管理")
            
            # 上传权限表
            permissions_file = st.file_uploader(
                "上传门店权限表", 
                type=['xlsx', 'xls'],
                help="包含门店名称和人员编号的Excel文件"
            )
            
            if permissions_file:
                try:
                    df = pd.read_excel(permissions_file)
                    
                    # 验证文件格式
                    if len(df.columns) >= 2:
                        with st.spinner("💾 保存权限数据到云数据库..."):
                            if save_permissions_to_sheets(df, gc):
                                # 统计信息
                                total_users = len(df)
                                unique_stores = df.iloc[:, 0].nunique()
                                
                                st.success(f"✅ 权限表已上传：{total_users} 个用户，{unique_stores} 个门店")
                                st.balloons()
                            else:
                                st.error("❌ 保存权限表失败")
                    else:
                        st.error("❌ 权限表格式错误：至少需要两列（门店名称、人员编号）")
                        
                except Exception as e:
                    st.error(f"❌ 读取权限表失败：{str(e)}")
            
            # 显示当前权限表状态
            with st.spinner("📋 加载权限表..."):
                permissions_data = load_permissions_from_sheets(gc)
            
            if permissions_data is not None:
                df = permissions_data
                st.info(f"📋 当前权限表：{len(df)} 个用户，{df.iloc[:, 0].nunique()} 个门店")
                
                if st.checkbox("查看权限表预览"):
                    st.dataframe(df.head(10), use_container_width=True)
                    
                    # 门店分布统计
                    if st.checkbox("查看门店分布"):
                        store_counts = df.iloc[:, 0].value_counts()
                        st.bar_chart(store_counts)
            
            st.divider()
            
            # 上传财务报表
            reports_file = st.file_uploader(
                "上传财务报表", 
                type=['xlsx', 'xls'],
                help="包含多个门店Sheet的Excel文件"
            )
            
            if reports_file:
                try:
                    with st.spinner("📊 处理报表文件..."):
                        excel_file = pd.ExcelFile(reports_file)
                        sheets = excel_file.sheet_names
                        
                        # 批量处理sheet
                        reports_dict = {}
                        progress_bar = st.progress(0)
                        
                        for i, sheet in enumerate(sheets):
                            try:
                                df = pd.read_excel(reports_file, sheet_name=sheet)
                                if not df.empty:
                                    reports_dict[sheet] = df
                                progress_bar.progress((i + 1) / len(sheets))
                            except Exception as e:
                                st.warning(f"⚠️ 跳过Sheet '{sheet}'：{str(e)}")
                                continue
                        
                        progress_bar.empty()
                    
                    # 保存到云数据库
                    with st.spinner("💾 保存报表数据到云数据库..."):
                        if save_reports_to_sheets(reports_dict, gc):
                            st.success(f"✅ 报表已上传：{len(reports_dict)} 个门店")
                            st.info("包含的门店：" + ", ".join(list(reports_dict.keys())[:10]) + 
                                   ("..." if len(reports_dict) > 10 else ""))
                            st.balloons()
                        else:
                            st.error("❌ 保存报表失败")
                        
                except Exception as e:
                    st.error(f"❌ 读取报表失败：{str(e)}")
            
            # 显示当前报表状态
            with st.spinner("📊 加载报表信息..."):
                reports_data = load_reports_from_sheets(gc)
            
            if reports_data:
                st.info(f"📊 当前报表：{len(reports_data)} 个门店")
                
                if st.checkbox("查看已上传的门店列表"):
                    stores = list(reports_data.keys())
                    for i in range(0, len(stores), 3):
                        cols = st.columns(3)
                        for j, store in enumerate(stores[i:i+3]):
                            if j < len(cols):
                                with cols[j]:
                                    st.write(f"• {store}")
            
            st.divider()
            
            # 管理功能
            st.subheader("🛠️ 管理功能")
            
            col1, col2 = st.columns(2)
            
            with col1:
                if st.button("🔄 重新加载数据", use_container_width=True):
                    st.session_state.data_loaded = False
                    st.cache_resource.clear()
                    st.success("✅ 数据已重新加载")
                    st.rerun()
            
            with col2:
                if st.button("📊 查看数据表格", use_container_width=True):
                    try:
                        spreadsheet = get_or_create_spreadsheet(gc)
                        if spreadsheet:
                            st.success("📋 云数据表格链接：")
                            st.write(f"🔗 [点击打开数据表格]({spreadsheet.url})")
                    except:
                        st.error("❌ 无法获取表格链接")
            
            if st.button("🚪 退出管理员", use_container_width=True):
                st.session_state.is_admin = False
                st.rerun()
    
    # 普通用户登录状态
    else:
        if st.session_state.logged_in:
            st.divider()
            st.subheader("👤 当前登录")
            st.info(f"**门店：** {st.session_state.store_name}")
            st.info(f"**编号：** {st.session_state.user_id}")
            if st.session_state.login_time:
                st.info(f"**时间：** {st.session_state.login_time}")
            
            if st.button("🚪 退出登录", use_container_width=True):
                st.session_state.logged_in = False
                st.session_state.store_name = ""
                st.session_state.user_id = ""
                st.session_state.login_time = None
                st.rerun()

# 主界面内容
if user_type == "管理员" and st.session_state.is_admin:
    # 管理员界面
    st.markdown("""
        <div class="admin-panel">
            <h3>👨‍💼 管理员控制面板</h3>
            <p>数据将永久保存在云端，支持多用户实时访问</p>
        </div>
    """, unsafe_allow_html=True)
    
    # 系统概览
    permissions_data = load_permissions_from_sheets(gc)
    reports_data = load_reports_from_sheets(gc)
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        permissions_status = "已上传" if permissions_data is not None else "未上传"
        permissions_count = len(permissions_data) if permissions_data is not None else 0
        st.metric("权限表状态", permissions_status, f"{permissions_count} 用户")
    
    with col2:
        reports_count = len(reports_data)
        st.metric("报表门店数", f"{reports_count} 个", "已就绪" if reports_count > 0 else "未上传")
    
    with col3:
        if permissions_data is not None:
            unique_stores = permissions_data.iloc[:, 0].nunique()
            st.metric("授权门店数", f"{unique_stores} 个")
        else:
            st.metric("授权门店数", "0 个")
    
    with col4:
        last_update = system_info.get('last_update')
        if last_update:
            try:
                dt = datetime.fromisoformat(last_update)
                update_time = dt.strftime("%H:%M")
                st.metric("最后更新", update_time)
            except:
                st.metric("最后更新", last_update[:16])
        else:
            st.metric("最后更新", "无")

elif user_type == "管理员" and not st.session_state.is_admin:
    # 提示输入管理员密码
    st.info("👈 请在左侧边栏输入管理员密码以访问管理功能")

else:
    # 普通用户界面
    if not st.session_state.logged_in:
        # 登录界面
        st.subheader("🔐 用户登录")
        
        # 加载权限数据
        with st.spinner("🔍 加载用户权限..."):
            permissions_data = load_permissions_from_sheets(gc)
        
        if permissions_data is None:
            st.markdown("""
                <div class="warning-message">
                    <h4>⚠️ 系统维护中</h4>
                    <p>系统暂无数据，请联系管理员上传权限表和报表文件</p>
                </div>
            """, unsafe_allow_html=True)
        else:
            if len(permissions_data.columns) >= 2:
                store_column = permissions_data.columns[0]
                id_column = permissions_data.columns[1]
                
                # 数据清理和转换
                permissions_data[store_column] = permissions_data[store_column].astype(str).str.strip()
                permissions_data[id_column] = permissions_data[id_column].astype(str).str.strip()
                
                # 获取门店列表
                stores = sorted(permissions_data[store_column].unique().tolist())
                
                # 登录表单
                col1, col2, col3 = st.columns([1, 2, 1])
                
                with col2:
                    st.markdown('<div class="search-container">', unsafe_allow_html=True)
                    
                    with st.form("login_form"):
                        st.markdown("#### 请输入登录信息")
                        
                        # 门店选择（支持搜索）
                        selected_store = st.selectbox(
                            "选择门店", 
                            stores,
                            help="请选择您所属的门店"
                        )
                        
                        # 人员编号输入
                        user_id = st.text_input(
                            "人员编号", 
                            placeholder="请输入您的人员编号",
                            help="请输入系统分配给您的人员编号"
                        )
                        
                        # 登录按钮
                        col_a, col_b, col_c = st.columns([1, 1, 1])
                        with col_b:
                            submit = st.form_submit_button("🚀 登录", use_container_width=True)
                        
                        if submit:
                            if selected_store and user_id.strip():
                                # 验证权限
                                if verify_user_permission(selected_store, user_id.strip(), permissions_data):
                                    st.session_state.logged_in = True
                                    st.session_state.store_name = selected_store
                                    st.session_state.user_id = user_id.strip()
                                    st.session_state.login_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                    st.success("✅ 登录成功！正在跳转...")
                                    st.balloons()
                                    time.sleep(1)
                                    st.rerun()
                                else:
                                    st.error("❌ 门店或人员编号错误！请检查后重试。")
                            else:
                                st.warning("⚠️ 请填写完整的登录信息")
                    
                    st.markdown('</div>', unsafe_allow_html=True)
    
    else:
        # 已登录 - 显示报表
        st.markdown(f"""
            <div class="store-info">
                <h3>🏪 {st.session_state.store_name}</h3>
                <p><strong>操作员：</strong>{st.session_state.user_id} &nbsp;|&nbsp; <strong>登录时间：</strong>{st.session_state.login_time}</p>
            </div>
        """, unsafe_allow_html=True)
        
        # 加载报表数据
        with st.spinner("📊 加载报表数据..."):
            reports_data = load_reports_from_sheets(gc)
        
        # 查找对应的报表
        matching_sheets = find_matching_reports(st.session_state.store_name, reports_data)
        
        if matching_sheets:
            # 如果有多个匹配的sheet，让用户选择
            if len(matching_sheets) > 1:
                selected_sheet = st.selectbox(
                    "🔍 找到多个相关报表，请选择：", 
                    matching_sheets,
                    help="系统找到了多个可能匹配的报表"
                )
            else:
                selected_sheet = matching_sheets[0]
                st.info(f"📊 已找到报表：{selected_sheet}")
            
            # 获取报表数据
            df = reports_data[selected_sheet]
            
            # 报表操作界面
            st.subheader(f"📈 财务报表 - {st.session_state.store_name}")
            
            # 搜索和过滤工具
            st.markdown('<div class="search-container">', unsafe_allow_html=True)
            
            col1, col2, col3 = st.columns([2, 1, 1])
            
            with col1:
                search_term = st.text_input(
                    "🔍 搜索报表内容", 
                    placeholder="输入关键词搜索...",
                    help="支持搜索所有列的内容"
                )
            
            with col2:
                n_rows = st.selectbox("显示行数", [10, 25, 50, 100, "全部"])
            
            with col3:
                show_analysis = st.checkbox("💰 显示应收未收额", value=True)
            
            st.markdown('</div>', unsafe_allow_html=True)
            
            # 应用搜索过滤
            if search_term:
                mask = df.astype(str).apply(
                    lambda x: x.str.contains(search_term, case=False, na=False)
                ).any(axis=1)
                filtered_df = df[mask]
                st.info(f"🔍 找到 {len(filtered_df)} 条包含 '{search_term}' 的记录")
            else:
                filtered_df = df
            
            # 显示数据统计
            total_rows = len(filtered_df)
            st.markdown(f"""
                <div style="background-color: #f8f9fa; padding: 1rem; border-radius: 8px; border-left: 4px solid #007bff; margin: 0.5rem 0;">
                    📊 <strong>数据统计：</strong>共 {total_rows} 条记录 | 
                    📅 <strong>报表列数：</strong>{len(df.columns)} 列
                </div>
            """, unsafe_allow_html=True)
            
            # 显示应收未收额分析（放在数据表格前面）
            if show_analysis:
                st.divider()
                st.subheader("💰 应收未收额分析")
                
                try:
                    analysis_results = analyze_receivable_data(df)
                    
                    if '应收-未收额' in analysis_results:
                        data = analysis_results['应收-未收额']
                        amount = data['amount']
                        
                        # 根据金额正负显示不同样式
                        if amount < 0:
                            # 负数 - 门店会收到退款（标绿）
                            st.markdown(f"""
                                <div class="receivable-negative">
                                    <h2 style="margin: 0; font-size: 2.5rem;">💚 ¥{abs(amount):,.2f}</h2>
                                    <p style="margin: 0.5rem 0 0 0; font-size: 1.2rem;">门店将收到退款</p>
                                    <p style="margin: 0.5rem 0 0 0; font-size: 0.9rem;">（金额为负，系统将退款给门店）</p>
                                </div>
                            """, unsafe_allow_html=True)
                            
                            # 显示成功状态的指标卡
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                st.metric("状态", "退款", "门店将收到退款", delta_color="normal")
                            with col2:
                                st.metric("退款金额", f"¥{abs(amount):,.2f}", "系统处理中")
                            with col3:
                                st.metric("数据来源", data['column_name'], f"第{data['row_index']+2}行")
                        else:
                            # 正数 - 门店需要付款
                            st.markdown(f"""
                                <div class="receivable-positive">
                                    <h2 style="margin: 0; font-size: 2.5rem;">💛 ¥{amount:,.2f}</h2>
                                    <p style="margin: 0.5rem 0 0 0; font-size: 1.2rem;">门店需要付款</p>
                                    <p style="margin: 0.5rem 0 0 0; font-size: 0.9rem;">（金额为正，请及时缴纳）</p>
                                </div>
                            """, unsafe_allow_html=True)
                            
                            # 显示警告状态的指标卡
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                st.metric("状态", "待付款", "门店需要付款", delta_color="off")
                            with col2:
                                st.metric("应付金额", f"¥{amount:,.2f}", "请及时处理")
                            with col3:
                                st.metric("数据来源", data['column_name'], f"第{data['row_index']+2}行")
                        
                        # 显示说明
                        with st.expander("💡 查看详细说明"):
                            st.markdown(f"""
                            ### 应收未收额说明：
                            
                            - **金额为正数**：表示门店欠款，需要向系统付款
                            - **金额为负数**：表示系统欠门店款项，门店将收到退款
                            
                            ### 处理建议：
                            
                            1. **如需付款**：请联系财务部门确认付款方式和时间
                            2. **如有退款**：退款将在月底统一处理，请注意查收
                            3. **如有疑问**：请截图保存并联系财务部门核实
                            
                            ### 数据定位：
                            - 指标名称：{data['row_name']}
                            - 匹配关键词：{data.get('matched_keyword', '应收-未收额')}
                            - 所在列：{data['column_name']}
                            - 所在行：第{data['row_index']+2}行
                            - 原始值：{data['amount']}
                            """)
                    else:
                        st.warning("⚠️ 未找到'应收-未收额'数据")
                        st.info("系统正在智能搜索财务相关数据，请查看详细调试信息")
                        
                        # 显示增强的调试信息
                        with st.expander("🔧 详细诊断报告", expanded=True):
                            
                            # 显示找到的合计列
                            if 'debug_info' in analysis_results:
                                debug = analysis_results['debug_info']
                                
                                st.write("**📊 数据表结构分析：**")
                                st.write(f"• 总列数：{len(debug['all_columns'])}")
                                st.write(f"• 识别到的数值列：{len(debug['total_columns_found'])}")
                                
                                if debug['total_columns_found']:
                                    st.write("**🎯 系统识别的可能合计列：**")
                                    for i, col in enumerate(debug['total_columns_found']):
                                        st.write(f"{i+1}. {col}")
                                
                                st.write("**📋 所有列名：**")
                                cols_display = st.columns(3)
                                for i, col in enumerate(debug['all_columns']):
                                    with cols_display[i % 3]:
                                        st.write(f"{i+1}. {col}")
                                
                                st.write("**📝 前20行项目名称：**")
                                for row_info in debug['first_20_rows']:
                                    st.write(f"• {row_info}")
                            
                            # 显示匹配尝试结果
                            if 'matches_found' in analysis_results and analysis_results['matches_found']:
                                st.write("**🔍 系统搜索到的相关项目：**")
                                
                                # 创建一个DataFrame来显示匹配结果
                                matches_df = pd.DataFrame(analysis_results['matches_found'])
                                
                                # 按是否匹配分组显示
                                matched_items = matches_df[matches_df['matched'] == True]
                                potential_items = matches_df[matches_df['matched'] == False]
                                
                                if not matched_items.empty:
                                    st.write("✅ **精确匹配的项目：**")
                                    for _, match in matched_items.iterrows():
                                        st.write(f"• 第{match['row_index']+2}行: **{match['row_name']}** (匹配关键词: {match['keyword']}) = {match['value']}")
                                
                                if not potential_items.empty:
                                    st.write("🔍 **可能相关的财务项目：**")
                                    for _, match in potential_items.head(10).iterrows():  # 只显示前10个
                                        st.write(f"• 第{match['row_index']+2}行: {match['row_name']} = {match['value']}")
                            
                            st.write("**💡 系统支持的关键词：**")
                            keywords = ['应收-未收额', '应收未收额', '应收-未收', '应收 未收额', '应收未收', 
                                       '应收-未收金额', '应收未收金额', '未收金额', '应收款', '应收账款', 
                                       '应收余额', '收支差额', '净收入', '盈亏', '利润', '净利润', 
                                       '收支结余', '结余', '差额', '总收支', '收支合计', '最终结果', '汇总金额']
                            
                            kw_cols = st.columns(3)
                            for i, kw in enumerate(keywords):
                                with kw_cols[i % 3]:
                                    st.write(f"• {kw}")
                            
                            st.write("**📞 需要帮助？**")
                            st.info("""
                            如果系统仍无法识别您的数据，可能的原因：
                            1. 表格中没有包含应收未收相关的汇总行
                            2. 数据格式需要调整
                            3. 需要在Excel中添加明确的"应收-未收额"行
                            
                            建议：在Excel表格最后添加一行，命名为"应收-未收额"，并填入对应的合计数值。
                            """)
                        
                        # 如果找到了一些财务相关数据，提供手动选择选项
                        if 'matches_found' in analysis_results and analysis_results['matches_found']:
                            potential_matches = [m for m in analysis_results['matches_found'] if m['value'] not in ['None', '', '0', 'None']]
                            
                            if potential_matches:
                                st.write("**🎯 手动选择财务数据：**")
                                
                                # 创建选择选项
                                options = []
                                for match in potential_matches[:10]:  # 最多显示10个选项
                                    label = f"{match['row_name']} = {match['value']} (第{match['row_index']+2}行, {match['column']}列)"
                                    options.append((label, match))
                                
                                if options:
                                    selected_option = st.selectbox(
                                        "如果以下数据中有您需要的应收未收额，请选择：",
                                        ["不选择"] + [opt[0] for opt in options]
                                    )
                                    
                                    if selected_option != "不选择":
                                        # 找到选中的数据
                                        selected_match = None
                                        for label, match in options:
                                            if label == selected_option:
                                                selected_match = match
                                                break
                                        
                                        if selected_match:
                                            try:
                                                # 尝试解析选中的数值
                                                val_str = str(selected_match['value']).replace(',', '').replace('¥', '').replace('￥', '').strip()
                                                amount = float(val_str)
                                                
                                                if amount < 0:
                                                    st.markdown(f"""
                                                        <div class="receivable-negative">
                                                            <h2 style="margin: 0; font-size: 2.5rem;">💚 ¥{abs(amount):,.2f}</h2>
                                                            <p style="margin: 0.5rem 0 0 0; font-size: 1.2rem;">门店将收到退款</p>
                                                            <p style="margin: 0.5rem 0 0 0; font-size: 0.9rem;">（{selected_match['row_name']}）</p>
                                                        </div>
                                                    """, unsafe_allow_html=True)
                                                else:
                                                    st.markdown(f"""
                                                        <div class="receivable-positive">
                                                            <h2 style="margin: 0; font-size: 2.5rem;">💛 ¥{amount:,.2f}</h2>
                                                            <p style="margin: 0.5rem 0 0 0; font-size: 1.2rem;">门店需要付款</p>
                                                            <p style="margin: 0.5rem 0 0 0; font-size: 0.9rem;">（{selected_match['row_name']}）</p>
                                                        </div>
                                                    """, unsafe_allow_html=True)
                                                
                                                # 显示数据来源信息
                                                col1, col2, col3 = st.columns(3)
                                                with col1:
                                                    st.metric("状态", "手动选择", "用户指定数据")
                                                with col2:
                                                    st.metric("金额", f"¥{abs(amount):,.2f}")
                                                with col3:
                                                    st.metric("数据来源", f"第{selected_match['row_index']+2}行")
                                                
                                            except ValueError:
                                                st.error(f"❌ 无法解析数值：{selected_match['value']}")
                
                except Exception as e:
                    st.error(f"❌ 分析数据时出错：{str(e)}")
                    with st.expander("🔧 错误详情"):
                        st.code(str(e))
                        st.write("**建议：**")
                        st.write("1. 检查Excel文件格式是否正确")
                        st.write("2. 确认数据表中包含财务汇总信息")
                        st.write("3. 联系技术支持获取帮助")
                
                st.divider()
            
            # 显示数据表
            if total_rows > 0:
                display_df = filtered_df.head(n_rows) if n_rows != "全部" else filtered_df
                st.dataframe(display_df, use_container_width=True, height=400)
            else:
                st.warning("没有找到符合条件的数据")
            
            # 下载功能
            st.subheader("📥 数据下载")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                # 下载完整报表
                buffer = io.BytesIO()
                with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                    df.to_excel(writer, index=False, sheet_name=st.session_state.store_name)
                
                st.download_button(
                    label="📥 下载完整报表 (Excel)",
                    data=buffer.getvalue(),
                    file_name=f"{st.session_state.store_name}_财务报表_{datetime.now().strftime('%Y%m%d')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
            
            with col2:
                # 下载筛选后的数据
                if search_term and len(filtered_df) > 0:
                    buffer_filtered = io.BytesIO()
                    with pd.ExcelWriter(buffer_filtered, engine='openpyxl') as writer:
                        filtered_df.to_excel(writer, index=False, sheet_name=st.session_state.store_name)
                    
                    st.download_button(
                        label="📥 下载筛选结果 (Excel)",
                        data=buffer_filtered.getvalue(),
                        file_name=f"{st.session_state.store_name}_筛选报表_{datetime.now().strftime('%Y%m%d')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True
                    )
                else:
                    st.button("📥 下载筛选结果 (Excel)", disabled=True, use_container_width=True,
                            help="没有筛选结果可下载")
            
            with col3:
                # 下载CSV格式
                csv = df.to_csv(index=False, encoding='utf-8-sig')
                st.download_button(
                    label="📥 下载CSV格式",
                    data=csv,
                    file_name=f"{st.session_state.store_name}_报表_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
        
        else:
            st.error(f"❌ 未找到门店 '{st.session_state.store_name}' 的报表")
            st.markdown("""
                <div class="warning-message">
                    <h4>🔍 找不到报表？</h4>
                    <p><strong>可能的原因：</strong></p>
                    <ul>
                        <li>管理员尚未上传包含该门店的报表文件</li>
                        <li>报表中的Sheet名称与门店名称不匹配</li>
                        <li>云数据库数据同步延迟</li>
                    </ul>
                    <p><strong>解决方案：</strong></p>
                    <ul>
                        <li>联系管理员确认报表是否已上传</li>
                        <li>确认门店名称是否正确</li>
                        <li>稍后重试或重新登录</li>
                    </ul>
                </div>
            """, unsafe_allow_html=True)

# 页脚
st.divider()
st.markdown("""
    <div style="text-align: center; color: #888; font-size: 0.8rem; padding: 1rem;">
        <p>🏪 门店报表查询系统 v5.0 - 应收未收额专用版</p>
        <p>💾 数据永久保存 | 🌐 支持多用户实时访问 | 🔄 自动同步更新</p>
        <p>技术支持：IT部门 | 建议使用Chrome浏览器访问</p>
    </div>
""", unsafe_allow_html=True)
