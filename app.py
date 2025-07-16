import streamlit as st
import pandas as pd
import io
import json
from datetime import datetime
import time
import gspread
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import Flow
import logging
from typing import Optional, Dict, Any, List
import hashlib
import traceback
from contextlib import contextmanager
import zlib
import base64
import urllib.parse as urlparse

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 页面配置
st.set_page_config(
    page_title="门店报表查询系统", 
    page_icon="📊",
    layout="wide"
)

# 系统配置
ADMIN_PASSWORD = st.secrets.get("system", {}).get("admin_password", "admin123")
PERMISSIONS_SHEET_NAME = "store_permissions"
REPORTS_SHEET_NAME = "store_reports"
SYSTEM_INFO_SHEET_NAME = "system_info"
MAX_RETRIES = 3
RETRY_DELAY = 1
MAX_CHUNK_SIZE = 25000
CACHE_DURATION = 300
COMPRESSION_LEVEL = 9

# OAuth配置
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/drive.file'
]

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
    .oauth-panel {
        background: linear-gradient(135deg, #a8edea 0%, #fed6e3 100%);
        padding: 2rem;
        border-radius: 15px;
        border: 2px solid #48cab2;
        margin: 1rem 0;
        text-align: center;
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
    .status-success {
        background: #d4edda;
        color: #155724;
        padding: 1rem;
        border-radius: 8px;
        border: 1px solid #c3e6cb;
        margin: 1rem 0;
    }
    .status-error {
        background: #f8d7da;
        color: #721c24;
        padding: 1rem;
        border-radius: 8px;
        border: 1px solid #f5c6cb;
        margin: 1rem 0;
    }
    .auth-button {
        display: inline-block;
        padding: 12px 24px;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        text-decoration: none;
        border-radius: 8px;
        font-weight: bold;
        border: none;
        cursor: pointer;
        font-size: 16px;
    }
    .auth-button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0,0,0,0.2);
    }
    </style>
""", unsafe_allow_html=True)

class OAuthError(Exception):
    """OAuth认证异常"""
    pass

@contextmanager
def error_handler(operation_name: str):
    """通用错误处理上下文管理器"""
    try:
        yield
    except Exception as e:
        logger.error(f"{operation_name} 失败: {str(e)}")
        logger.error(traceback.format_exc())
        st.error(f"❌ {operation_name} 失败: {str(e)}")
        raise

def get_oauth_flow():
    """创建OAuth flow"""
    try:
        client_config = {
            "web": {
                "client_id": st.secrets["google_oauth"]["client_id"],
                "client_secret": st.secrets["google_oauth"]["client_secret"],
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token"
            }
        }
        
        flow = Flow.from_client_config(
            client_config,
            scopes=SCOPES,
            redirect_uri=st.secrets["google_oauth"]["redirect_uri"]
        )
        
        return flow
    except KeyError as e:
        st.error(f"❌ OAuth配置缺失: {str(e)}")
        st.markdown("""
        请确保 `.streamlit/secrets.toml` 包含以下配置：
        ```toml
        [google_oauth]
        client_id = "你的客户端ID"
        client_secret = "你的客户端密钥"
        redirect_uri = "https://你的应用域名.streamlit.app"
        ```
        """)
        return None
    except Exception as e:
        st.error(f"❌ OAuth flow创建失败: {str(e)}")
        return None

def show_oauth_authorization():
    """显示OAuth授权界面"""
    st.markdown('<div class="oauth-panel">', unsafe_allow_html=True)
    st.markdown("### 🔐 Google账号授权")
    st.markdown("使用你的个人Google账号登录，享受15GB存储空间！")
    
    flow = get_oauth_flow()
    if not flow:
        st.error("❌ OAuth配置错误，请检查secrets配置")
        return False
    
    # 生成授权URL
    auth_url, state = flow.authorization_url(
        access_type='offline',
        include_granted_scopes='true',
        prompt='consent'
    )
    
    # 存储state用于验证
    st.session_state['oauth_state'] = state
    
    # 显示授权按钮
    st.markdown(f"""
    <a href="{auth_url}" target="_blank" class="auth-button">
        🚀 点击授权Google账号
    </a>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # 处理授权回调
    query_params = st.experimental_get_query_params()
    
    if 'code' in query_params:
        try:
            # 验证state
            if 'state' in query_params:
                received_state = query_params['state'][0]
                expected_state = st.session_state.get('oauth_state', '')
                if received_state != expected_state:
                    st.error("❌ 授权验证失败：状态不匹配")
                    return False
            
            # 获取授权码
            auth_code = query_params['code'][0]
            
            with st.spinner("正在完成授权..."):
                # 交换token
                flow.fetch_token(code=auth_code)
                credentials = flow.credentials
                
                # 存储凭据
                st.session_state['google_credentials'] = {
                    'token': credentials.token,
                    'refresh_token': credentials.refresh_token,
                    'id_token': credentials.id_token,
                    'token_uri': credentials.token_uri,
                    'client_id': credentials.client_id,
                    'client_secret': credentials.client_secret,
                    'scopes': credentials.scopes
                }
                
                # 清理URL参数
                st.experimental_set_query_params()
                
                st.success("✅ 授权成功！")
                st.balloons()
                time.sleep(1)
                st.rerun()
                
        except Exception as e:
            st.error(f"❌ 授权处理失败: {str(e)}")
            logger.error(f"OAuth授权失败: {str(e)}")
            return False
    
    else:
        st.markdown("""
        **操作步骤：**
        1. 点击上方按钮
        2. 选择你的Google账号
        3. 同意权限请求
        4. 系统将自动完成授权
        """)
    
    st.markdown('</div>', unsafe_allow_html=True)
    return False

def get_google_sheets_client():
    """获取Google Sheets客户端（OAuth版本）"""
    
    # 检查是否已有凭据
    if 'google_credentials' not in st.session_state:
        return None
    
    try:
        # 从session state恢复凭据
        cred_data = st.session_state['google_credentials']
        credentials = Credentials(
            token=cred_data['token'],
            refresh_token=cred_data.get('refresh_token'),
            id_token=cred_data.get('id_token'),
            token_uri=cred_data['token_uri'],
            client_id=cred_data['client_id'],
            client_secret=cred_data['client_secret'],
            scopes=cred_data['scopes']
        )
        
        # 检查并刷新token
        if credentials.expired and credentials.refresh_token:
            request = Request()
            credentials.refresh(request)
            
            # 更新存储的凭据
            st.session_state['google_credentials'].update({
                'token': credentials.token,
                'id_token': credentials.id_token
            })
        
        # 创建客户端
        client = gspread.authorize(credentials)
        
        # 测试连接
        try:
            client.openall()  # 这会验证权限
        except Exception as e:
            if "unauthorized" in str(e).lower() or "invalid" in str(e).lower():
                # 凭据无效，清除并重新授权
                del st.session_state['google_credentials']
                st.error("❌ 授权已过期，请重新授权")
                st.rerun()
            raise
        
        logger.info("Google Sheets客户端创建成功（OAuth）")
        return client
        
    except Exception as e:
        logger.error(f"OAuth客户端创建失败: {str(e)}")
        # 如果凭据有问题，清除并重新授权
        if 'google_credentials' in st.session_state:
            del st.session_state['google_credentials']
        raise OAuthError(f"认证失败: {str(e)}")

def retry_operation(func, *args, max_retries=MAX_RETRIES, delay=RETRY_DELAY, **kwargs):
    """重试操作装饰器"""
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            error_msg = str(e)
            if "unauthorized" in error_msg.lower() or "invalid" in error_msg.lower():
                # 认证问题不重试
                raise OAuthError(error_msg)
            if attempt == max_retries - 1:
                logger.error(f"操作失败，已重试 {max_retries} 次: {error_msg}")
                raise
            logger.warning(f"操作失败，第 {attempt + 1} 次重试: {error_msg}")
            time.sleep(delay * (attempt + 1))

def compress_data(data: str) -> str:
    """压缩数据"""
    try:
        compressed = zlib.compress(data.encode('utf-8'), COMPRESSION_LEVEL)
        encoded = base64.b64encode(compressed).decode('ascii')
        logger.info(f"数据压缩: {len(data)} -> {len(encoded)} bytes (压缩率: {(1-len(encoded)/len(data))*100:.1f}%)")
        return encoded
    except Exception as e:
        logger.error(f"数据压缩失败: {str(e)}")
        return data

def decompress_data(compressed_data: str) -> str:
    """解压数据"""
    try:
        decoded = base64.b64decode(compressed_data.encode('ascii'))
        decompressed = zlib.decompress(decoded).decode('utf-8')
        return decompressed
    except Exception:
        # 如果解压失败，说明数据未压缩
        return compressed_data

def get_or_create_spreadsheet(gc, name="门店报表系统数据"):
    """获取或创建表格"""
    def _operation():
        try:
            spreadsheet = gc.open(name)
            logger.info(f"表格 '{name}' 已存在")
            return spreadsheet
        except gspread.SpreadsheetNotFound:
            logger.info(f"创建新表格 '{name}'")
            spreadsheet = gc.create(name)
            return spreadsheet
    
    return retry_operation(_operation)

def get_or_create_worksheet(spreadsheet, name, rows=500, cols=10):
    """获取或创建工作表"""
    def _operation():
        try:
            worksheet = spreadsheet.worksheet(name)
            logger.info(f"工作表 '{name}' 已存在")
            return worksheet
        except gspread.WorksheetNotFound:
            logger.info(f"创建新工作表 '{name}'")
            worksheet = spreadsheet.add_worksheet(title=name, rows=rows, cols=cols)
            return worksheet
    
    return retry_operation(_operation)

def clean_and_compress_dataframe(df: pd.DataFrame) -> str:
    """清理并压缩DataFrame"""
    try:
        df_cleaned = df.copy()
        for col in df_cleaned.columns:
            df_cleaned[col] = df_cleaned[col].astype(str)
            df_cleaned[col] = df_cleaned[col].replace({
                'nan': '', 'None': '', 'NaT': '', 'null': '', '<NA>': ''
            })
            # 限制字符串长度
            df_cleaned[col] = df_cleaned[col].apply(
                lambda x: x[:800] + '...' if len(str(x)) > 800 else x
            )
        
        # 转换为JSON
        json_data = df_cleaned.to_json(orient='records', force_ascii=False, ensure_ascii=False)
        
        # 压缩数据
        compressed_data = compress_data(json_data)
        
        logger.info(f"数据处理完成: {len(df_cleaned)} 行 -> {len(compressed_data)} 字符")
        return compressed_data
        
    except Exception as e:
        logger.error(f"清理压缩DataFrame失败: {str(e)}")
        raise

def save_permissions_to_sheets(df: pd.DataFrame, gc) -> bool:
    """保存权限数据"""
    with error_handler("保存权限数据"):
        def _save_operation():
            spreadsheet = get_or_create_spreadsheet(gc)
            worksheet = get_or_create_worksheet(spreadsheet, PERMISSIONS_SHEET_NAME, rows=100, cols=5)
            
            # 清空现有数据
            worksheet.clear()
            time.sleep(1)
            
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # 压缩权限数据
            compressed_data = []
            for _, row in df.iterrows():
                store_name = str(row.iloc[0]).strip()
                user_id = str(row.iloc[1]).strip()
                compressed_data.append(f"{store_name}|{user_id}")
            
            # 将所有权限数据压缩到一个单元格
            all_permissions = ";".join(compressed_data)
            compressed_permissions = compress_data(all_permissions)
            
            # 保存到表格
            data = [
                ['数据类型', '压缩数据', '记录数', '更新时间'],
                ['permissions', compressed_permissions, len(df), current_time]
            ]
            
            worksheet.update('A1', data)
            logger.info(f"权限数据保存成功: {len(df)} 条记录")
            
            # 清除缓存
            if 'cache_permissions_load' in st.session_state:
                del st.session_state['cache_permissions_load']
            
            return True
        
        return retry_operation(_save_operation)

def load_permissions_from_sheets(gc) -> Optional[pd.DataFrame]:
    """加载权限数据"""
    # 检查缓存
    if 'cache_permissions_load' in st.session_state:
        cache_data = st.session_state['cache_permissions_load']
        if time.time() - cache_data['timestamp'] < CACHE_DURATION:
            logger.info("从缓存加载权限数据")
            return cache_data['data']
    
    with error_handler("加载权限数据"):
        def _load_operation():
            try:
                spreadsheet = get_or_create_spreadsheet(gc)
                worksheet = spreadsheet.worksheet(PERMISSIONS_SHEET_NAME)
                data = worksheet.get_all_values()
                
                if len(data) <= 1:
                    logger.info("权限表为空")
                    return None
                
                # 解析压缩数据
                if len(data) >= 2 and len(data[1]) >= 2:
                    compressed_data = data[1][1]
                    
                    # 解压数据
                    decompressed_data = decompress_data(compressed_data)
                    
                    # 解析权限数据
                    permissions = []
                    for item in decompressed_data.split(';'):
                        if '|' in item:
                            store_name, user_id = item.split('|', 1)
                            permissions.append({
                                '门店名称': store_name.strip(),
                                '人员编号': user_id.strip()
                            })
                    
                    if permissions:
                        result_df = pd.DataFrame(permissions)
                        # 移除空行
                        result_df = result_df[
                            (result_df['门店名称'] != '') & 
                            (result_df['人员编号'] != '')
                        ]
                        
                        logger.info(f"权限数据加载成功: {len(result_df)} 条记录")
                        
                        # 设置缓存
                        st.session_state['cache_permissions_load'] = {
                            'data': result_df,
                            'timestamp': time.time()
                        }
                        return result_df
                
                return None
                
            except gspread.WorksheetNotFound:
                logger.info("权限表不存在")
                return None
        
        return retry_operation(_load_operation)

def save_reports_to_sheets(reports_dict: Dict[str, pd.DataFrame], gc) -> bool:
    """保存报表数据"""
    with error_handler("保存报表数据"):
        def _save_operation():
            spreadsheet = get_or_create_spreadsheet(gc)
            worksheet = get_or_create_worksheet(spreadsheet, REPORTS_SHEET_NAME, rows=300, cols=8)
            
            # 清空现有数据
            with st.spinner("清理旧数据..."):
                worksheet.clear()
                time.sleep(1)
            
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            all_data = [['门店名称', '压缩数据', '行数', '列数', '更新时间', '分片序号', '总分片数', '数据哈希']]
            
            with st.spinner("压缩并保存数据..."):
                total_stores = len(reports_dict)
                progress_bar = st.progress(0)
                
                for idx, (store_name, df) in enumerate(reports_dict.items()):
                    try:
                        # 清理并压缩数据
                        compressed_data = clean_and_compress_dataframe(df)
                        
                        # 计算哈希
                        data_hash = hashlib.md5(compressed_data.encode('utf-8')).hexdigest()[:16]
                        
                        # 检查是否需要分片
                        if len(compressed_data) <= MAX_CHUNK_SIZE:
                            all_data.append([
                                store_name, compressed_data, len(df), len(df.columns), 
                                current_time, "1", "1", data_hash
                            ])
                        else:
                            # 分片存储
                            chunks = []
                            for i in range(0, len(compressed_data), MAX_CHUNK_SIZE):
                                chunks.append(compressed_data[i:i + MAX_CHUNK_SIZE])
                            
                            total_chunks = len(chunks)
                            for chunk_idx, chunk in enumerate(chunks):
                                chunk_name = f"{store_name}_分片{chunk_idx+1}"
                                all_data.append([
                                    chunk_name, chunk, len(df), len(df.columns),
                                    current_time, str(chunk_idx+1), str(total_chunks), data_hash
                                ])
                        
                        # 更新进度
                        progress_bar.progress((idx + 1) / total_stores)
                        logger.info(f"处理完成: {store_name}")
                        
                    except Exception as e:
                        logger.error(f"处理 {store_name} 时出错: {str(e)}")
                        continue
                
                progress_bar.empty()
            
            # 分批保存数据
            batch_size = 15
            if len(all_data) > 1:
                for i in range(1, len(all_data), batch_size):
                    batch_data = all_data[i:i+batch_size]
                    
                    if i == 1:
                        worksheet.update('A1', [all_data[0]] + batch_data)
                    else:
                        row_num = i + 1
                        worksheet.update(f'A{row_num}', batch_data)
                    
                    time.sleep(0.8)
            
            logger.info(f"报表数据保存完成: {len(all_data) - 1} 条记录")
            
            # 清除缓存
            if 'cache_reports_load' in st.session_state:
                del st.session_state['cache_reports_load']
            
            return True
        
        return retry_operation(_save_operation)

def load_reports_from_sheets(gc) -> Dict[str, pd.DataFrame]:
    """加载报表数据"""
    # 检查缓存
    if 'cache_reports_load' in st.session_state:
        cache_data = st.session_state['cache_reports_load']
        if time.time() - cache_data['timestamp'] < CACHE_DURATION:
            logger.info("从缓存加载报表数据")
            return cache_data['data']
    
    with error_handler("加载报表数据"):
        def _load_operation():
            try:
                spreadsheet = get_or_create_spreadsheet(gc)
                worksheet = spreadsheet.worksheet(REPORTS_SHEET_NAME)
                data = worksheet.get_all_values()
                
                if len(data) <= 1:
                    logger.info("报表数据为空")
                    return {}
                
                # 重构数据
                reports_dict = {}
                fragments_dict = {}
                
                for row in data[1:]:
                    if len(row) >= 7:
                        store_name = row[0]
                        compressed_data = row[1]
                        rows_count = row[2]
                        cols_count = row[3]
                        update_time = row[4]
                        chunk_num = row[5]
                        total_chunks = row[6]
                        data_hash = row[7] if len(row) > 7 else ''
                        
                        # 处理分片数据
                        if '_分片' in store_name:
                            base_name = store_name.split('_分片')[0]
                            if base_name not in fragments_dict:
                                fragments_dict[base_name] = []
                            
                            fragments_dict[base_name].append({
                                'data': compressed_data,
                                'chunk_num': chunk_num,
                                'total_chunks': total_chunks,
                                'data_hash': data_hash
                            })
                        else:
                            fragments_dict[store_name] = [{
                                'data': compressed_data,
                                'chunk_num': '1',
                                'total_chunks': '1',
                                'data_hash': data_hash
                            }]
                
                # 重构所有数据
                for store_name, fragments in fragments_dict.items():
                    try:
                        if len(fragments) == 1:
                            compressed_data = fragments[0]['data']
                        else:
                            # 按顺序重组分片
                            fragments.sort(key=lambda x: int(x['chunk_num']))
                            compressed_data = ''.join([frag['data'] for frag in fragments])
                        
                        # 解压数据
                        json_data = decompress_data(compressed_data)
                        
                        # 解析为DataFrame
                        df = pd.read_json(json_data, orient='records')
                        
                        if not df.empty:
                            # 数据后处理
                            if len(df) > 0:
                                first_row = df.iloc[0]
                                non_empty_count = sum(1 for val in first_row if pd.notna(val) and str(val).strip() != '')
                                if non_empty_count <= 2 and len(df) > 1:
                                    df = df.iloc[1:].reset_index(drop=True)
                            
                            # 处理表头
                            if len(df) > 1:
                                header_row = df.iloc[0].fillna('').astype(str).tolist()
                                data_rows = df.iloc[1:].copy()
                                
                                cols = []
                                for i, col in enumerate(header_row):
                                    col = str(col).strip()
                                    if col == '' or col == 'nan' or col == '0':
                                        col = f'列{i+1}' if i > 0 else '项目名称'
                                    
                                    original_col = col
                                    counter = 1
                                    while col in cols:
                                        col = f"{original_col}_{counter}"
                                        counter += 1
                                    cols.append(col)
                                
                                min_cols = min(len(data_rows.columns), len(cols))
                                cols = cols[:min_cols]
                                data_rows = data_rows.iloc[:, :min_cols]
                                data_rows.columns = cols
                                df = data_rows.reset_index(drop=True).fillna('')
                            
                            reports_dict[store_name] = df
                            logger.info(f"{store_name} 数据加载成功: {len(df)} 行")
                    
                    except Exception as e:
                        logger.error(f"解析 {store_name} 数据失败: {str(e)}")
                        continue
                
                logger.info(f"报表数据加载完成: {len(reports_dict)} 个门店")
                
                # 设置缓存
                st.session_state['cache_reports_load'] = {
                    'data': reports_dict,
                    'timestamp': time.time()
                }
                return reports_dict
                
            except gspread.WorksheetNotFound:
                logger.info("报表数据表不存在")
                return {}
        
        return retry_operation(_load_operation)

def analyze_receivable_data(df: pd.DataFrame) -> Dict[str, Any]:
    """分析应收未收额数据"""
    result = {}
    
    if len(df.columns) == 0 or len(df) == 0:
        return result
    
    # 检查第一行是否是门店名称
    first_row = df.iloc[0] if len(df) > 0 else None
    if first_row is not None:
        non_empty_count = sum(1 for val in first_row if pd.notna(val) and str(val).strip() != '')
        if non_empty_count <= 2:
            df = df.iloc[1:].reset_index(drop=True)
            result['skipped_store_name_row'] = True
    
    # 查找第69行
    target_row_index = 68
    
    if len(df) > target_row_index:
        row = df.iloc[target_row_index]
        first_col_value = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ""
        
        keywords = ['应收-未收额', '应收未收额', '应收-未收', '应收未收']
        
        for keyword in keywords:
            if keyword in first_col_value:
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
                                    'row_name': first_col_value,
                                    'row_index': target_row_index,
                                    'actual_row_number': target_row_index + 1
                                }
                                return result
                        except ValueError:
                            continue
                break
    
    # 备用查找
    if '应收-未收额' not in result:
        keywords = ['应收-未收额', '应收未收额', '应收-未收', '应收未收']
        
        for idx, row in df.iterrows():
            try:
                row_name = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ""
                if not row_name.strip():
                    continue
                
                for keyword in keywords:
                    if keyword in row_name:
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
    
    # 调试信息
    result['debug_info'] = {
        'total_rows': len(df),
        'checked_row_69': len(df) > target_row_index,
        'row_69_content': str(df.iloc[target_row_index].iloc[0]) if len(df) > target_row_index else 'N/A'
    }
    
    return result

def verify_user_permission(store_name: str, user_id: str, permissions_data: Optional[pd.DataFrame]) -> bool:
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

def find_matching_reports(store_name: str, reports_data: Dict[str, pd.DataFrame]) -> List[str]:
    """查找匹配的报表"""
    matching = []
    for sheet_name in reports_data.keys():
        if store_name in sheet_name or sheet_name in store_name:
            matching.append(sheet_name)
    return matching

def show_user_info(gc):
    """显示用户账号信息"""
    try:
        if 'google_credentials' in st.session_state:
            cred_data = st.session_state['google_credentials']
            
            # 尝试获取用户信息
            st.markdown("""
            <div class="status-success">
                ✅ <strong>已连接到你的个人Google账号</strong><br>
                🗄️ 使用个人Google Drive存储空间 (15GB)<br>
                🔒 数据安全存储在你的个人账号中
            </div>
            """, unsafe_allow_html=True)
    except Exception as e:
        logger.warning(f"获取用户信息失败: {str(e)}")

# 初始化会话状态
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
if 'store_name' not in st.session_state:
    st.session_state.store_name = ""
if 'user_id' not in st.session_state:
    st.session_state.user_id = ""
if 'is_admin' not in st.session_state:
    st.session_state.is_admin = False

# 主标题
st.markdown('<h1 class="main-header">📊 门店报表查询系统</h1>', unsafe_allow_html=True)

# Google账号认证
try:
    gc = get_google_sheets_client()
    if gc:
        show_user_info(gc)
    else:
        if show_oauth_authorization():
            st.rerun()
        else:
            st.stop()
except OAuthError:
    st.error("❌ 认证失败，请重新授权")
    if 'google_credentials' in st.session_state:
        del st.session_state['google_credentials']
    st.stop()
except Exception as e:
    st.error(f"❌ 系统初始化失败: {str(e)}")
    st.stop()

# 侧边栏
with st.sidebar:
    st.title("⚙️ 系统功能")
    
    # 系统状态
    st.subheader("📡 系统状态")
    if gc:
        st.success("🟢 个人账号已连接")
        st.info("💾 使用个人15GB存储")
    else:
        st.error("🔴 未连接")
    
    # 账号管理
    st.subheader("👤 账号管理")
    if st.button("🔄 重新授权"):
        if 'google_credentials' in st.session_state:
            del st.session_state['google_credentials']
        st.rerun()
    
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
            
            # 缓存管理
            if st.button("🗑️ 清理系统缓存"):
                cache_keys = [key for key in st.session_state.keys() if key.startswith('cache_')]
                for key in cache_keys:
                    del st.session_state[key]
                st.success("✅ 缓存已清除")
                st.rerun()
            
            # 上传权限表
            permissions_file = st.file_uploader("上传门店权限表", type=['xlsx', 'xls'])
            if permissions_file:
                try:
                    with st.spinner("处理权限表文件..."):
                        df = pd.read_excel(permissions_file)
                        if len(df.columns) >= 2:
                            with st.spinner("压缩并保存到个人Drive..."):
                                if save_permissions_to_sheets(df, gc):
                                    st.success(f"✅ 权限表已上传：{len(df)} 个用户")
                                    st.balloons()
                                else:
                                    st.error("❌ 保存失败")
                        else:
                            st.error("❌ 格式错误：需要至少两列（门店名称、人员编号）")
                except Exception as e:
                    st.error(f"❌ 处理失败：{str(e)}")
            
            # 上传财务报表
            reports_file = st.file_uploader("上传财务报表", type=['xlsx', 'xls'])
            if reports_file:
                try:
                    with st.spinner("处理报表文件..."):
                        excel_file = pd.ExcelFile(reports_file)
                        reports_dict = {}
                        
                        for sheet in excel_file.sheet_names:
                            try:
                                df = pd.read_excel(reports_file, sheet_name=sheet)
                                if not df.empty:
                                    reports_dict[sheet] = df
                                    logger.info(f"读取工作表 '{sheet}': {len(df)} 行")
                            except Exception as e:
                                logger.warning(f"跳过工作表 '{sheet}': {str(e)}")
                                continue
                        
                        if reports_dict:
                            with st.spinner("压缩并保存到个人Drive..."):
                                if save_reports_to_sheets(reports_dict, gc):
                                    st.success(f"✅ 报表已上传：{len(reports_dict)} 个门店")
                                    st.balloons()
                                else:
                                    st.error("❌ 保存失败")
                        else:
                            st.error("❌ 文件中没有有效的工作表")
                            
                except Exception as e:
                    st.error(f"❌ 处理失败：{str(e)}")
    
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
    st.markdown('<div class="admin-panel"><h3>👨‍💼 管理员控制面板</h3><p>使用个人Google账号，享受15GB存储空间和高速访问</p></div>', unsafe_allow_html=True)
    
    try:
        with st.spinner("加载数据统计..."):
            permissions_data = load_permissions_from_sheets(gc)
            reports_data = load_reports_from_sheets(gc)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            perms_count = len(permissions_data) if permissions_data is not None else 0
            st.metric("权限表用户数", perms_count)
        with col2:
            reports_count = len(reports_data)
            st.metric("报表门店数", reports_count)
        with col3:
            cache_count = len([key for key in st.session_state.keys() if key.startswith('cache_')])
            st.metric("缓存项目数", cache_count)
            
        # 数据预览
        if permissions_data is not None and len(permissions_data) > 0:
            st.subheader("👥 权限数据预览")
            st.dataframe(permissions_data.head(10), use_container_width=True)
        
        if reports_data:
            st.subheader("📊 报表数据预览")
            report_names = list(reports_data.keys())[:3]
            for name in report_names:
                with st.expander(f"📋 {name}"):
                    df = reports_data[name]
                    st.write(f"数据规模: {len(df)} 行 × {len(df.columns)} 列")
                    st.dataframe(df.head(3), use_container_width=True)
                    
    except Exception as e:
        st.error(f"❌ 数据加载失败：{str(e)}")

elif user_type == "管理员" and not st.session_state.is_admin:
    st.info("👈 请在左侧边栏输入管理员密码")

else:
    if not st.session_state.logged_in:
        st.subheader("🔐 用户登录")
        
        try:
            with st.spinner("加载权限数据..."):
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
                            
        except Exception as e:
            st.error(f"❌ 权限验证失败：{str(e)}")
    
    else:
        # 已登录 - 显示报表
        st.markdown(f'<div class="store-info"><h3>🏪 {st.session_state.store_name}</h3><p>操作员：{st.session_state.user_id}</p></div>', unsafe_allow_html=True)
        
        try:
            with st.spinner("加载报表数据..."):
                reports_data = load_reports_from_sheets(gc)
                matching_sheets = find_matching_reports(st.session_state.store_name, reports_data)
            
            if matching_sheets:
                if len(matching_sheets) > 1:
                    selected_sheet = st.selectbox("选择报表", matching_sheets)
                else:
                    selected_sheet = matching_sheets[0]
                
                df = reports_data[selected_sheet]
                
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
                                        <h3 style="margin: 0.5rem 0;">门店应付款</h3>
                                        <p style="margin: 0; font-size: 0.9rem;">数据来源: {data['row_name']} (第{data['actual_row_number']}行)</p>
                                    </div>
                                ''', unsafe_allow_html=True)
                            
                            elif amount < 0:
                                st.markdown(f'''
                                    <div class="receivable-negative">
                                        <h1 style="margin: 0; font-size: 3rem;">💚 ¥{abs(amount):,.2f}</h1>
                                        <h3 style="margin: 0.5rem 0;">总部应退款</h3>
                                        <p style="margin: 0; font-size: 0.9rem;">数据来源: {data['row_name']} (第{data['actual_row_number']}行)</p>
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
                        
                        with st.expander("🔍 查看详情", expanded=False):
                            debug_info = analysis_results.get('debug_info', {})
                            
                            st.markdown("### 📋 数据查找说明")
                            st.write(f"- **报表总行数：** {debug_info.get('total_rows', 0)} 行")
                            
                            if debug_info.get('checked_row_69'):
                                st.write(f"- **第69行内容：** {debug_info.get('row_69_content', 'N/A')}")
                            else:
                                st.write("- **第69行：** 报表行数不足69行")
                
                except Exception as e:
                    st.error(f"❌ 分析数据时出错：{str(e)}")
                
                st.divider()
                
                # 完整报表数据
                st.subheader("📋 完整报表数据")
                
                search_term = st.text_input("🔍 搜索报表内容")
                
                try:
                    if search_term:
                        search_df = df.copy()
                        for col in search_df.columns:
                            search_df[col] = search_df[col].astype(str).fillna('')
                        
                        mask = search_df.apply(
                            lambda x: x.str.contains(search_term, case=False, na=False, regex=False)
                        ).any(axis=1)
                        filtered_df = df[mask]
                        st.info(f"找到 {len(filtered_df)} 条包含 '{search_term}' 的记录")
                    else:
                        filtered_df = df
                    
                    st.info(f"📊 数据统计：共 {len(filtered_df)} 条记录，{len(df.columns)} 列")
                    
                    if len(filtered_df) > 0:
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
                        csv_df = df.copy()
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
                
        except Exception as e:
            st.error(f"❌ 报表加载失败：{str(e)}")

# 页面底部状态信息
st.divider()
col1, col2, col3 = st.columns(3)
with col1:
    st.caption(f"🕒 当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
with col2:
    cache_count = len([key for key in st.session_state.keys() if key.startswith('cache_')])
    st.caption(f"💾 缓存项目: {cache_count}")
with col3:
    st.caption("🔧 版本: v4.0 (OAuth个人账号版)")
