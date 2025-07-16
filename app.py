import streamlit as st
import pandas as pd
import io
import json
from datetime import datetime
import time
import gspread
from google.oauth2.service_account import Credentials
import logging
from typing import Optional, Dict, Any, List
import hashlib
import pickle
import traceback
from contextlib import contextmanager

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
ADMIN_PASSWORD = "admin123"
PERMISSIONS_SHEET_NAME = "store_permissions"
REPORTS_SHEET_NAME = "store_reports"
SYSTEM_INFO_SHEET_NAME = "system_info" # 目前代码中未用到，但保留
MAX_RETRIES = 3
RETRY_DELAY = 1
MAX_CHUNK_SIZE = 30000  # 减小分片大小
CACHE_DURATION = 300  # 缓存5分钟

# *** 新增配置：你个人 Google Drive 中目标 Google Sheets 表格的完整 URL ***
# 请确保这是你已经创建并共享给服务账户（编辑者权限）的 Google Sheets 表格的完整 URL。
TARGET_SPREADSHEET_URL = 'https://docs.google.com/spreadsheets/d/1Ly2QCB3zAhQ7o_8h2Aj-lbSLL8YdPI2UZyNSxyWDp_Y/edit?gid=0#gid=0' # <--- 现在直接使用你提供的URL，没有多余检查了！

# Google Drive API 权限范围，允许读写 Drive 文件
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]


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
    .status-success {
        background: #d4edda;
        color: #155724;
        padding: 0.75rem;
        border-radius: 5px;
        border: 1px solid #c3e6cb;
        margin: 0.5rem 0;
    }
    .status-error {
        background: #f8d7da;
        color: #721c24;
        padding: 0.75rem;
        border-radius: 5px;
        border: 1px solid #f5c6cb;
        margin: 0.5rem 0;
    }
    .status-warning {
        background: #fff3cd;
        color: #856404;
        padding: 0.75rem;
        border-radius: 5px;
        border: 1px solid #ffeaa7;
        margin: 0.5rem 0;
    }
    </style>
""", unsafe_allow_html=True)

class SheetOperationError(Exception):
    """Google Sheets操作异常"""
    pass

class DataProcessingError(Exception):
    """数据处理异常"""
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

def retry_operation(func, *args, max_retries=MAX_RETRIES, delay=RETRY_DELAY, **kwargs):
    """重试操作装饰器"""
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if attempt == max_retries - 1:
                logger.error(f"操作失败，已重试 {max_retries} 次: {str(e)}")
                raise
            logger.warning(f"操作失败，第 {attempt + 1} 次重试: {str(e)}")
            time.sleep(delay * (attempt + 1))  # 递增延迟

def get_cache_key(operation: str, params: str) -> str:
    """生成缓存键"""
    return hashlib.md5(f"{operation}_{params}".encode()).hexdigest()

def set_cache(key: str, data: Any, duration: int = CACHE_DURATION):
    """设置缓存"""
    try:
        cache_data = {
            'data': data,
            'timestamp': time.time(),
            'duration': duration
        }
        st.session_state[f"cache_{key}"] = cache_data
    except Exception as e:
        logger.warning(f"设置缓存失败: {str(e)}")

def get_cache(key: str) -> Optional[Any]:
    """获取缓存"""
    try:
        cache_key = f"cache_{key}"
        if cache_key in st.session_state:
            cache_data = st.session_state[cache_key]
            if time.time() - cache_data['timestamp'] < cache_data['duration']:
                return cache_data['data']
            else:
                del st.session_state[cache_key]
    except Exception as e:
        logger.warning(f"获取缓存失败: {str(e)}")
    return None

@st.cache_resource(show_spinner="连接云数据库...")
def get_google_sheets_client():
    """获取Google Sheets客户端 - 使用缓存"""
    try:
        # 确保 `google_sheets` secret 存在且有效
        if "google_sheets" not in st.secrets:
            raise ValueError("`st.secrets['google_sheets']` 未配置。请在 .streamlit/secrets.toml 中添加服务账户密钥。")

        credentials_info = st.secrets["google_sheets"]
        # SCOPES 定义在文件顶部
        credentials = Credentials.from_service_account_info(credentials_info, scopes=SCOPES)
        client = gspread.authorize(credentials)
        logger.info("Google Sheets客户端创建成功")
        return client
    except Exception as e:
        logger.error(f"Google Sheets客户端创建失败: {str(e)}")
        raise SheetOperationError(f"连接失败: {str(e)}")

def safe_sheet_operation(operation_func, *args, **kwargs):
    """安全的表格操作"""
    return retry_operation(operation_func, *args, **kwargs)

# --- 修改此函数以使用 TARGET_SPREADSHEET_URL ---
def get_target_spreadsheet(gc):
    """
    通过URL获取指定的Google Sheets表格。
    此函数替换了原先的 get_or_create_spreadsheet，确保操作的是指定的表格。
    """
    def _operation():
        try:
            # 使用 open_by_url 尝试打开表格
            spreadsheet = gc.open_by_url(TARGET_SPREADSHEET_URL)
            logger.info(f"表格 (URL: {TARGET_SPREADSHEET_URL}) 已成功打开。")
            return spreadsheet
        except gspread.exceptions.SpreadsheetNotFound:
            raise SheetOperationError(f"表格 (URL: {TARGET_SPREADSHEET_URL}) 未找到。请确认URL是否正确，以及服务账户是否有访问权限。")
        except Exception as e:
            raise SheetOperationError(f"打开表格 (URL: {TARGET_SPREADSHEET_URL}) 失败: {str(e)}")

    return safe_sheet_operation(_operation)


def get_or_create_worksheet(spreadsheet, name, rows=1000, cols=20):
    """获取或创建工作表 - 增强错误处理"""
    def _operation():
        try:
            worksheet = spreadsheet.worksheet(name)
            logger.info(f"工作表 '{name}' 已存在")
            return worksheet
        except gspread.WorksheetNotFound:
            logger.info(f"创建新工作表 '{name}'")
            worksheet = spreadsheet.add_worksheet(title=name, rows=rows, cols=cols)
            return worksheet

    return safe_sheet_operation(_operation)

def clean_dataframe_for_json(df: pd.DataFrame) -> pd.DataFrame:
    """清理DataFrame以便JSON序列化"""
    try:
        df_cleaned = df.copy()

        # 处理各种数据类型
        for col in df_cleaned.columns:
            # 转换为字符串并处理特殊值
            df_cleaned[col] = df_cleaned[col].astype(str)
            df_cleaned[col] = df_cleaned[col].replace({
                'nan': '',
                'None': '',
                'NaT': '',
                'null': '',
                '<NA>': ''
            })

            # 处理过长的字符串（防止单个单元格过大）
            df_cleaned[col] = df_cleaned[col].apply(
                lambda x: x[:1000] + '...' if len(str(x)) > 1000 else x
            )

        logger.info(f"DataFrame清理完成: {len(df_cleaned)} 行 x {len(df_cleaned.columns)} 列")
        return df_cleaned

    except Exception as e:
        logger.error(f"清理DataFrame失败: {str(e)}")
        raise DataProcessingError(f"数据清理失败: {str(e)}")

def save_large_data_to_sheets(data_dict: Dict[str, Any], worksheet, batch_size: int = 15) -> bool:
    """分批保存大数据到表格"""
    try:
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        all_data = [['门店名称', '报表数据JSON', '行数', '列数', '更新时间', '分片序号', '总分片数', '数据哈希']]

        for store_name, df in data_dict.items():
            try:
                # 清理数据
                df_cleaned = clean_dataframe_for_json(df)

                # 转换为JSON
                json_data = df_cleaned.to_json(orient='records', force_ascii=False, ensure_ascii=False)

                # 计算数据哈希用于验证
                data_hash = hashlib.md5(json_data.encode('utf-8')).hexdigest()[:16]

                # 检查是否需要分片
                if len(json_data) <= MAX_CHUNK_SIZE:
                    # 不需要分片
                    all_data.append([
                        store_name,
                        json_data,
                        len(df),
                        len(df.columns),
                        current_time,
                        "1",
                        "1",
                        data_hash
                    ])
                else:
                    # 分片存储
                    chunks = []
                    for i in range(0, len(json_data), MAX_CHUNK_SIZE):
                        chunks.append(json_data[i:i + MAX_CHUNK_SIZE])

                    total_chunks = len(chunks)

                    for idx, chunk in enumerate(chunks):
                        chunk_name = f"{store_name}_分片{idx+1}"
                        all_data.append([
                            chunk_name,
                            chunk,
                            len(df),
                            len(df.columns),
                            current_time,
                            str(idx+1),
                            str(total_chunks),
                            data_hash
                        ])

                logger.info(f"准备保存 {store_name}: {len(df)} 行数据")

            except Exception as e:
                logger.error(f"处理 {store_name} 时出错: {str(e)}")
                # 保存错误信息
                error_data = {
                    "error": str(e),
                    "rows": len(df) if 'df' in locals() else 0,
                    "columns": len(df.columns) if 'df' in locals() else 0,
                    "timestamp": current_time
                }
                all_data.append([
                    f"{store_name}_错误",
                    json.dumps(error_data, ensure_ascii=False),
                    0,
                    0,
                    current_time,
                    "1",
                    "1",
                    "ERROR"
                ])
                continue

        # 分批上传数据
        if len(all_data) > 1:
            # First, update the header row and the first batch of data
            # This ensures the header is always correct
            first_batch_data = all_data[1:batch_size+1]
            worksheet.update('A1', [all_data[0]] + first_batch_data)
            time.sleep(0.8)

            # Update remaining batches
            for i in range(batch_size + 1, len(all_data), batch_size):
                batch_data = all_data[i:i+batch_size]
                row_num = i + 1
                worksheet.update(f'A{row_num}', batch_data)

                # API限制延迟
                time.sleep(0.8)

                # 显示进度
                progress = min(i + batch_size, len(all_data) - 1)
                st.progress(progress / (len(all_data) - 1))

        logger.info(f"数据保存完成: {len(all_data) - 1} 条记录")
        return True

    except Exception as e:
        logger.error(f"保存大数据失败: {str(e)}")
        raise

def save_reports_to_sheets(reports_dict: Dict[str, pd.DataFrame], gc) -> bool:
    """保存报表数据 - 增强版"""
    with error_handler("保存报表数据"):
        def _save_operation():
            # 获取指定的表格
            spreadsheet = get_target_spreadsheet(gc) # <--- 修改点：使用新函数
            worksheet = get_or_create_worksheet(spreadsheet, REPORTS_SHEET_NAME, rows=2000, cols=10)

            # 清空现有数据
            with st.spinner("清理旧数据..."):
                worksheet.clear()
                time.sleep(1)

            # 保存数据
            with st.spinner("保存新数据..."):
                success = save_large_data_to_sheets(reports_dict, worksheet)

            if success:
                # 清除相关缓存
                cache_key = get_cache_key("reports", "load")
                if f"cache_{cache_key}" in st.session_state:
                    del st.session_state[f"cache_{cache_key}"]

                logger.info("报表数据保存成功")
                return True
            return False

        return safe_sheet_operation(_save_operation)

def reconstruct_fragmented_data(fragments: List[Dict[str, Any]], store_name: str) -> Optional[pd.DataFrame]:
    """重构分片数据"""
    try:
        if len(fragments) == 1:
            # 单片数据
            json_data = fragments[0]['json_data']
        else:
            # 多片数据需要重构
            fragments.sort(key=lambda x: int(x['chunk_num']))
            json_data = ''.join([frag['json_data'] for frag in fragments])

        # 验证数据完整性
        expected_hash = fragments[0].get('data_hash', '')
        if expected_hash and expected_hash != 'ERROR':
            actual_hash = hashlib.md5(json_data.encode('utf-8')).hexdigest()[:16]
            if actual_hash != expected_hash:
                logger.warning(f"{store_name} 数据哈希不匹配，可能存在数据损坏")

        # 解析JSON
        df = pd.read_json(io.StringIO(json_data), orient='records') # 使用io.StringIO处理字符串JSON

        # 数据后处理
        if len(df) > 0:
            # 检查第一行是否是门店名称 (为了处理某些Excel导出可能带的额外行)
            # 这里的逻辑是假设excel文件的第一行可能是门店名称，且只有1-2个非空单元格
            original_df_rows_count = len(df)
            first_row = df.iloc[0]
            non_empty_count = sum(1 for val in first_row if pd.notna(val) and str(val).strip() != '')

            if non_empty_count <= 2 and original_df_rows_count > 1: # 只有很少的非空值，可能是标题行
                df = df.iloc[1:].reset_index(drop=True) # 跳过第一行

        # 处理表头
        if len(df) > 0: # 确保DataFrame有数据再处理
            # 检查是否有有效列名行
            potential_header = df.iloc[0].fillna('').astype(str).tolist()
            # 如果第一行有足够的非空值，我们认为它是表头
            if sum(1 for x in potential_header if x.strip() != '') >= 2: # 至少有两个非空列名
                header_row = potential_header
                data_rows = df.iloc[1:].copy()
            else: # 否则使用默认表头或者认为整个df就是数据
                header_row = [f'列{i+1}' for i in range(len(df.columns))]
                data_rows = df.copy()

            # 清理列名并处理重复
            cols = []
            for i, col in enumerate(header_row):
                col = str(col).strip()
                if col == '' or col == 'nan' or col == '0':
                    col = f'列{i+1}' if i < len(data_rows.columns) else f'额外列{i+1}' # 避免越界

                # 处理重复列名
                original_col = col
                counter = 1
                while col in cols:
                    col = f"{original_col}_{counter}"
                    counter += 1
                cols.append(col)

            # 确保列数匹配，避免设置的列名数量与实际数据列数不符
            if len(cols) != len(data_rows.columns):
                min_cols = min(len(data_rows.columns), len(cols))
                cols = cols[:min_cols]
                data_rows = data_rows.iloc[:, :min_cols] # 截断数据列以匹配列名

            data_rows.columns = cols
            df = data_rows.reset_index(drop=True).fillna('')
        else: # 如果df是空的
            df = pd.DataFrame() # 返回空DataFrame

        logger.info(f"{store_name} 数据重构成功: {len(df)} 行")
        return df

    except Exception as e:
        logger.error(f"重构 {store_name} 数据失败: {str(e)}")
        return None

def load_reports_from_sheets(gc) -> Dict[str, pd.DataFrame]:
    """加载报表数据 - 使用缓存和分片重构"""
    cache_key = get_cache_key("reports", "load")
    cached_data = get_cache(cache_key)
    if cached_data is not None:
        logger.info("从缓存加载报表数据")
        return cached_data

    with error_handler("加载报表数据"):
        def _load_operation():
            # 获取指定的表格
            spreadsheet = get_target_spreadsheet(gc) # <--- 修改点：使用新函数

            try:
                worksheet = spreadsheet.worksheet(REPORTS_SHEET_NAME)
                data = worksheet.get_all_values()

                if len(data) <= 1:
                    logger.info("报表数据为空")
                    return {}

                # 解析数据
                reports_dict = {}
                fragments_dict = {}  # 存储分片数据

                # 检查并跳过空行，避免索引错误
                for row_idx, row in enumerate(data[1:]):
                    if not any(cell.strip() for cell in row): # 如果整行都是空的，跳过
                        logger.debug(f"跳过空行 {row_idx + 2}") # +2 是因为跳过了标题行和0索引
                        continue

                    # 确保有足够的列
                    if len(row) < 7: # 至少需要7列来解析基本的分片信息
                        logger.warning(f"跳过不完整行 (行 {row_idx + 2}): {row}")
                        continue

                    store_name = row[0]
                    json_data = row[1]
                    # rows_count = row[2] # 暂时不使用，因为重构后会重新计算
                    # cols_count = row[3] # 暂时不使用
                    # update_time = row[4] # 暂时不使用
                    chunk_num = row[5]
                    total_chunks = row[6]
                    data_hash = row[7] if len(row) > 7 else '' # 确保data_hash存在

                    # 跳过错误数据 (错误标记通常在 store_name 后面)
                    if store_name.endswith('_错误'):
                        logger.warning(f"跳过错误数据: {store_name}")
                        continue

                    # 处理分片数据
                    if '_分片' in store_name:
                        base_name = store_name.split('_分片')[0]
                        if base_name not in fragments_dict:
                            fragments_dict[base_name] = []

                        fragments_dict[base_name].append({
                            'json_data': json_data,
                            'chunk_num': chunk_num,
                            'total_chunks': total_chunks,
                            'data_hash': data_hash
                        })
                    else:
                        # 单片数据
                        fragments_dict[store_name] = [{
                            'json_data': json_data,
                            'chunk_num': '1',
                            'total_chunks': '1',
                            'data_hash': data_hash
                        }]

                # 重构所有分片数据
                for store_name, fragments in fragments_dict.items():
                    df = reconstruct_fragmented_data(fragments, store_name)
                    if df is not None and not df.empty: # 确保重构后不是空DataFrame
                        reports_dict[store_name] = df
                    elif df is not None and df.empty:
                        logger.info(f"门店 '{store_name}' 重构后为空DataFrame，跳过。")
                    else:
                        logger.error(f"门店 '{store_name}' 重构失败，跳过。")


                logger.info(f"报表数据加载成功: {len(reports_dict)} 个门店")

                # 设置缓存
                set_cache(cache_key, reports_dict)
                return reports_dict

            except gspread.WorksheetNotFound:
                logger.info("报表数据表不存在")
                return {}

        return safe_sheet_operation(_load_operation)

def analyze_receivable_data(df: pd.DataFrame) -> Dict[str, Any]:
    """分析应收未收额数据 - 专门查找第69行"""
    result = {}

    if len(df.columns) == 0 or len(df) == 0:
        return result

    # 检查第一行是否是门店名称
    # original_df = df.copy() # 不再需要，直接操作df
    first_row_skipped = False
    first_row = df.iloc[0] if len(df) > 0 else None
    if first_row is not None:
        non_empty_count = sum(1 for val in first_row if pd.notna(val) and str(val).strip() != '')
        # 如果第一行只有1-2个非空值，且行数大于1，则跳过第一行
        if non_empty_count <= 2 and len(df) > 1:
            df = df.iloc[1:].reset_index(drop=True)
            first_row_skipped = True
            result['skipped_store_name_row'] = True


    # 查找第69行 (0-indexed 对应 68)
    # 如果原始excel有标题行且被跳过，那么原来的第69行现在是第68行
    target_row_index_in_processed_df = 68 - (1 if first_row_skipped else 0)

    if len(df) > target_row_index_in_processed_df and target_row_index_in_processed_df >= 0: # 确保索引有效
        row = df.iloc[target_row_index_in_processed_df]
        first_col_value = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ""

        # 检查关键词
        keywords = ['应收-未收额', '应收未收额', '应收-未收', '应收未收']

        for keyword in keywords:
            if keyword in first_col_value:
                # 查找数值 (从右向左查找第一个非空数值)
                for col_idx in range(len(row)-1, 0, -1):
                    val = row.iloc[col_idx]
                    if pd.notna(val) and str(val).strip() not in ['', 'None', 'nan']:
                        cleaned = str(val).replace(',', '').replace('¥', '').replace('￥', '').strip()

                        if cleaned.startswith('(') and cleaned.endswith(')'):
                            cleaned = '-' + cleaned[1:-1]

                        try:
                            amount = float(cleaned)
                            if amount != 0: # 找到非零值才返回
                                result['应收-未收额'] = {
                                    'amount': amount,
                                    'column_name': str(df.columns[col_idx]),
                                    'row_name': first_col_value,
                                    'row_index': target_row_index_in_processed_df,
                                    'actual_row_number': target_row_index_in_processed_df + 1 + (1 if first_row_skipped else 0), # 报告原始excel行号
                                }
                                return result
                        except ValueError:
                            continue # 不是有效数字，继续找
                break # 找到关键词但没找到有效数字，跳出关键词循环

    # 备用查找 (如果在目标行没找到，则全表查找)
    if '应收-未收额' not in result:
        keywords = ['应收-未收额', '应收未收额', '应收-未收', '应收未收']

        for idx, row in df.iterrows():
            try:
                row_name = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ""

                if not row_name.strip(): # 跳过空行名称
                    continue

                for keyword in keywords:
                    if keyword in row_name:
                        for col_idx in range(len(row)-1, 0, -1):
                            val = row.iloc[col_idx]
                            if pd.notna(val) and str(val).strip() not in ['', 'None', 'nan']:
                                cleaned = str(val).replace(',', '').replace('¥', '').replace('￥', '').replace('￥', '').strip()

                                if cleaned.startswith('(') and cleaned.endswith(')'):
                                    cleaned = '-' + cleaned[1:-1]

                                try:
                                    amount = float(cleaned)
                                    if amount != 0: # 找到非零值才返回
                                        result['应收-未收额'] = {
                                            'amount': amount,
                                            'column_name': str(df.columns[col_idx]),
                                            'row_name': row_name,
                                            'row_index': idx,
                                            'actual_row_number': idx + 1 + (1 if first_row_skipped else 0), # 报告原始excel行号
                                            'note': f'在原始Excel第{idx + 1 + (1 if first_row_skipped else 0)}行找到（非预设第69行）'
                                        }
                                        return result
                                except ValueError:
                                    continue
                        break # 找到关键词但没找到有效数字，跳出关键词循环
            except Exception: # 避免单个行处理错误导致整个分析失败
                continue

    # 调试信息
    result['debug_info'] = {
        'total_rows': len(df),
        'first_row_skipped': first_row_skipped,
        'checked_row_69_in_processed_df': len(df) > target_row_index_in_processed_df and target_row_index_in_processed_df >= 0,
        'row_69_content_in_processed_df': str(df.iloc[target_row_index_in_processed_df].iloc[0]) if len(df) > target_row_index_in_processed_df and target_row_index_in_processed_df >= 0 else 'N/A'
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

        # 增强匹配逻辑，支持部分匹配和忽略大小写（如果需要）
        if (store_name.lower() in stored_store.lower() or stored_store.lower() in store_name.lower()) and stored_id == str(user_id).strip():
            return True

    return False

def find_matching_reports(store_name: str, reports_data: Dict[str, pd.DataFrame]) -> List[str]:
    """查找匹配的报表"""
    matching = []
    # 转换为小写进行不敏感匹配
    store_name_lower = store_name.lower()
    for sheet_name in reports_data.keys():
        sheet_name_lower = sheet_name.lower()
        if store_name_lower in sheet_name_lower or sheet_name_lower in store_name_lower:
            matching.append(sheet_name)
    return matching

def show_status_message(message: str, status_type: str = "info"):
    """显示状态消息"""
    css_class = f"status-{status_type}"
    st.markdown(f'<div class="{css_class}">{message}</div>', unsafe_allow_html=True)

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
if 'operation_status' not in st.session_state:
    st.session_state.operation_status = []

# 主标题
st.markdown('<h1 class="main-header">📊 门店报表查询系统</h1>', unsafe_allow_html=True)

# 初始化Google Sheets客户端
if not st.session_state.google_sheets_client:
    try:
        with st.spinner("连接云数据库..."):
            gc = get_google_sheets_client()
            st.session_state.google_sheets_client = gc
            show_status_message("✅ 云数据库连接成功！", "success")
    except Exception as e:
        show_status_message(f"❌ 连接失败: {str(e)}", "error")
        st.stop()

gc = st.session_state.google_sheets_client

# 显示操作状态
for status in st.session_state.operation_status:
    show_status_message(status['message'], status['type'])
st.session_state.operation_status = [] # 清除已显示的状态消息

# 侧边栏
with st.sidebar:
    st.title("⚙️ 系统功能")

    # 系统状态
    st.subheader("📡 系统状态")
    if gc:
        st.success("🟢 云数据库已连接")
    else:
        st.error("🔴 云数据库断开")

    user_type = st.radio("选择用户类型", ["普通用户", "管理员"])

    if user_type == "管理员":
        st.subheader("🔐 管理员登录")
        admin_password = st.text_input("管理员密码", type="password")

        if st.button("验证管理员身份"):
            if admin_password == ADMIN_PASSWORD:
                st.session_state.is_admin = True
                show_status_message("✅ 管理员验证成功！", "success")
                st.rerun()
            else:
                show_status_message("❌ 密码错误！", "error")

        if st.session_state.is_admin:
            st.subheader("📁 文件管理")

            # 上传权限表
            permissions_file = st.file_uploader("上传门店权限表", type=['xlsx', 'xls'], key="permissions_uploader")
            if permissions_file:
                try:
                    with st.spinner("处理权限表文件..."):
                        df = pd.read_excel(permissions_file)
                        if len(df.columns) >= 2:
                            with st.spinner("保存到云端..."):
                                if save_permissions_to_sheets(df, gc):
                                    show_status_message(f"✅ 权限表已上传：{len(df)} 个用户", "success")
                                    st.balloons()
                                else:
                                st.session_state.operation_status.append({"message": "❌ 权限表保存失败", "type": "error"})
                        else:
                            st.session_state.operation_status.append({"message": "❌ 格式错误：需要至少两列（门店名称、人员编号）", "type": "error"})
                except Exception as e:
                    st.session_state.operation_status.append({"message": f"❌ 处理权限表失败：{str(e)}", "type": "error"})
                st.rerun() # 上传后刷新页面显示状态

            # 上传财务报表
            reports_file = st.file_uploader("上传财务报表", type=['xlsx', 'xls'], key="reports_uploader")
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
                            with st.spinner("保存到云端..."):
                                if save_reports_to_sheets(reports_dict, gc):
                                    show_status_message(f"✅ 报表已上传：{len(reports_dict)} 个门店", "success")
                                    st.balloons()
                                else:
                                    st.session_state.operation_status.append({"message": "❌ 报表保存失败", "type": "error"})
                        else:
                            st.session_state.operation_status.append({"message": "❌ 文件中没有有效的工作表", "type": "error"})

                except Exception as e:
                    st.session_state.operation_status.append({"message": f"❌ 处理报表失败：{str(e)}", "type": "error"})
                st.rerun() # 上传后刷新页面显示状态

            # 缓存管理
            st.subheader("🗂️ 缓存管理")
            if st.button("清除所有缓存", key="clear_cache_button"):
                cache_keys = [key for key in st.session_state.keys() if key.startswith('cache_')]
                for key in cache_keys:
                    del st.session_state[key]
                st.session_state.operation_status.append({"message": "✅ 缓存已清除", "type": "success"})
                st.rerun()
    else:
        if st.session_state.logged_in:
            st.subheader("👤 当前登录")
            st.info(f"门店：{st.session_state.store_name}")
            st.info(f"编号：{st.session_state.user_id}")

            if st.button("🚪 退出登录", key="logout_button"):
                st.session_state.logged_in = False
                st.session_state.store_name = ""
                st.session_state.user_id = ""
                st.session_state.is_admin = False # 退出登录也重置管理员状态
                st.session_state.operation_status.append({"message": "👋 已退出登录", "type": "success"})
                st.rerun()

# 主界面
if user_type == "管理员" and st.session_state.is_admin:
    st.markdown('<div class="admin-panel"><h3>👨‍💼 管理员控制面板</h3><p>数据永久保存在云端，支持分片存储和缓存机制</p></div>', unsafe_allow_html=True)

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
            report_names = list(reports_data.keys())[:5]  # 显示前5个
            for name in report_names:
                with st.expander(f"📋 {name}"):
                    df = reports_data[name]
                    st.write(f"数据规模: {len(df)} 行 × {len(df.columns)} 列")
                    st.dataframe(df.head(3), use_container_width=True)

    except Exception as e:
        show_status_message(f"❌ 数据加载失败：{str(e)}", "error")

elif user_type == "管理员" and not st.session_state.is_admin:
    st.info("👈 请在左侧边栏输入管理员密码")

else: # 普通用户界面
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
                            st.session_state.operation_status.append({"message": "✅ 登录成功！", "type": "success"})
                            st.balloons()
                            st.rerun()
                        else:
                            st.session_state.operation_status.append({"message": "❌ 门店或编号错误！", "type": "error"})
                            st.rerun() # 登录失败也刷新，显示错误

        except Exception as e:
            show_status_message(f"❌ 权限验证失败：{str(e)}", "error")

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
                                        <p style="margin: 0; font-size: 0.9rem;">数据来源: {data['row_name']} (原始Excel第{data['actual_row_number']}行)</p>
                                    </div>
                                ''', unsafe_allow_html=True)

                            elif amount < 0:
                                st.markdown(f'''
                                    <div class="receivable-negative">
                                        <h1 style="margin: 0; font-size: 3rem;">💚 ¥{abs(amount):,.2f}</h1>
                                        <h3 style="margin: 0.5rem 0;">总部应退款</h3>
                                        <p style="margin: 0; font-size: 0.9rem;">数据来源: {data['row_name']} (原始Excel第{data['actual_row_number']}行)</p>
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
                            st.write(f"- **报表总行数（处理后）：** {debug_info.get('total_rows', 0)} 行")
                            st.write(f"- **是否跳过第一行门店名：** {debug_info.get('first_row_skipped', False)}")

                            if debug_info.get('checked_row_69_in_processed_df'):
                                st.write(f"- **处理后第69行（原始第{debug_info.get('actual_row_number_69', 'N/A')}行）内容：** '{debug_info.get('row_69_content_in_processed_df', 'N/A')}'")
                            else:
                                st.write("- **预设应收未收额行（原始第69行）：** 报表行数不足或处理后行号无效")

                            st.markdown("""
                            ### 💡 可能的原因
                            1. 报表中预设的第69行（或其处理后的对应行）不包含"应收-未收额"相关关键词。
                            2. 预设行中的数值为空或格式不正确（如非数字字符）。
                            3. 报表整体格式与系统预期不符，导致关键行被跳过或定位错误。

                            ### 🛠️ 建议
                            - 请确保原始Excel报表中的"应收-未收额"数据位于**第69行**。
                            - 确认该行对应的金额数据是清晰的数字格式。
                            - 如需调整查找位置或报表解析逻辑，请联系技术支持。
                            """)

                except Exception as e:
                    show_status_message(f"❌ 分析数据时出错：{str(e)}", "error")

                st.divider()

                # 完整报表数据
                st.subheader("📋 完整报表数据")

                search_term = st.text_input("🔍 搜索报表内容", key="report_search_input")

                try:
                    if search_term:
                        search_df = df.copy()
                        for col in search_df.columns:
                            # 转换为字符串并处理可能的非字符串类型，然后进行搜索
                            search_df[col] = search_df[col].astype(str).fillna('')

                        mask = search_df.apply(
                            lambda x: x.str.contains(search_term, case=False, na=False, regex=False)
                        ).any(axis=1)
                        filtered_df = df[mask] # 使用原始df过滤，避免显示类型转换后的列
                        st.info(f"找到 {len(filtered_df)} 条包含 '{search_term}' 的记录")
                    else:
                        filtered_df = df

                    st.info(f"📊 数据统计：共 {len(filtered_df)} 条记录，{len(df.columns)} 列")

                    if not filtered_df.empty: # 使用 .empty 判断是否为空
                        display_df = filtered_df.copy()

                        # 确保列名唯一（这部分逻辑应在 reconstruct_fragmented_data 中更完整处理）
                        # 但为保险起见，这里再做一次检查
                        unique_columns = []
                        for i, col in enumerate(display_df.columns):
                            col_name = str(col)
                            if col_name in unique_columns:
                                col_name = f"{col_name}_{i}"
                            unique_columns.append(col_name)
                        display_df.columns = unique_columns

                        # 清理数据内容，确保显示时都是字符串
                        for col in display_df.columns:
                            display_df[col] = display_df[col].astype(str).fillna('')

                        st.dataframe(display_df, use_container_width=True, height=400)

                    else:
                        st.warning("没有找到符合条件的数据")

                except Exception as e:
                    show_status_message(f"❌ 数据处理时出错：{str(e)}", "error")

                # 下载功能
                st.subheader("📥 数据下载")

                col1, col2 = st.columns(2)
                with col1:
                    try:
                        buffer = io.BytesIO()
                        download_df = df.copy()

                        # 确保列名唯一，这里再次处理以防万一
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
                            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key="download_excel_button"
                        )
                    except Exception as e:
                        show_status_message(f"Excel下载准备失败：{str(e)}", "error")

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

                        # ensure_ascii=False for Chinese characters
                        csv = csv_df.to_csv(index=False, encoding='utf-8-sig')
                        st.download_button(
                            "📥 下载CSV格式",
                            csv,
                            f"{st.session_state.store_name}_报表_{datetime.now().strftime('%Y%m%d')}.csv",
                            "text/csv",
                            key="download_csv_button"
                        )
                    except Exception as e:
                        show_status_message(f"CSV下载准备失败：{str(e)}", "error")

            else:
                st.error(f"❌ 未找到门店 '{st.session_state.store_name}' 的报表")

        except Exception as e:
            show_status_message(f"❌ 报表加载失败：{str(e)}", "error")

# 页面底部状态信息
st.divider()
col1, col2, col3 = st.columns(3)
with col1:
    st.caption(f"🕒 当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
with col2:
    cache_count = len([key for key in st.session_state.keys() if key.startswith('cache_')])
    st.caption(f"💾 缓存项目: {cache_count}")
with col3:
    st.caption("🔧 版本: v2.0 (稳定版) - 存储在个人Drive")
