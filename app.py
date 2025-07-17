import streamlit as st
import pandas as pd
import io
import json
from datetime import datetime
import time
import logging
from typing import Optional, Dict, Any, List
import hashlib
import pickle
import traceback
from contextlib import contextmanager
from qcloud_cos import CosConfig, CosS3Client
from qcloud_cos.cos_exception import CosServiceError, CosClientError
import openpyxl

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
MAX_RETRIES = 3
RETRY_DELAY = 1
CACHE_DURATION = 300  # 缓存5分钟

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

class CosOperationError(Exception):
    """腾讯云COS操作异常"""
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
            time.sleep(delay * (attempt + 1))

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
        logger.info(f"缓存已设置: {key}")
    except Exception as e:
        logger.warning(f"设置缓存失败: {str(e)}")

def get_cache(key: str) -> Optional[Any]:
    """获取缓存"""
    try:
        cache_key = f"cache_{key}"
        if cache_key in st.session_state:
            cache_data = st.session_state[cache_key]
            if time.time() - cache_data['timestamp'] < cache_data['duration']:
                logger.info(f"缓存命中: {key}")
                return cache_data['data']
            else:
                del st.session_state[cache_key]
                logger.info(f"缓存过期: {key}")
    except Exception as e:
        logger.warning(f"获取缓存失败: {str(e)}")
    return None

def validate_cos_config(config: dict) -> bool:
    """验证腾讯云COS配置"""
    required_keys = ["region", "secret_id", "secret_key", "bucket_name", "permissions_file"]
    for key in required_keys:
        if key not in config or not config[key]:
            logger.error(f"COS配置缺少必要参数: {key}")
            return False
    return True

@st.cache_resource(show_spinner="连接腾讯云存储...")
def get_cos_client():
    """获取腾讯云COS客户端 - 使用缓存"""
    try:
        if "tencent_cloud" not in st.secrets:
            raise CosOperationError("未找到腾讯云配置，请检查 secrets.toml 文件")
        
        cos_config = st.secrets["tencent_cloud"]
        
        # 验证配置
        if not validate_cos_config(cos_config):
            raise CosOperationError("腾讯云配置不完整")
        
        config = CosConfig(
            Region=cos_config["region"],
            SecretId=cos_config["secret_id"],
            SecretKey=cos_config["secret_key"],
            Scheme='https'  # 使用HTTPS协议
        )
        
        client = CosS3Client(config)
        
        # 测试连接
        try:
            client.head_bucket(Bucket=cos_config["bucket_name"])
            logger.info("腾讯云COS客户端创建成功，连接测试通过")
        except CosServiceError as e:
            logger.error(f"COS连接测试失败: {e.get_error_code()} - {e.get_error_msg()}")
            raise CosOperationError(f"存储桶连接失败: {e.get_error_msg()}")
        
        return client, cos_config["bucket_name"], cos_config["permissions_file"]
    
    except Exception as e:
        logger.error(f"腾讯云COS客户端创建失败: {str(e)}")
        raise CosOperationError(f"连接失败: {str(e)}")

def safe_cos_operation(operation_func, *args, **kwargs):
    """安全的COS操作"""
    return retry_operation(operation_func, *args, **kwargs)

def save_permissions_to_cos(df: pd.DataFrame, cos_client, bucket_name: str, permissions_file: str) -> bool:
    """保存权限数据到COS - 增强版，包含详细处理日志"""
    with error_handler("保存权限数据"):
        def _save_operation():
            # 数据验证
            if df is None or len(df) == 0:
                raise DataProcessingError("权限数据为空")
            
            if len(df.columns) < 2:
                raise DataProcessingError("权限数据格式错误：需要至少两列（门店名称、人员编号）")
            
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # 创建处理报告
            processing_report = {
                'original_rows': len(df),
                'original_columns': len(df.columns),
                'processed_rows': 0,
                'skipped_rows': [],
                'error_rows': [],
                'duplicate_rows': [],
                'empty_rows': []
            }
            
            # 显示原始数据统计
            st.info(f"📊 原始数据：{len(df)} 行 × {len(df.columns)} 列")
            
            # 显示原始数据预览
            with st.expander("🔍 原始数据预览（前10行）", expanded=False):
                st.dataframe(df.head(10), use_container_width=True)
            
            # 准备CSV数据
            csv_data = []
            csv_data.append(['门店名称', '人员编号', '更新时间'])
            
            processed_count = 0
            seen_combinations = set()  # 用于检测重复数据
            
            # 创建进度条
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            for idx, row in df.iterrows():
                progress = (idx + 1) / len(df)
                progress_bar.progress(progress)
                status_text.text(f"处理第 {idx + 1}/{len(df)} 行...")
                
                try:
                    # 获取原始值
                    raw_store = row.iloc[0] if pd.notna(row.iloc[0]) else ""
                    raw_user = row.iloc[1] if pd.notna(row.iloc[1]) else ""
                    
                    # 转换为字符串并清理
                    store_name = str(raw_store).strip()
                    user_id = str(raw_user).strip()
                    
                    # 记录空行
                    if (not store_name or store_name == 'nan' or store_name == 'None') and \
                       (not user_id or user_id == 'nan' or user_id == 'None'):
                        processing_report['empty_rows'].append({
                            'row': idx + 1,
                            'store': raw_store,
                            'user': raw_user,
                            'reason': '门店和编号都为空'
                        })
                        continue
                    
                    # 检查门店名称
                    if not store_name or store_name == 'nan' or store_name == 'None':
                        processing_report['skipped_rows'].append({
                            'row': idx + 1,
                            'store': raw_store,
                            'user': raw_user,
                            'reason': '门店名称为空'
                        })
                        continue
                    
                    # 检查人员编号
                    if not user_id or user_id == 'nan' or user_id == 'None':
                        processing_report['skipped_rows'].append({
                            'row': idx + 1,
                            'store': raw_store,
                            'user': raw_user,
                            'reason': '人员编号为空'
                        })
                        continue
                    
                    # 清理特殊字符但保留更多有效数据
                    store_name = store_name.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
                    user_id = user_id.replace('\n', '').replace('\r', '').replace('\t', '')
                    
                    # 去除多余空格
                    store_name = ' '.join(store_name.split())
                    user_id = ' '.join(user_id.split())
                    
                    # 最终验证
                    if len(store_name) == 0 or len(user_id) == 0:
                        processing_report['skipped_rows'].append({
                            'row': idx + 1,
                            'store': raw_store,
                            'user': raw_user,
                            'reason': '清理后数据为空'
                        })
                        continue
                    
                    # 检查重复数据
                    combination = (store_name.lower(), user_id.lower())
                    if combination in seen_combinations:
                        processing_report['duplicate_rows'].append({
                            'row': idx + 1,
                            'store': store_name,
                            'user': user_id,
                            'reason': '重复的门店-编号组合'
                        })
                        continue
                    
                    seen_combinations.add(combination)
                    csv_data.append([store_name, user_id, current_time])
                    processed_count += 1
                    
                except Exception as e:
                    processing_report['error_rows'].append({
                        'row': idx + 1,
                        'store': raw_store if 'raw_store' in locals() else 'N/A',
                        'user': raw_user if 'raw_user' in locals() else 'N/A',
                        'reason': f'处理错误: {str(e)}'
                    })
                    logger.warning(f"处理第{idx+1}行时出错: {str(e)}")
                    continue
            
            progress_bar.empty()
            status_text.empty()
            
            # 更新处理报告
            processing_report['processed_rows'] = processed_count
            
            # 显示处理结果
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("原始数据", f"{processing_report['original_rows']} 行")
            with col2:
                st.metric("成功处理", f"{processed_count} 行", 
                         delta=f"{processed_count - processing_report['original_rows']}")
            with col3:
                skipped_total = len(processing_report['skipped_rows']) + \
                               len(processing_report['error_rows']) + \
                               len(processing_report['empty_rows']) + \
                               len(processing_report['duplicate_rows'])
                st.metric("跳过数据", f"{skipped_total} 行")
            
            # 显示详细处理报告
            with st.expander("📋 详细处理报告", expanded=True):
                
                # 成功处理的数据预览
                if processed_count > 0:
                    st.subheader("✅ 成功处理的数据")
                    processed_df = pd.DataFrame(csv_data[1:], columns=csv_data[0])
                    st.dataframe(processed_df.head(10), use_container_width=True)
                    if len(processed_df) > 10:
                        st.caption(f"显示前10条，共{len(processed_df)}条")
                
                # 空行报告
                if processing_report['empty_rows']:
                    st.subheader("⚪ 空行数据")
                    st.write(f"发现 {len(processing_report['empty_rows'])} 个完全空行")
                    if len(processing_report['empty_rows']) <= 5:
                        for item in processing_report['empty_rows']:
                            st.caption(f"第{item['row']}行: 门店='{item['store']}', 编号='{item['user']}'")
                    else:
                        st.caption(f"显示前5个: {[f'第{item['row']}行' for item in processing_report['empty_rows'][:5]]}")
                
                # 跳过的数据报告
                if processing_report['skipped_rows']:
                    st.subheader("⚠️ 跳过的数据")
                    skip_df = pd.DataFrame(processing_report['skipped_rows'])
                    st.dataframe(skip_df, use_container_width=True)
                
                # 重复数据报告
                if processing_report['duplicate_rows']:
                    st.subheader("🔄 重复数据")
                    dup_df = pd.DataFrame(processing_report['duplicate_rows'])
                    st.dataframe(dup_df, use_container_width=True)
                
                # 错误数据报告
                if processing_report['error_rows']:
                    st.subheader("❌ 处理错误的数据")
                    error_df = pd.DataFrame(processing_report['error_rows'])
                    st.dataframe(error_df, use_container_width=True)
            
            if processed_count == 0:
                raise DataProcessingError("没有有效的权限数据")
            
            # 转换为CSV格式
            csv_buffer = io.StringIO()
            permissions_df = pd.DataFrame(csv_data[1:], columns=csv_data[0])
            permissions_df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
            csv_content = csv_buffer.getvalue()
            
            # 上传到COS
            cos_client.put_object(
                Bucket=bucket_name,
                Body=csv_content.encode('utf-8-sig'),
                Key=permissions_file,
                ContentType='text/csv; charset=utf-8',
                Metadata={
                    'upload-time': current_time,
                    'record-count': str(processed_count),
                    'original-count': str(processing_report['original_rows']),
                    'skipped-count': str(len(processing_report['skipped_rows']) + 
                                          len(processing_report['error_rows']) + 
                                          len(processing_report['empty_rows']) + 
                                          len(processing_report['duplicate_rows']))
                }
            )
            
            logger.info(f"权限数据保存成功: {processed_count} 条记录 (原始: {processing_report['original_rows']} 条)")
            
            # 清除相关缓存
            clear_permissions_cache()
            
            return True
        
        return safe_cos_operation(_save_operation)

def clear_permissions_cache():
    """清除权限相关缓存"""
    cache_keys_to_clear = [
        get_cache_key("permissions", "load"),
        get_cache_key("store_list", "load")
    ]
    
    for cache_key in cache_keys_to_clear:
        full_key = f"cache_{cache_key}"
        if full_key in st.session_state:
            del st.session_state[full_key]
            logger.info(f"已清除缓存: {cache_key}")

def load_permissions_from_cos(cos_client, bucket_name: str, permissions_file: str) -> Optional[pd.DataFrame]:
    """从COS加载权限数据 - 优化过滤逻辑版本"""
    cache_key = get_cache_key("permissions", "load")
    cached_data = get_cache(cache_key)
    if cached_data is not None:
        return cached_data
    
    with error_handler("加载权限数据"):
        def _load_operation():
            try:
                # 从COS下载文件
                response = cos_client.get_object(
                    Bucket=bucket_name,
                    Key=permissions_file
                )
                
                # 读取CSV内容
                csv_content = response['Body'].read().decode('utf-8-sig')
                df = pd.read_csv(io.StringIO(csv_content))
                
                if len(df) == 0:
                    logger.info("权限表为空")
                    return None
                
                logger.info(f"从COS读取到原始数据: {len(df)} 行 × {len(df.columns)} 列")
                
                # 确保有必要的列
                if len(df.columns) < 2:
                    logger.error("权限表格式错误：列数不足")
                    return None
                
                # 创建过滤统计
                filter_stats = {
                    'original_count': len(df),
                    'empty_store': 0,
                    'empty_user': 0,
                    'both_empty': 0,
                    'final_count': 0
                }
                
                # 只取前两列作为权限数据
                result_df = df.iloc[:, :2].copy()
                result_df.columns = ['门店名称', '人员编号']
                
                logger.info(f"提取权限列后: {len(result_df)} 行")
                
                # 优化的数据清理 - 更宽松的条件
                def is_empty_value(val):
                    """判断值是否为空 - 更宽松的条件"""
                    if pd.isna(val):
                        return True
                    val_str = str(val).strip()
                    return val_str in ['', 'nan', 'None', 'NaN', 'null', 'NULL']
                
                def clean_value(val):
                    """清理数据值 - 保留更多有效数据"""
                    if pd.isna(val):
                        return ''
                    
                    val_str = str(val).strip()
                    
                    # 移除首尾空格和特殊字符
                    val_str = val_str.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
                    val_str = ' '.join(val_str.split())  # 合并多个空格
                    
                    # 如果清理后为这些值，则认为是空
                    if val_str.lower() in ['nan', 'none', 'null', '']:
                        return ''
                    
                    return val_str
                
                # 清理数据
                result_df['门店名称'] = result_df['门店名称'].apply(clean_value)
                result_df['人员编号'] = result_df['人员编号'].apply(clean_value)
                
                logger.info(f"数据清理后: {len(result_df)} 行")
                
                # 统计空值情况
                empty_store_mask = result_df['门店名称'] == ''
                empty_user_mask = result_df['人员编号'] == ''
                both_empty_mask = empty_store_mask & empty_user_mask
                
                filter_stats['empty_store'] = empty_store_mask.sum()
                filter_stats['empty_user'] = empty_user_mask.sum()
                filter_stats['both_empty'] = both_empty_mask.sum()
                
                # 只过滤两个字段都为空的行（更宽松的过滤）
                result_df = result_df[~both_empty_mask]
                
                # 如果门店名称为空但人员编号不为空，保留并标记
                store_empty_but_user_exists = result_df['门店名称'] == ''
                if store_empty_but_user_exists.any():
                    logger.warning(f"发现 {store_empty_but_user_exists.sum()} 行门店名称为空但人员编号不为空的数据，已保留")
                    result_df.loc[store_empty_but_user_exists, '门店名称'] = '未知门店'
                
                # 如果人员编号为空但门店名称不为空，保留并标记
                user_empty_but_store_exists = result_df['人员编号'] == ''
                if user_empty_but_store_exists.any():
                    logger.warning(f"发现 {user_empty_but_store_exists.sum()} 行人员编号为空但门店名称不为空的数据，已保留")
                    result_df.loc[user_empty_but_store_exists, '人员编号'] = '未知编号'
                
                # 移除重复数据（保留第一次出现的）
                original_len = len(result_df)
                result_df = result_df.drop_duplicates(subset=['门店名称', '人员编号'], keep='first')
                duplicates_removed = original_len - len(result_df)
                if duplicates_removed > 0:
                    logger.info(f"移除重复数据: {duplicates_removed} 行")
                
                filter_stats['final_count'] = len(result_df)
                
                # 记录过滤统计
                logger.info(f"权限数据过滤统计:")
                logger.info(f"  原始数据: {filter_stats['original_count']} 行")
                logger.info(f"  门店名称为空: {filter_stats['empty_store']} 行")
                logger.info(f"  人员编号为空: {filter_stats['empty_user']} 行")
                logger.info(f"  两者都为空: {filter_stats['both_empty']} 行")
                logger.info(f"  重复数据: {duplicates_removed} 行")
                logger.info(f"  最终保留: {filter_stats['final_count']} 行")
                
                # 将统计信息保存到session state，供管理员界面显示
                st.session_state.permissions_filter_stats = {
                    'original_count': filter_stats['original_count'],
                    'empty_store': filter_stats['empty_store'],
                    'empty_user': filter_stats['empty_user'],
                    'both_empty': filter_stats['both_empty'],
                    'duplicates_removed': duplicates_removed,
                    'final_count': filter_stats['final_count'],
                    'load_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                
                if len(result_df) == 0:
                    logger.warning("过滤后权限数据为空")
                    return None
                
                # 重置索引
                result_df = result_df.reset_index(drop=True)
                
                logger.info(f"权限数据加载成功: {len(result_df)} 条记录")
                
                # 设置缓存
                set_cache(cache_key, result_df)
                return result_df
                
            except CosServiceError as e:
                if e.get_error_code() == 'NoSuchKey':
                    logger.info("权限文件不存在")
                    return None
                else:
                    logger.error(f"COS服务错误: {e.get_error_code()} - {e.get_error_msg()}")
                    raise e
            except Exception as e:
                logger.error(f"加载权限数据时发生错误: {str(e)}")
                raise e
        
        return safe_cos_operation(_load_operation)

def save_single_report_to_cos(df: pd.DataFrame, cos_client, bucket_name: str, store_name: str) -> bool:
    """保存单个门店报表到COS"""
    try:
        # 数据验证
        if df is None or len(df) == 0:
            logger.warning(f"门店 {store_name} 的数据为空，跳过保存")
            return False
        
        # 清理门店名称，用于文件名
        safe_store_name = "".join(c for c in store_name if c.isalnum() or c in (' ', '-', '_')).strip()
        safe_store_name = safe_store_name.replace(' ', '_')
        
        # 生成文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"reports/{safe_store_name}_{timestamp}.xlsx"
        
        # 创建Excel文件
        excel_buffer = io.BytesIO()
        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
            # 工作表名限制31字符
            sheet_name = store_name[:31] if len(store_name) <= 31 else store_name[:28] + "..."
            df.to_excel(writer, index=False, sheet_name=sheet_name)
        
        excel_content = excel_buffer.getvalue()
        
        # 上传到COS
        cos_client.put_object(
            Bucket=bucket_name,
            Body=excel_content,
            Key=filename,
            ContentType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            Metadata={
                'store-name': store_name,
                'upload-time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'row-count': str(len(df)),
                'col-count': str(len(df.columns))
            }
        )
        
        logger.info(f"报表保存成功: {store_name} -> {filename}")
        return True
        
    except Exception as e:
        logger.error(f"保存 {store_name} 报表失败: {str(e)}")
        return False

def save_reports_to_cos(reports_dict: Dict[str, pd.DataFrame], cos_client, bucket_name: str) -> bool:
    """保存报表数据到COS"""
    with error_handler("保存报表数据"):
        def _save_operation():
            if not reports_dict:
                raise DataProcessingError("没有报表数据需要保存")
            
            success_count = 0
            total_count = len(reports_dict)
            
            # 显示进度
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            for idx, (store_name, df) in enumerate(reports_dict.items()):
                try:
                    status_text.text(f"正在保存 {store_name}... ({idx+1}/{total_count})")
                    
                    if save_single_report_to_cos(df, cos_client, bucket_name, store_name):
                        success_count += 1
                    
                    # 更新进度
                    progress = (idx + 1) / total_count
                    progress_bar.progress(progress)
                    
                    # API限制延迟
                    time.sleep(0.3)
                    
                except Exception as e:
                    logger.error(f"保存门店 {store_name} 时出错: {str(e)}")
                    continue
            
            progress_bar.empty()
            status_text.empty()
            
            # 清除相关缓存
            clear_reports_cache()
            
            logger.info(f"报表数据保存完成: {success_count}/{total_count}")
            return success_count > 0  # 只要有一个成功就算成功
        
        return safe_cos_operation(_save_operation)

def clear_reports_cache():
    """清除报表相关缓存"""
    cache_keys_to_clear = []
    
    # 清除所有以cache_开头的键
    for key in list(st.session_state.keys()):
        if key.startswith('cache_'):
            cache_keys_to_clear.append(key)
    
    for key in cache_keys_to_clear:
        del st.session_state[key]
        logger.info(f"已清除缓存: {key}")

def get_store_list_from_cos(cos_client, bucket_name: str) -> List[str]:
    """从COS获取门店列表 - 优化版"""
    cache_key = get_cache_key("store_list", "load")
    cached_data = get_cache(cache_key)
    if cached_data is not None:
        return cached_data
    
    with error_handler("加载门店列表"):
        def _load_operation():
            try:
                store_set = set()
                marker = ''
                
                # 分页获取所有文件
                while True:
                    list_params = {
                        'Bucket': bucket_name,
                        'Prefix': 'reports/',
                        'MaxKeys': 1000
                    }
                    
                    if marker:
                        list_params['Marker'] = marker
                    
                    response = cos_client.list_objects(**list_params)
                    
                    if 'Contents' not in response:
                        break
                    
                    # 从文件名提取门店名称
                    for obj in response['Contents']:
                        key = obj['Key']
                        if key.endswith('.xlsx') and '/' in key:
                            # 从文件名提取门店名称
                            filename = key.split('/')[-1]  # 获取文件名
                            if '_' in filename:
                                store_name = filename.split('_')[0]  # 提取门店名称
                                # 还原下划线为空格
                                store_name = store_name.replace('_', ' ')
                                if store_name:
                                    store_set.add(store_name)
                    
                    # 检查是否还有更多数据
                    if response.get('IsTruncated') == 'true':
                        marker = response.get('NextMarker', '')
                        if not marker and response['Contents']:
                            marker = response['Contents'][-1]['Key']
                    else:
                        break
                
                store_list = sorted(list(store_set))
                logger.info(f"门店列表加载成功: {len(store_list)} 个门店")
                
                # 设置缓存
                set_cache(cache_key, store_list)
                return store_list
                
            except CosServiceError as e:
                logger.error(f"COS操作失败: {e.get_error_code()} - {e.get_error_msg()}")
                return []
            except Exception as e:
                logger.error(f"获取门店列表失败: {str(e)}")
                return []
        
        return safe_cos_operation(_load_operation)

def get_single_report_from_cos(cos_client, bucket_name: str, store_name: str) -> Optional[pd.DataFrame]:
    """从COS获取单个门店的报表数据 - 按需加载优化"""
    cache_key = get_cache_key("single_report", store_name)
    cached_data = get_cache(cache_key)
    if cached_data is not None:
        return cached_data
    
    with error_handler(f"加载门店 {store_name} 的报表"):
        def _load_operation():
            try:
                # 清理门店名称用于搜索
                safe_store_name = store_name.replace(' ', '_')
                
                # 列出该门店的所有报表文件
                response = cos_client.list_objects(
                    Bucket=bucket_name,
                    Prefix=f'reports/{safe_store_name}_',
                    MaxKeys=100
                )
                
                if 'Contents' not in response or len(response['Contents']) == 0:
                    logger.info(f"门店 {store_name} 没有报表文件")
                    return None
                
                # 获取最新的文件（按时间排序）
                latest_file = None
                latest_time = None
                
                for obj in response['Contents']:
                    key = obj['Key']
                    if key.endswith('.xlsx'):
                        file_time = obj['LastModified']
                        if latest_time is None or file_time > latest_time:
                            latest_time = file_time
                            latest_file = key
                
                if latest_file is None:
                    logger.info(f"门店 {store_name} 没有有效的Excel文件")
                    return None
                
                # 下载并解析最新的报表文件
                file_response = cos_client.get_object(
                    Bucket=bucket_name,
                    Key=latest_file
                )
                
                # 读取Excel文件
                excel_content = file_response['Body'].read()
                df = pd.read_excel(io.BytesIO(excel_content))
                
                # 数据处理和清理
                df = process_report_dataframe(df)
                
                logger.info(f"门店 {store_name} 报表加载成功: {len(df)} 行")
                
                # 设置缓存
                set_cache(cache_key, df)
                return df
                
            except CosServiceError as e:
                logger.error(f"COS操作失败: {e.get_error_code()} - {e.get_error_msg()}")
                return None
            except Exception as e:
                logger.error(f"加载报表数据失败: {str(e)}")
                return None
        
        return safe_cos_operation(_load_operation)

def process_report_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """处理报表DataFrame - 统一的数据清理逻辑"""
    if df is None or len(df) == 0:
        return pd.DataFrame()
    
    try:
        # 检查第一行是否是门店名称
        if len(df) > 0:
            first_row = df.iloc[0]
            non_empty_count = sum(1 for val in first_row if pd.notna(val) and str(val).strip() != '')
            
            # 如果第一行只有1-2个非空值，可能是门店名称行，跳过
            if non_empty_count <= 2 and len(df) > 1:
                df = df.iloc[1:].reset_index(drop=True)
        
        # 处理表头
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
            df = data_rows.reset_index(drop=True).fillna('')
        else:
            # 处理少于3行的数据
            df = df.fillna('')
            default_cols = []
            for i in range(len(df.columns)):
                col_name = f'列{i+1}' if i > 0 else '项目名称'
                default_cols.append(col_name)
            df.columns = default_cols
        
        return df
        
    except Exception as e:
        logger.error(f"处理DataFrame时出错: {str(e)}")
        return df.fillna('')

def analyze_receivable_data(df: pd.DataFrame) -> Dict[str, Any]:
    """分析应收未收额数据 - 专门查找第69行"""
    result = {}
    
    if len(df.columns) == 0 or len(df) == 0:
        return result
    
    # 使用处理后的数据
    processed_df = df.copy()
    
    # 查找第69行
    target_row_index = 68  # 第69行（从0开始索引）
    
    if len(processed_df) > target_row_index:
        row = processed_df.iloc[target_row_index]
        first_col_value = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ""
        
        # 检查关键词
        keywords = ['应收-未收额', '应收未收额', '应收-未收', '应收未收']
        
        for keyword in keywords:
            if keyword in first_col_value:
                # 查找数值（从右往左）
                for col_idx in range(len(row)-1, 0, -1):
                    val = row.iloc[col_idx]
                    if pd.notna(val) and str(val).strip() not in ['', 'None', 'nan']:
                        cleaned = str(val).replace(',', '').replace('¥', '').replace('￥', '').strip()
                        
                        # 处理括号表示负数
                        if cleaned.startswith('(') and cleaned.endswith(')'):
                            cleaned = '-' + cleaned[1:-1]
                        
                        try:
                            amount = float(cleaned)
                            if amount != 0:
                                result['应收-未收额'] = {
                                    'amount': amount,
                                    'column_name': str(processed_df.columns[col_idx]),
                                    'row_name': first_col_value,
                                    'row_index': target_row_index,
                                    'actual_row_number': target_row_index + 1
                                }
                                return result
                        except ValueError:
                            continue
                break
    
    # 备用查找策略
    if '应收-未收额' not in result:
        keywords = ['应收-未收额', '应收未收额', '应收-未收', '应收未收']
        
        for idx, row in processed_df.iterrows():
            try:
                row_name = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ""
                
                if not row_name.strip():
                    continue
                
                for keyword in keywords:
                    if keyword in row_name:
                        # 查找数值（从右往左）
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
                                            'column_name': str(processed_df.columns[col_idx]),
                                            'row_name': row_name,
                                            'row_index': idx,
                                            'actual_row_number': idx + 1,
                                            'note': f'在第{idx+1}行找到（非第69行）'
                                        }
                                        return result
                                except ValueError:
                                    continue
                        break
            except Exception as e:
                logger.warning(f"分析第{idx+1}行时出错: {str(e)}")
                continue
    
    # 调试信息
    result['debug_info'] = {
        'total_rows': len(processed_df),
        'checked_row_69': len(processed_df) > target_row_index,
        'row_69_content': str(processed_df.iloc[target_row_index].iloc[0]) if len(processed_df) > target_row_index else 'N/A'
    }
    
    return result

def verify_user_permission(store_name: str, user_id: str, permissions_data: Optional[pd.DataFrame]) -> bool:
    """验证用户权限"""
    if permissions_data is None or len(permissions_data.columns) < 2:
        logger.warning("权限数据无效")
        return False
    
    try:
        store_col = permissions_data.columns[0]
        id_col = permissions_data.columns[1]
        
        # 清理输入数据
        store_name = str(store_name).strip()
        user_id = str(user_id).strip()
        
        for _, row in permissions_data.iterrows():
            try:
                stored_store = str(row[store_col]).strip()
                stored_id = str(row[id_col]).strip()
                
                # 门店名称模糊匹配 + 用户ID精确匹配
                store_match = (store_name in stored_store or stored_store in store_name)
                id_match = (stored_id == user_id)
                
                if store_match and id_match:
                    logger.info(f"权限验证通过: {store_name} - {user_id}")
                    return True
                    
            except Exception as e:
                logger.warning(f"检查权限行时出错: {str(e)}")
                continue
        
        logger.warning(f"权限验证失败: {store_name} - {user_id}")
        return False
        
    except Exception as e:
        logger.error(f"权限验证出错: {str(e)}")
        return False

def find_matching_stores(store_name: str, store_list: List[str]) -> List[str]:
    """查找匹配的门店"""
    if not store_name or not store_list:
        return []
    
    matching = []
    store_name = str(store_name).strip()
    
    for store in store_list:
        store = str(store).strip()
        if store_name in store or store in store_name:
            matching.append(store)
    
    return sorted(matching)

def show_status_message(message: str, status_type: str = "info"):
    """显示状态消息"""
    css_class = f"status-{status_type}"
    st.markdown(f'<div class="{css_class}">{message}</div>', unsafe_allow_html=True)

def init_session_state():
    """初始化会话状态"""
    defaults = {
        'logged_in': False,
        'store_name': "",
        'user_id': "",
        'is_admin': False,
        'cos_client': None,
        'operation_status': [],
        'reports_uploader_key': 'initial_reports_uploader_key',
        'permissions_uploader_key': 'initial_permissions_uploader_key'
    }
    
    for key, default_value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = default_value

# 主程序开始
def main():
    # 初始化会话状态
    init_session_state()
    
    # 主标题
    st.markdown('<h1 class="main-header">📊 门店报表查询系统</h1>', unsafe_allow_html=True)
    
    # 初始化腾讯云COS客户端
    if not st.session_state.cos_client:
        try:
            with st.spinner("正在连接腾讯云存储..."):
                cos_client, bucket_name, permissions_file = get_cos_client()
                st.session_state.cos_client = (cos_client, bucket_name, permissions_file)
                show_status_message("✅ 腾讯云存储连接成功！", "success")
        except Exception as e:
            show_status_message(f"❌ 连接失败: {str(e)}", "error")
            st.error("请检查 secrets.toml 中的腾讯云配置是否正确")
            st.stop()
    
    cos_client, bucket_name, permissions_file = st.session_state.cos_client
    
    # 显示操作状态
    for status in st.session_state.operation_status:
        show_status_message(status['message'], status['type'])
    
    # 侧边栏
    with st.sidebar:
        st.title("⚙️ 系统功能")
        
        # 系统状态
        st.subheader("📡 系统状态")
        if cos_client:
            st.success("🟢 腾讯云存储已连接")
        else:
            st.error("🔴 腾讯云存储断开")
        
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
                permissions_file_upload = st.file_uploader(
                    "上传门店权限表", 
                    type=['xlsx', 'xls'],
                    key=st.session_state.permissions_uploader_key,
                    help="Excel文件，需包含门店名称和人员编号两列"
                )
                
                if permissions_file_upload:
                    try:
                        # 计算文件哈希
                        file_hash = hashlib.md5(permissions_file_upload.getvalue()).hexdigest()
                        
                        # 检查是否已处理过
                        if ("last_permissions_hash" in st.session_state and 
                            st.session_state.last_permissions_hash == file_hash and 
                            st.session_state.get("permissions_upload_successful", False)):
                            st.info("ℹ️ 该权限表已成功处理，无需重复操作。")
                        else:
                            st.session_state.last_permissions_hash = file_hash
                            st.session_state.permissions_upload_successful = False
                            
                            with st.spinner("分析Excel文件结构..."):
                                # 尝试读取Excel文件的所有工作表
                                excel_file = pd.ExcelFile(permissions_file_upload)
                                st.info(f"📄 发现 {len(excel_file.sheet_names)} 个工作表：{excel_file.sheet_names}")
                                
                                # 让用户选择工作表（如果有多个）
                                if len(excel_file.sheet_names) > 1:
                                    selected_sheet = st.selectbox(
                                        "选择包含权限数据的工作表：", 
                                        excel_file.sheet_names,
                                        key="permission_sheet_selector"
                                    )
                                else:
                                    selected_sheet = excel_file.sheet_names[0]
                                
                                # 读取选定的工作表
                                df_raw = pd.read_excel(permissions_file_upload, sheet_name=selected_sheet)
                                st.info(f"📊 原始数据：{len(df_raw)} 行 × {len(df_raw.columns)} 列")
                                
                                # 显示原始数据的前几行，帮助用户确认数据结构
                                st.subheader("🔍 原始数据预览")
                                st.dataframe(df_raw.head(10), use_container_width=True)
                                
                                # 让用户选择包含权限数据的列
                                if len(df_raw.columns) >= 2:
                                    col1, col2 = st.columns(2)
                                    with col1:
                                        store_column = st.selectbox(
                                            "选择门店名称列：",
                                            df_raw.columns.tolist(),
                                            index=0,
                                            key="store_column_selector"
                                        )
                                    with col2:
                                        user_column = st.selectbox(
                                            "选择人员编号列：",
                                            df_raw.columns.tolist(),
                                            index=1 if len(df_raw.columns) > 1 else 0,
                                            key="user_column_selector"
                                        )
                                    
                                    # 检查是否有表头行需要跳过
                                    header_row = st.number_input(
                                        "数据开始行（0表示第一行）：",
                                        min_value=0,
                                        max_value=len(df_raw)-1,
                                        value=0,
                                        key="header_row_selector"
                                    )
                                    
                                    if st.button("🚀 开始处理权限数据", key="process_permissions"):
                                        try:
                                            # 重新读取Excel，跳过指定的表头行
                                            if header_row > 0:
                                                df_processed = pd.read_excel(
                                                    permissions_file_upload, 
                                                    sheet_name=selected_sheet,
                                                    skiprows=header_row
                                                )
                                            else:
                                                df_processed = df_raw.copy()
                                            
                                            # 重新排列列顺序，确保门店名称和人员编号在前两列
                                            store_idx = df_processed.columns.get_loc(store_column)
                                            user_idx = df_processed.columns.get_loc(user_column)
                                            
                                            # 创建新的DataFrame，门店名称和人员编号作为前两列
                                            df = pd.DataFrame({
                                                '门店名称': df_processed[store_column],
                                                '人员编号': df_processed[user_column]
                                            })
                                            
                                            st.info(f"🔄 处理后数据：{len(df)} 行 × {len(df.columns)} 列")
                                            
                                            with st.spinner("保存到腾讯云..."):
                                                if save_permissions_to_cos(df, cos_client, bucket_name, permissions_file):
                                                    st.session_state.permissions_upload_successful = True
                                                    st.balloons()
                                                    
                                                    # 重置上传器
                                                    st.session_state.permissions_uploader_key = f"{datetime.now().timestamp()}_permissions"
                                                    st.rerun()
                                                else:
                                                    show_status_message("❌ 保存失败", "error")
                                                    st.session_state.permissions_upload_successful = False
                                        
                                        except Exception as e:
                                            show_status_message(f"❌ 数据处理失败：{str(e)}", "error")
                                            st.session_state.permissions_upload_successful = False
                                
                                else:
                                    show_status_message("❌ 数据格式错误：需要至少两列数据", "error")
                                    st.session_state.permissions_upload_successful = False
                    
                    except Exception as e:
                        show_status_message(f"❌ 文件读取失败：{str(e)}", "error")
                        st.session_state.permissions_upload_successful = False
                
                # 上传财务报表
                reports_file_upload = st.file_uploader(
                    "上传财务报表", 
                    type=['xlsx', 'xls'],
                    key=st.session_state.reports_uploader_key,
                    help="Excel文件，每个工作表代表一个门店的报表"
                )
                
                if reports_file_upload:
                    try:
                        # 计算文件哈希
                        file_hash = hashlib.md5(reports_file_upload.getvalue()).hexdigest()
                        
                        # 检查是否已处理过
                        if ("last_reports_hash" in st.session_state and 
                            st.session_state.last_reports_hash == file_hash and 
                            st.session_state.get("reports_upload_successful", False)):
                            st.info("ℹ️ 该报表文件已成功处理，无需重复操作。")
                        else:
                            st.session_state.last_reports_hash = file_hash
                            st.session_state.reports_upload_successful = False
                            
                            with st.spinner("处理报表文件..."):
                                excel_file = pd.ExcelFile(reports_file_upload)
                                reports_dict = {}
                                
                                for sheet in excel_file.sheet_names:
                                    try:
                                        df = pd.read_excel(reports_file_upload, sheet_name=sheet)
                                        if not df.empty:
                                            reports_dict[sheet] = df
                                            logger.info(f"读取工作表 '{sheet}': {len(df)} 行")
                                    except Exception as e:
                                        logger.warning(f"跳过工作表 '{sheet}': {str(e)}")
                                        continue
                                
                                if reports_dict:
                                    with st.spinner("保存到腾讯云..."):
                                        if save_reports_to_cos(reports_dict, cos_client, bucket_name):
                                            show_status_message(f"✅ 报表已上传：{len(reports_dict)} 个门店", "success")
                                            st.session_state.reports_upload_successful = True
                                            st.balloons()
                                            
                                            # 重置上传器
                                            st.session_state.reports_uploader_key = f"{datetime.now().timestamp()}_reports"
                                            st.rerun()
                                        else:
                                            show_status_message("❌ 保存失败", "error")
                                            st.session_state.reports_upload_successful = False
                                else:
                                    show_status_message("❌ 文件中没有有效的工作表", "error")
                                    st.session_state.reports_upload_successful = False
                                    
                    except Exception as e:
                        show_status_message(f"❌ 处理失败：{str(e)}", "error")
                        st.session_state.reports_upload_successful = False
                
                # 缓存管理
                st.subheader("🗂️ 缓存管理")
                col1, col2 = st.columns(2)
                
                with col1:
                    if st.button("清除所有缓存"):
                        cache_keys = [key for key in st.session_state.keys() if key.startswith('cache_')]
                        for key in cache_keys:
                            del st.session_state[key]
                        # 同时清除统计信息
                        if hasattr(st.session_state, 'permissions_filter_stats'):
                            del st.session_state.permissions_filter_stats
                        show_status_message("✅ 所有缓存已清除", "success")
                        st.rerun()
                
                with col2:
                    if st.button("🔄 重新加载权限数据"):
                        # 清除权限相关缓存
                        clear_permissions_cache()
                        if hasattr(st.session_state, 'permissions_filter_stats'):
                            del st.session_state.permissions_filter_stats
                        show_status_message("✅ 权限数据缓存已清除，将重新加载", "success")
                        st.rerun()
        
        else:
            if st.session_state.logged_in:
                st.subheader("👤 当前登录")
                st.info(f"门店：{st.session_state.store_name}")
                st.info(f"编号：{st.session_state.user_id}")
                
                if st.button("🚪 退出登录"):
                    st.session_state.logged_in = False
                    st.session_state.store_name = ""
                    st.session_state.user_id = ""
                    show_status_message("👋 已退出登录", "success")
                    st.rerun()
    
    # 清除状态消息
    st.session_state.operation_status = []
    
    # 主界面
    if user_type == "管理员" and st.session_state.is_admin:
        st.markdown('<div class="admin-panel"><h3>👨‍💼 管理员控制面板</h3><p>数据永久保存在腾讯云，支持高效存储和缓存机制</p></div>', unsafe_allow_html=True)
        
        try:
            with st.spinner("加载数据统计..."):
                permissions_data = load_permissions_from_cos(cos_client, bucket_name, permissions_file)
                store_list = get_store_list_from_cos(cos_client, bucket_name)
            
            col1, col2, col3 = st.columns(3)
            with col1:
                perms_count = len(permissions_data) if permissions_data is not None else 0
                st.metric("权限表用户数", perms_count)
            with col2:
                reports_count = len(store_list)
                st.metric("报表门店数", reports_count)
            with col3:
                cache_count = len([key for key in st.session_state.keys() if key.startswith('cache_')])
                st.metric("缓存项目数", cache_count)
                
            # 数据预览
            if permissions_data is not None and len(permissions_data) > 0:
                st.subheader("👥 权限数据预览")
                
                # 显示过滤统计（如果有的话）
                if hasattr(st.session_state, 'permissions_filter_stats'):
                    stats = st.session_state.permissions_filter_stats
                    
                    # 创建统计显示
                    st.markdown("#### 📊 数据处理统计")
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        st.metric(
                            "原始数据", 
                            f"{stats['original_count']} 行"
                        )
                    with col2:
                        st.metric(
                            "最终保留", 
                            f"{stats['final_count']} 行",
                            delta=f"{stats['final_count'] - stats['original_count']}"
                        )
                    with col3:
                        filtered_total = stats['both_empty'] + stats.get('duplicates_removed', 0)
                        st.metric(
                            "过滤数据", 
                            f"{filtered_total} 行"
                        )
                    with col4:
                        retention_rate = (stats['final_count'] / stats['original_count'] * 100) if stats['original_count'] > 0 else 0
                        st.metric(
                            "保留率", 
                            f"{retention_rate:.1f}%"
                        )
                    
                    # 详细过滤信息
                    with st.expander("🔍 详细过滤统计", expanded=False):
                        col1, col2 = st.columns(2)
                        with col1:
                            st.write("**过滤原因统计：**")
                            st.write(f"• 门店名称为空：{stats['empty_store']} 行")
                            st.write(f"• 人员编号为空：{stats['empty_user']} 行")
                            st.write(f"• 两者都为空：{stats['both_empty']} 行")
                            st.write(f"• 重复数据：{stats.get('duplicates_removed', 0)} 行")
                        with col2:
                            st.write("**数据质量：**")
                            if retention_rate >= 90:
                                st.success("✅ 数据质量良好")
                            elif retention_rate >= 70:
                                st.warning("⚠️ 数据质量一般，建议检查原始文件")
                            else:
                                st.error("❌ 数据质量较差，请检查文件格式")
                            
                            st.caption(f"统计时间：{stats.get('load_time', 'N/A')}")
                
                st.dataframe(permissions_data.head(10), use_container_width=True)
                
                if len(permissions_data) > 10:
                    st.caption(f"显示前10条记录，共{len(permissions_data)}条")
            
            if store_list:
                st.subheader("📊 门店列表预览")
                st.write(f"共有 {len(store_list)} 个门店")
                
                # 显示前10个门店
                display_stores = store_list[:10]
                for i in range(0, len(display_stores), 5):
                    cols = st.columns(5)
                    for j, store in enumerate(display_stores[i:i+5]):
                        with cols[j]:
                            st.info(f"🏪 {store}")
                
                if len(store_list) > 10:
                    st.caption(f"...以及其他 {len(store_list) - 10} 个门店")
                        
        except Exception as e:
            show_status_message(f"❌ 数据加载失败：{str(e)}", "error")
    
    elif user_type == "管理员" and not st.session_state.is_admin:
        st.info("👈 请在左侧边栏输入管理员密码")
    
    else:
        if not st.session_state.logged_in:
            st.subheader("🔐 用户登录")
            
            try:
                with st.spinner("加载权限数据..."):
                    permissions_data = load_permissions_from_cos(cos_client, bucket_name, permissions_file)
                
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
                                show_status_message("✅ 登录成功！", "success")
                                st.balloons()
                                st.rerun()
                            else:
                                show_status_message("❌ 门店或编号错误！", "error")
                                
            except Exception as e:
                show_status_message(f"❌ 权限验证失败：{str(e)}", "error")
        
        else:
            # 已登录 - 显示报表
            st.markdown(f'<div class="store-info"><h3>🏪 {st.session_state.store_name}</h3><p>操作员：{st.session_state.user_id}</p></div>', unsafe_allow_html=True)
            
            try:
                with st.spinner("加载门店列表..."):
                    store_list = get_store_list_from_cos(cos_client, bucket_name)
                    matching_stores = find_matching_stores(st.session_state.store_name, store_list)
                
                if matching_stores:
                    if len(matching_stores) > 1:
                        selected_store = st.selectbox("选择报表", matching_stores)
                    else:
                        selected_store = matching_stores[0]
                    
                    # 按需加载选定门店的报表数据
                    with st.spinner(f"加载 {selected_store} 的报表数据..."):
                        df = get_single_report_from_cos(cos_client, bucket_name, selected_store)
                    
                    if df is not None:
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
                            show_status_message(f"❌ 分析数据时出错：{str(e)}", "error")
                        
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
                            show_status_message(f"❌ 数据处理时出错：{str(e)}", "error")
                        
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
                                
                                csv = csv_df.to_csv(index=False, encoding='utf-8-sig')
                                st.download_button(
                                    "📥 下载CSV格式",
                                    csv,
                                    f"{st.session_state.store_name}_报表_{datetime.now().strftime('%Y%m%d')}.csv",
                                    "text/csv"
                                )
                            except Exception as e:
                                show_status_message(f"CSV下载准备失败：{str(e)}", "error")
                    
                    else:
                        st.error(f"❌ 无法加载门店 '{selected_store}' 的报表数据")
                    
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
        st.caption("🔧 版本: v3.2 (腾讯云优化版)")

if __name__ == "__main__":
    main()
