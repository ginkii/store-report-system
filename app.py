# streamlit_app.py - 门店报表系统完整版
"""
门店报表查询系统 - 完整功能单文件部署版本
包含查询、上传、权限管理功能
修复: 1.完全覆盖历史文件 2.修复表头消失问题 3.第41行第2个合计列应收金额
"""

import streamlit as st
import pandas as pd
import numpy as np
import pymongo
from pymongo import MongoClient
import plotly.express as px
import plotly.graph_objects as go
import traceback
import os
import time
import hashlib
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
import io

# 页面配置 - 修复重复配置问题
if "page_configured" not in st.session_state:
    st.set_page_config(
        page_title="门店报表系统",
        page_icon="🏪",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    st.session_state.page_configured = True

# 配置管理
class ConfigManager:
    """配置管理器"""
    
    @staticmethod
    def get_mongodb_config():
        """获取MongoDB配置"""
        try:
            if hasattr(st, 'secrets') and 'mongodb' in st.secrets:
                return {
                    'uri': st.secrets["mongodb"]["uri"],
                    'database_name': st.secrets["mongodb"]["database_name"]
                }
        except Exception:
            pass
        
        return {
            'uri': os.getenv('MONGODB_URI', 'mongodb://localhost:27017/'),
            'database_name': os.getenv('DATABASE_NAME', 'store_reports')
        }
    
    @staticmethod
    def get_admin_password():
        """获取管理员密码"""
        try:
            if hasattr(st, 'secrets') and 'security' in st.secrets:
                return st.secrets["security"]["admin_password"]
        except Exception:
            pass
        return os.getenv('ADMIN_PASSWORD', 'admin123')

# 数据库管理
try:
    import pymongo
    from pymongo import MongoClient
    PYMONGO_AVAILABLE = True
except ImportError:
    PYMONGO_AVAILABLE = False

class DatabaseManager:
    """数据库管理器"""
    
    def __init__(self):
        self.db = None
        self.client = None
        self._connect()
    
    def _connect(self):
        """建立数据库连接"""
        if not PYMONGO_AVAILABLE:
            st.error("PyMongo未安装，请检查requirements.txt文件")
            return
            
        try:
            config = ConfigManager.get_mongodb_config()
            self.client = MongoClient(config['uri'], serverSelectionTimeoutMS=5000)
            self.db = self.client[config['database_name']]
            
            # 测试连接
            self.db.command('ping')
            self._create_indexes()
            
        except Exception as e:
            # 更详细的错误信息
            error_msg = f"数据库连接失败: {e}"
            if "ServerSelectionTimeoutError" in str(type(e)):
                error_msg += "\n💡 提示：请检查MongoDB URI和网络连接"
            elif "Authentication" in str(e):
                error_msg += "\n💡 提示：请检查数据库用户名和密码"
            
            st.error(error_msg)
            self.db = None
            self.client = None
    
    def _create_indexes(self):
        """创建索引"""
        if self.db is None:
            return
            
        try:
            self.db['stores'].create_index([("store_code", 1)], background=True)
            self.db['permissions'].create_index([("query_code", 1)], background=True)
            self.db['reports'].create_index([("store_id", 1), ("report_month", -1)], background=True)
        except Exception:
            pass
    
    def get_database(self):
        """获取数据库实例"""
        return self.db
    
    def is_connected(self):
        """检查数据库是否连接"""
        return self.db is not None

# 全局数据库管理器
@st.cache_resource
def get_db_manager():
    return DatabaseManager()

# 数据模型
class StoreModel:
    """门店数据模型"""
    
    @staticmethod
    def create_store_document(store_name: str, store_code: str = None, **kwargs) -> Dict:
        """创建标准门店文档"""
        timestamp = int(datetime.now().timestamp())
        return {
            '_id': kwargs.get('_id', f"store_{store_code or store_name.replace(' ', '_')}_{timestamp}"),
            'store_name': store_name.strip(),
            'store_code': store_code or StoreModel._generate_store_code(store_name),
            'region': kwargs.get('region', '未分类'),
            'manager': kwargs.get('manager', '待设置'),
            'aliases': kwargs.get('aliases', [store_name.strip()]),
            'created_at': kwargs.get('created_at', datetime.now()),
            'created_by': kwargs.get('created_by', 'system'),
            'status': kwargs.get('status', 'active')
        }
    
    @staticmethod
    def _generate_store_code(store_name: str) -> str:
        """生成门店代码"""
        try:
            normalized = store_name.replace('犀牛百货', '').replace('门店', '').replace('店', '').strip()
            hash_obj = hashlib.md5(normalized.encode('utf-8'))
            return f"AUTO_{hash_obj.hexdigest()[:6].upper()}"
        except Exception:
            return f"AUTO_{int(datetime.now().timestamp()) % 100000}"

class ReportModel:
    """报表数据模型"""
    
    @staticmethod
    def create_report_document(store_data: Dict, report_month: str, excel_data: List[Dict], headers: List[str], **kwargs) -> Dict:
        """创建标准报表文档，保存完整表头"""
        return {
            'store_id': store_data['_id'],
            'store_code': store_data['store_code'],
            'store_name': store_data['store_name'],
            'report_month': report_month,
            'sheet_name': kwargs.get('sheet_name', store_data['store_name']),
            'raw_excel_data': excel_data,
            'table_headers': headers,  # 新增：保存表头信息
            'financial_data': kwargs.get('financial_data', {}),
            'created_at': kwargs.get('created_at', datetime.now()),
            'updated_at': datetime.now(),
            'uploaded_by': kwargs.get('uploaded_by', 'system')
        }
    
    @staticmethod
    def dataframe_to_dict_list(df: pd.DataFrame) -> tuple[List[Dict], List[str]]:
        """将DataFrame转换为字典列表，保留表头信息并修复#NAME?错误"""
        # 保存原始列名作为表头
        headers = [str(col) for col in df.columns]
        
        result = []
        for index, row in df.iterrows():
            row_dict = {}
            for col_idx, value in enumerate(row):
                col_key = f"col_{col_idx}"
                if pd.isna(value):
                    row_dict[col_key] = ""
                elif isinstance(value, (int, float)):
                    row_dict[col_key] = float(value) if not pd.isna(value) else 0.0
                else:
                    # 修复CSV中的#NAME?错误
                    value_str = str(value).strip()
                    if value_str.startswith('='):
                        # 处理Excel公式，特别是"=--平台内支出"这类
                        if '平台内支出' in value_str:
                            row_dict[col_key] = "--平台内支出"
                        elif value_str.startswith('=--'):
                            row_dict[col_key] = value_str[3:]  # 去除"=--"
                        else:
                            row_dict[col_key] = value_str[1:]  # 去除"="
                    else:
                        row_dict[col_key] = value_str
            result.append(row_dict)
        
        return result, headers

class PermissionModel:
    """权限数据模型"""
    
    @staticmethod
    def create_permission_document(query_code: str, store_data: Dict, **kwargs) -> Dict:
        """创建标准权限文档"""
        return {
            'query_code': query_code.strip(),
            'store_id': store_data['_id'],
            'store_name': store_data['store_name'],
            'store_code': store_data['store_code'],
            'created_at': kwargs.get('created_at', datetime.now()),
            'updated_at': datetime.now(),
            'created_by': kwargs.get('created_by', 'system'),
            'status': kwargs.get('status', 'active')
        }

# 批量上传器
class BulkReportUploader:
    """批量报表上传器"""
    
    def __init__(self, db):
        if db is None:
            raise Exception("数据库连接失败")
        self.db = db
        self.stores_collection = self.db['stores']
        self.reports_collection = self.db['reports']
    
    def normalize_store_name(self, sheet_name: str) -> str:
        """标准化门店名称"""
        name = sheet_name.strip()
        name = name.replace('犀牛百货', '').replace('门店', '').replace('店', '')
        name = name.replace('(', '').replace(')', '').replace('（', '').replace('）', '')
        name = ''.join(name.split())
        return name
    
    def find_or_create_store(self, sheet_name: str) -> Optional[Dict]:
        """通过sheet名称查找门店，如果不存在则创建"""
        normalized_name = self.normalize_store_name(sheet_name)
        
        # 查找现有门店
        search_patterns = [
            {"store_name": sheet_name},
            {"store_name": {"$regex": normalized_name, "$options": "i"}},
            {"aliases": {"$in": [sheet_name, normalized_name]}},
        ]
        
        for pattern in search_patterns:
            try:
                store = self.stores_collection.find_one(pattern)
                if store:
                    return store
            except Exception:
                continue
        
        # 创建新门店
        return self._create_store_from_sheet_name(sheet_name)
    
    def _create_store_from_sheet_name(self, sheet_name: str) -> Optional[Dict]:
        """从工作表名称创建新门店"""
        try:
            store_data = StoreModel.create_store_document(
                store_name=sheet_name.strip(),
                aliases=[sheet_name.strip(), self.normalize_store_name(sheet_name)],
                created_by='bulk_upload'
            )
            self.stores_collection.insert_one(store_data)
            return store_data
        except Exception as e:
            st.error(f"创建门店失败: {e}")
            return None
    
    def process_excel_file(self, file_buffer, report_month: str, clear_history: bool = True, progress_callback=None) -> Dict:
        """处理Excel文件并上传报表数据"""
        start_time = time.time()
        result = {
            'success_count': 0,
            'failed_count': 0,
            'errors': [],
            'processed_stores': [],
            'failed_stores': [],
            'total_time': 0,
            'cleared_count': 0
        }
        
        try:
            if progress_callback:
                progress_callback(5, "准备上传，清理历史数据...")
            
            # 1. 完全清除历史数据
            if clear_history:
                try:
                    clear_result = self.reports_collection.delete_many({'report_month': report_month})
                    result['cleared_count'] = clear_result.deleted_count
                    if progress_callback:
                        progress_callback(10, f"已清除 {result['cleared_count']} 条历史数据")
                except Exception as e:
                    result['errors'].append(f"清除历史数据失败: {str(e)}")
            
            if progress_callback:
                progress_callback(15, "正在读取Excel文件...")
            
            # 2. 读取Excel文件 - 以第4行为表头
            excel_data = pd.read_excel(file_buffer, sheet_name=None, engine='openpyxl', header=3)  # header=3 表示第4行为表头
            total_sheets = len(excel_data)
            
            if progress_callback:
                progress_callback(20, f"发现 {total_sheets} 个工作表，开始处理...")
            
            processed = 0
            
            for sheet_name, df in excel_data.items():
                try:
                    processed += 1
                    progress = 20 + (processed / total_sheets) * 70
                    if progress_callback:
                        progress_callback(progress, f"正在处理: {sheet_name}")
                    
                    store = self.find_or_create_store(sheet_name)
                    if not store:
                        result['failed_stores'].append({
                            'store_name': sheet_name,
                            'reason': '无法创建门店记录'
                        })
                        result['failed_count'] += 1
                        continue
                    
                    # 3. 处理数据 - 保持完整表头
                    df_cleaned = df.dropna(axis=1, how='all')
                    if df_cleaned.empty:
                        result['failed_stores'].append({
                            'store_name': sheet_name,
                            'reason': '数据为空'
                        })
                        result['failed_count'] += 1
                        continue
                    
                    # 4. 转换数据格式，保存表头
                    excel_data_dict, headers = ReportModel.dataframe_to_dict_list(df_cleaned)
                    financial_data = self._extract_financial_data_v2(df_cleaned)
                    
                    # 5. 创建报表文档
                    report_data = ReportModel.create_report_document(
                        store_data=store,
                        report_month=report_month,
                        excel_data=excel_data_dict,
                        headers=headers,  # 保存表头
                        sheet_name=sheet_name,
                        financial_data=financial_data,
                        uploaded_by='bulk_upload'
                    )
                    
                    # 6. 保存到数据库（不检查existing，因为已经清空）
                    self.reports_collection.insert_one(report_data)
                    
                    result['success_count'] += 1
                    result['processed_stores'].append({
                        'sheet_name': sheet_name,
                        'store_name': store['store_name'],
                        'store_code': store['store_code']
                    })
                
                except Exception as e:
                    result['failed_stores'].append({
                        'store_name': sheet_name,
                        'reason': f"处理错误: {str(e)}"
                    })
                    result['failed_count'] += 1
                    result['errors'].append(f"{sheet_name}: {str(e)}")
            
            if progress_callback:
                progress_callback(100, "上传完成！")
            
        except Exception as e:
            result['errors'].append(f"文件处理失败: {str(e)}")
        
        result['total_time'] = time.time() - start_time
        return result
    
    def _extract_financial_data_v2(self, df: pd.DataFrame) -> Dict:
        """改进的财务数据提取 - 第4行为表头，查找合计列，从第37行提取总部应收未收金额"""
        financial_data = {
            'revenue': {},
            'cost': {},
            'profit': {},
            'receivables': {},
            'other_metrics': {}
        }
        
        try:
            # 1. 查找合计列
            total_col_indices = []
            
            for col_idx, col_name in enumerate(df.columns):
                col_str = str(col_name).lower().strip()
                if any(keyword in col_str for keyword in [
                    '合计', 'total', '总计', '小计', 'sum', '汇总',
                    '金额', '总金额', '合计金额', '小计金额',
                    '总额', '总和', '累计', '统计'
                ]):
                    total_col_indices.append(col_idx)
            
            # 如果没有找到合计列，按数值含量智能识别
            if not total_col_indices:
                numeric_counts = []
                for col_idx in range(len(df.columns)):
                    try:
                        numeric_count = df.iloc[:, col_idx].apply(lambda x: pd.to_numeric(x, errors='coerce')).notna().sum()
                        numeric_counts.append((col_idx, numeric_count))
                    except:
                        numeric_counts.append((col_idx, 0))
                
                # 按数字含量排序，取前2个作为合计列
                numeric_counts.sort(key=lambda x: x[1], reverse=True)
                if len(numeric_counts) >= 2:
                    total_col_indices = [numeric_counts[0][0], numeric_counts[1][0]]
            
            # 调试信息：记录列识别结果
            financial_data['other_metrics']['所有列名'] = [str(col) for col in df.columns]
            financial_data['other_metrics']['合计列位置'] = str(total_col_indices)
            financial_data['other_metrics']['合计列数量'] = len(total_col_indices)
            if total_col_indices:
                financial_data['other_metrics']['合计列名称'] = [str(df.columns[i]) for i in total_col_indices]
            
            # 2. 直接从第37行第2个合计列提取总部应收未收金额
            if len(df) >= 37 and len(total_col_indices) >= 2:
                target_row_index = 36  # 第37行（索引36，因为第4行为表头）
                target_col_idx = total_col_indices[1]  # 使用第二个合计列
                column_desc = f"第{target_col_idx+1}列(第2个合计列)"
                
                try:
                    # 直接提取第37行第2个合计列的值
                    raw_value = df.iloc[target_row_index, target_col_idx]
                    financial_data['other_metrics']['第37行第2合计列原值'] = str(raw_value)
                    financial_data['other_metrics']['使用列索引'] = target_col_idx
                    financial_data['other_metrics']['使用列描述'] = column_desc
                    
                    parsed_value = pd.to_numeric(raw_value, errors='coerce')
                    if not pd.isna(parsed_value):
                        financial_data['receivables']['net_amount'] = float(parsed_value)
                        financial_data['other_metrics']['总部应收未收金额'] = float(parsed_value)
                        financial_data['other_metrics']['提取位置'] = f"第37行{column_desc}"
                        financial_data['other_metrics']['提取成功'] = True
                        financial_data['other_metrics']['数值处理'] = "直接显示在可视化看板"
                    else:
                        financial_data['other_metrics']['提取失败原因'] = "数值转换失败"
                        
                except (ValueError, TypeError, IndexError) as e:
                    financial_data['other_metrics']['提取失败原因'] = f"异常: {str(e)}"
                    
            else:
                if len(df) < 37:
                    financial_data['other_metrics']['提取失败原因'] = f"数据行数不足37行，实际{len(df)}行"
                elif len(total_col_indices) < 2:
                    financial_data['other_metrics']['提取失败原因'] = f"合计列数不足2列，实际{len(total_col_indices)}列"
            
            # 3. 提取其他财务指标
            for idx, row in df.iterrows():
                try:
                    if len(row) < 2:
                        continue
                    
                    metric_name = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ""
                    if not metric_name:
                        continue
                    
                    # 查找数值（优先从合计列取值）
                    value = None
                    
                    # 先从合计列查找
                    for col_idx in total_col_indices:
                        if col_idx < len(row):
                            try:
                                if pd.notna(row.iloc[col_idx]):
                                    value = float(row.iloc[col_idx])
                                    break
                            except:
                                continue
                    
                    # 如果合计列没有值，从其他列查找
                    if value is None:
                        for col_idx in range(1, len(row)):
                            if col_idx not in total_col_indices:  # 跳过合计列
                                try:
                                    if pd.notna(row.iloc[col_idx]):
                                        value = float(row.iloc[col_idx])
                                        break
                                except:
                                    continue
                    
                    if value is None:
                        value = 0
                    
                    # 4. 分类存储财务指标
                    if any(keyword in metric_name for keyword in ['收入', '营收', '销售额', '营业收入']):
                        if '线上' in metric_name:
                            financial_data['revenue']['online_revenue'] = value
                        elif '线下' in metric_name:
                            financial_data['revenue']['offline_revenue'] = value
                        elif '总' in metric_name or '合计' in metric_name:
                            financial_data['revenue']['total_revenue'] = value
                    
                    elif any(keyword in metric_name for keyword in ['成本', '费用', '支出']):
                        if '商品' in metric_name:
                            financial_data['cost']['product_cost'] = value
                        elif '租金' in metric_name or '房租' in metric_name:
                            financial_data['cost']['rent_cost'] = value
                        elif '人工' in metric_name or '工资' in metric_name:
                            financial_data['cost']['labor_cost'] = value
                    
                    elif any(keyword in metric_name for keyword in ['利润', '盈利', '净利', '毛利']):
                        if '毛利' in metric_name:
                            financial_data['profit']['gross_profit'] = value
                        elif '净利' in metric_name:
                            financial_data['profit']['net_profit'] = value
                    
                    # 保存所有指标到other_metrics用于调试
                    if metric_name and value != 0:
                        financial_data['other_metrics'][f"第{idx+1}行_{metric_name}"] = value
                
                except:
                    continue
            
        except Exception as e:
            st.error(f"提取财务数据时出错: {e}")
        
        return financial_data

# 权限管理器
class PermissionManager:
    """权限管理器"""
    
    def __init__(self, db):
        if db is None:
            raise Exception("数据库连接失败")
        self.db = db
        self.permissions_collection = self.db['permissions']
        self.stores_collection = self.db['stores']
    
    def upload_permission_table(self, uploaded_file) -> Dict:
        """上传权限表"""
        try:
            if uploaded_file.name.endswith('.csv'):
                df = pd.read_csv(uploaded_file)
            else:
                df = pd.read_excel(uploaded_file)
            
            # 自动识别列名
            query_code_col = None
            store_name_col = None
            
            for col in df.columns:
                col_str = str(col).lower().strip()
                if any(keyword in col_str for keyword in ['查询编号', 'query', 'code', '编号', '代码', '查询码']):
                    query_code_col = col
                    break
            
            for col in df.columns:
                col_str = str(col).lower().strip()
                if any(keyword in col_str for keyword in ['门店名称', 'store', '门店', '名称', 'name', 'shop']):
                    store_name_col = col
                    break
            
            if not query_code_col or not store_name_col:
                if len(df.columns) >= 2:
                    query_code_col = df.columns[0]
                    store_name_col = df.columns[1]
                else:
                    return {"success": False, "message": "文件至少需要两列数据"}
            
            results = {
                "success": True,
                "processed": 0,
                "created": 0,
                "updated": 0,
                "errors": [],
                "detected_columns": {
                    "query_code": str(query_code_col),
                    "store_name": str(store_name_col)
                }
            }
            
            for _, row in df.iterrows():
                try:
                    query_code = str(row[query_code_col]).strip()
                    store_name = str(row[store_name_col]).strip()
                    
                    if not query_code or not store_name or query_code == 'nan' or store_name == 'nan':
                        continue
                    
                    store = self._find_or_create_store(store_name)
                    if not store:
                        results["errors"].append(f"无法处理门店: {store_name}")
                        continue
                    
                    existing = self.permissions_collection.find_one({'query_code': query_code})
                    
                    permission_doc = PermissionModel.create_permission_document(
                        query_code=query_code,
                        store_data=store,
                        created_at=existing.get('created_at') if existing else None,
                        created_by=existing.get('created_by', 'upload') if existing else 'upload'
                    )
                    
                    if existing:
                        self.permissions_collection.replace_one(
                            {'query_code': query_code},
                            permission_doc
                        )
                        results["updated"] += 1
                    else:
                        self.permissions_collection.insert_one(permission_doc)
                        results["created"] += 1
                    
                    results["processed"] += 1
                
                except Exception as e:
                    results["errors"].append(f"处理行数据时出错: {str(e)}")
            
            return results
            
        except Exception as e:
            return {"success": False, "message": f"处理文件时出错: {str(e)}"}
    
    def _find_or_create_store(self, store_name: str) -> Optional[Dict]:
        """根据门店名称查找门店，如果不存在则创建"""
        try:
            # 精确匹配
            store = self.stores_collection.find_one({'store_name': store_name})
            if store:
                return store
            
            # 模糊匹配
            clean_name = store_name.replace('犀牛百货', '').replace('门店', '').replace('店', '').strip()
            if clean_name:
                stores = list(self.stores_collection.find({
                    '$or': [
                        {'store_name': {'$regex': clean_name, '$options': 'i'}},
                        {'aliases': {'$in': [store_name, clean_name]}}
                    ]
                }))
                if stores:
                    return stores[0]
            
            # 创建新门店
            store_data = StoreModel.create_store_document(
                store_name=store_name,
                created_by='permission_upload'
            )
            self.stores_collection.insert_one(store_data)
            return store_data
            
        except Exception as e:
            st.error(f"查找门店时出错: {e}")
            return None
    
    def get_all_permissions(self) -> List[Dict]:
        """获取所有权限配置"""
        try:
            return list(self.permissions_collection.find().sort('query_code', 1))
        except Exception as e:
            st.error(f"获取权限配置失败: {e}")
            return []
    
    def delete_permission(self, query_code: str) -> bool:
        """删除权限配置"""
        try:
            result = self.permissions_collection.delete_one({'query_code': query_code})
            return result.deleted_count > 0
        except Exception as e:
            st.error(f"删除权限配置失败: {e}")
            return False

# 报表数据处理工具
def rebuild_dataframe_with_headers(raw_data: List[Dict], headers: List[str]) -> pd.DataFrame:
    """根据保存的表头重建DataFrame，解决表头消失问题"""
    if not raw_data or not headers:
        return pd.DataFrame()
    
    try:
        # 重建数据矩阵
        data_matrix = []
        for row_data in raw_data:
            row_values = []
            for col_idx in range(len(headers)):
                col_key = f"col_{col_idx}"
                value = row_data.get(col_key, "")
                row_values.append(value)
            data_matrix.append(row_values)
        
        # 使用保存的表头创建DataFrame
        df = pd.DataFrame(data_matrix, columns=headers)
        return df.fillna('')
    
    except Exception as e:
        st.error(f"重建表格失败: {e}")
        return pd.DataFrame()

# 应用界面
def create_query_app():
    """门店查询应用"""
    st.title("🔍 门店查询系统")
    
    db_manager = get_db_manager()
    if not db_manager.is_connected():
        st.error("数据库连接失败，请检查配置")
        return
    
    db = db_manager.get_database()
    
    # 检查登录状态
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False
    
    if not st.session_state.authenticated:
        st.subheader("🔐 查询编号登录")
        
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            query_code = st.text_input("查询编号", placeholder="请输入查询编号")
            
            if st.button("登录", use_container_width=True):
                if query_code:
                    try:
                        permission = db['permissions'].find_one({'query_code': query_code})
                        if permission:
                            store = db['stores'].find_one({'_id': permission['store_id']})
                            if store:
                                st.session_state.authenticated = True
                                st.session_state.store_info = store
                                st.session_state.query_code = query_code
                                st.success(f"登录成功！欢迎 {store['store_name']}")
                                st.rerun()
                            else:
                                st.error("门店信息不存在")
                        else:
                            st.error("查询编号无效")
                    except Exception as e:
                        st.error(f"查询失败: {e}")
                else:
                    st.warning("请输入查询编号")
    else:
        # 已登录，显示报表
        store_info = st.session_state.store_info
        
        with st.sidebar:
            st.info(f"当前门店: {store_info['store_name']}")
            if st.button("退出登录"):
                st.session_state.authenticated = False
                st.rerun()
        
        st.title(f"📊 {store_info['store_name']}")
        
        # 获取报表数据
        try:
            reports = list(db['reports'].find({'store_id': store_info['_id']}).sort('report_month', -1))
            
            if reports:
                # 美化的应收未收看板
                try:
                    latest_report = reports[0]
                    receivables = latest_report.get('financial_data', {}).get('receivables', {})
                    amount = receivables.get('net_amount', 0)
                    st.markdown("### 总部应收未收金额")
                    
                    # 添加自定义CSS样式
                    if amount < 0:
                        # 负数：总部应退 - 渐变绿色
                        abs_amount = abs(amount)
                        st.markdown(f"""
                        <div style="
                            background: linear-gradient(135deg, #4CAF50, #8BC34A, #CDDC39);
                            padding: 30px;
                            border-radius: 15px;
                            text-align: center;
                            box-shadow: 0 8px 25px rgba(76, 175, 80, 0.3);
                            margin: 20px 0;
                            border: 3px solid #4CAF50;
                        ">
                            <div style="
                                font-size: 28px;
                                font-weight: bold;
                                color: white;
                                text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
                                margin-bottom: 15px;
                                letter-spacing: 2px;
                            ">
                                总部应退
                            </div>
                            <div style="
                                font-size: 48px;
                                font-weight: 900;
                                color: white;
                                text-shadow: 3px 3px 6px rgba(0,0,0,0.4);
                                font-family: 'Arial Black', sans-serif;
                            ">
                                ¥{abs_amount:,.2f}
                            </div>
                        </div>
                        """, unsafe_allow_html=True)
                    elif amount > 0:
                        # 正数：门店应返 - 渐变蓝色
                        st.markdown(f"""
                        <div style="
                            background: linear-gradient(135deg, #2196F3, #03A9F4, #00BCD4);
                            padding: 30px;
                            border-radius: 15px;
                            text-align: center;
                            box-shadow: 0 8px 25px rgba(33, 150, 243, 0.3);
                            margin: 20px 0;
                            border: 3px solid #2196F3;
                        ">
                            <div style="
                                font-size: 28px;
                                font-weight: bold;
                                color: white;
                                text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
                                margin-bottom: 15px;
                                letter-spacing: 2px;
                            ">
                                门店应返
                            </div>
                            <div style="
                                font-size: 48px;
                                font-weight: 900;
                                color: white;
                                text-shadow: 3px 3px 6px rgba(0,0,0,0.4);
                                font-family: 'Arial Black', sans-serif;
                            ">
                                ¥{amount:,.2f}
                            </div>
                        </div>
                        """, unsafe_allow_html=True)
                    else:
                        # 零：已结清 - 渐变灰色
                        st.markdown(f"""
                        <div style="
                            background: linear-gradient(135deg, #9E9E9E, #BDBDBD, #E0E0E0);
                            padding: 30px;
                            border-radius: 15px;
                            text-align: center;
                            box-shadow: 0 8px 25px rgba(158, 158, 158, 0.3);
                            margin: 20px 0;
                            border: 3px solid #9E9E9E;
                        ">
                            <div style="
                                font-size: 28px;
                                font-weight: bold;
                                color: white;
                                text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
                                margin-bottom: 15px;
                                letter-spacing: 2px;
                            ">
                                已结清
                            </div>
                            <div style="
                                font-size: 48px;
                                font-weight: 900;
                                color: white;
                                text-shadow: 3px 3px 6px rgba(0,0,0,0.4);
                                font-family: 'Arial Black', sans-serif;
                            ">
                                ¥0.00
                            </div>
                        </div>
                        """, unsafe_allow_html=True)
                        
                except Exception:
                    # 错误状态的看板
                    st.markdown(f"""
                    <div style="
                        background: linear-gradient(135deg, #FF5722, #FF7043, #FF8A65);
                        padding: 30px;
                        border-radius: 15px;
                        text-align: center;
                        box-shadow: 0 8px 25px rgba(255, 87, 34, 0.3);
                        margin: 20px 0;
                        border: 3px solid #FF5722;
                    ">
                        <div style="
                            font-size: 28px;
                            font-weight: bold;
                            color: white;
                            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
                            margin-bottom: 15px;
                            letter-spacing: 2px;
                        ">
                            暂无数据
                        </div>
                        <div style="
                            font-size: 24px;
                            font-weight: 600;
                            color: white;
                            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
                            font-family: Arial, sans-serif;
                        ">
                            请联系管理员
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                
                # 报表数据展示 - 修复表头问题
                st.subheader("报表数据")
                
                try:
                    latest_report = reports[0]
                    raw_data = latest_report.get('raw_excel_data', [])
                    headers = latest_report.get('table_headers', [])
                    
                    if raw_data and headers:
                        # 使用保存的表头重建DataFrame
                        df = rebuild_dataframe_with_headers(raw_data, headers)
                        
                        if not df.empty:
                            # 显示只读表格
                            st.dataframe(df, use_container_width=True, height=400)
                            
                            # 提供Excel下载功能
                            buffer = io.BytesIO()
                            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                                df.to_excel(writer, index=False, sheet_name=store_info['store_name'][:31])
                            
                            st.download_button(
                                label="📥 下载完整报表 (Excel)",
                                data=buffer.getvalue(),
                                file_name=f"{store_info['store_name']}_{latest_report['report_month']}_报表.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                            )
                        else:
                            st.info("报表数据格式错误")
                    else:
                        st.info("暂无报表数据")
                        
                except Exception as e:
                    st.error(f"数据显示错误: {e}")
                    
                    # 显示调试信息
                    with st.expander("调试信息"):
                        st.write("原始数据预览:", latest_report.get('raw_excel_data', [])[:5])
                        st.write("表头信息:", latest_report.get('table_headers', []))
            else:
                st.info("暂无报表数据")
        except Exception as e:
            st.error(f"查询报表失败: {e}")

def create_upload_app():
    """批量上传应用"""
    st.title("📤 批量上传系统")
    
    db_manager = get_db_manager()
    if not db_manager.is_connected():
        st.error("数据库连接失败，请检查配置")
        return
    
    # 管理员验证
    if 'admin_authenticated' not in st.session_state:
        st.session_state.admin_authenticated = False
    
    if not st.session_state.admin_authenticated:
        st.subheader("🔐 管理员登录")
        
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            password = st.text_input("管理员密码", type="password")
            
            if st.button("登录", use_container_width=True):
                if password == ConfigManager.get_admin_password():
                    st.session_state.admin_authenticated = True
                    st.success("管理员登录成功！")
                    st.rerun()
                else:
                    st.error("密码错误")
        return
    
    db = db_manager.get_database()
    
    try:
        uploader = BulkReportUploader(db)
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.subheader("上传设置")
            
            # 月份选择
            report_month = st.text_input(
                "报表月份",
                value=datetime.now().strftime("%Y-%m"),
                help="格式：YYYY-MM，例如：2024-12"
            )
            
            # 清除历史数据选项
            clear_history = st.checkbox(
                "🗑️ 完全覆盖历史数据", 
                value=True,
                help="勾选后将清除该月份的所有历史数据，确保数据一致性"
            )
            
            if clear_history:
                st.warning("⚠️ 将清除该月份所有历史数据，上传的新文件将完全替换旧数据")
            
            # 文件上传
            uploaded_file = st.file_uploader(
                "选择Excel文件",
                type=['xlsx', 'xls'],
                help="选择包含所有门店报表的Excel文件，每个工作表对应一个门店"
            )
            
            if uploaded_file and report_month:
                if st.button("开始上传", type="primary", use_container_width=True):
                    # 进度显示
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    def update_progress(progress, message):
                        progress_bar.progress(progress / 100)
                        status_text.text(message)
                    
                    # 处理文件
                    result = uploader.process_excel_file(
                        uploaded_file, 
                        report_month, 
                        clear_history=clear_history,
                        progress_callback=update_progress
                    )
                    
                    # 显示结果
                    st.subheader("📊 上传结果")
                    
                    # 结果统计
                    col_cleared, col_success, col_failed, col_time = st.columns(4)
                    with col_cleared:
                        st.metric("🗑️ 清理历史", result['cleared_count'])
                    with col_success:
                        st.metric("✅ 成功上传", result['success_count'])
                    with col_failed:
                        st.metric("❌ 失败数量", result['failed_count'])
                    with col_time:
                        st.metric("⏱️ 总耗时", f"{result['total_time']:.2f}s")
                    
                    # 成功信息
                    if result['success_count'] > 0:
                        st.success(f"✅ 成功处理 {result['success_count']} 个门店的数据")
                        
                        if result['processed_stores']:
                            with st.expander("查看成功上传的门店"):
                                success_df = pd.DataFrame(result['processed_stores'])
                                st.dataframe(success_df, use_container_width=True)
                        
                        # 显示应收未收金额提取调试信息
                        with st.expander("🔧 应收金额提取调试信息"):
                            try:
                                # 获取一个示例报表的调试信息
                                sample_report = db['reports'].find_one({'report_month': report_month})
                                if sample_report:
                                    debug_info = sample_report.get('financial_data', {}).get('other_metrics', {})
                                    if debug_info:
                                        for key, value in debug_info.items():
                                            st.write(f"**{key}:** {value}")
                                    else:
                                        st.write("无调试信息")
                                else:
                                    st.write("未找到报表数据")
                            except Exception as e:
                                st.write(f"获取调试信息失败: {e}")
                    
                    # 失败信息
                    if result['failed_count'] > 0:
                        st.error(f"❌ {result['failed_count']} 个门店上传失败")
                        
                        if result['failed_stores']:
                            with st.expander("查看失败详情"):
                                failed_df = pd.DataFrame(result['failed_stores'])
                                st.dataframe(failed_df, use_container_width=True)
                    
                    # 错误信息
                    if result['errors']:
                        with st.expander("查看错误详情"):
                            for error in result['errors']:
                                st.error(error)
                    
                    progress_bar.empty()
                    status_text.empty()
        
        with col2:
            st.subheader("📈 系统统计")
            
            try:
                stores_count = db['stores'].count_documents({})
                reports_count = db['reports'].count_documents({})
                permissions_count = db['permissions'].count_documents({})
                
                st.metric("🏪 门店总数", stores_count)
                st.metric("📋 报表总数", reports_count)
                st.metric("🔑 权限总数", permissions_count)
                
                # 当月统计
                current_month_reports = db['reports'].count_documents({
                    'report_month': datetime.now().strftime("%Y-%m")
                })
                st.metric("📅 本月报表", current_month_reports)
                
                st.subheader("🏪 门店管理")
                if st.button("查看门店列表"):
                    stores = list(db['stores'].find({}, {'store_name': 1, 'store_code': 1, 'region': 1}))
                    if stores:
                        stores_df = pd.DataFrame(stores)
                        st.dataframe(stores_df[['store_name', 'store_code', 'region']], use_container_width=True)
                    else:
                        st.info("暂无门店数据")
                        
            except Exception as e:
                st.error(f"获取统计失败: {e}")
            
            st.markdown("---")
            if st.button("退出管理员登录", type="secondary"):
                st.session_state.admin_authenticated = False
                st.rerun()
    
    except Exception as e:
        st.error(f"初始化上传器失败: {e}")

def create_permission_app():
    """权限管理应用"""
    st.title("👥 权限管理系统")
    
    db_manager = get_db_manager()
    if not db_manager.is_connected():
        st.error("数据库连接失败，请检查配置")
        return
    
    # 管理员验证
    if 'perm_admin_authenticated' not in st.session_state:
        st.session_state.perm_admin_authenticated = False
    
    if not st.session_state.perm_admin_authenticated:
        st.subheader("🔐 管理员登录")
        
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            password = st.text_input("管理员密码", type="password", key="perm_pass")
            
            if st.button("登录", use_container_width=True, key="perm_login"):
                if password == ConfigManager.get_admin_password():
                    st.session_state.perm_admin_authenticated = True
                    st.success("管理员登录成功！")
                    st.rerun()
                else:
                    st.error("密码错误")
        return
    
    db = db_manager.get_database()
    
    try:
        permission_manager = PermissionManager(db)
        
        # 标签页
        tab1, tab2 = st.tabs(["📤 上传权限表", "📋 权限配置"])
        
        with tab1:
            st.subheader("上传权限表")
            st.info("上传包含查询编号和门店名称对应关系的Excel或CSV文件")
            
            uploaded_file = st.file_uploader(
                "选择权限表文件",
                type=['xlsx', 'xls', 'csv'],
                help="文件应包含查询编号和门店名称两列，系统会自动识别列名"
            )
            
            if uploaded_file is not None:
                try:
                    if uploaded_file.name.endswith('.csv'):
                        preview_df = pd.read_csv(uploaded_file)
                    else:
                        preview_df = pd.read_excel(uploaded_file)
                    
                    st.subheader("文件预览")
                    st.dataframe(preview_df.head(10))
                    
                    if st.button("开始上传", type="primary"):
                        with st.spinner("正在处理权限表..."):
                            uploaded_file.seek(0)
                            result = permission_manager.upload_permission_table(uploaded_file)
                        
                        if result["success"]:
                            st.success("权限表上传成功！")
                            
                            if "detected_columns" in result:
                                cols = result["detected_columns"]
                                st.info(f"✅ 自动识别列名：查询编号列='{cols['query_code']}'，门店名称列='{cols['store_name']}'")
                            
                            col1, col2, col3, col4 = st.columns(4)
                            with col1:
                                st.metric("📊 处理记录数", result["processed"])
                            with col2:
                                st.metric("✅ 成功上传", result["created"] + result["updated"])
                            with col3:
                                st.metric("🆕 新建权限", result["created"])
                            with col4:
                                st.metric("🔄 更新权限", result["updated"])
                            
                            if result["errors"]:
                                st.warning(f"⚠️ 处理过程中出现 {len(result['errors'])} 个问题：")
                                for error in result["errors"]:
                                    st.write(f"• {error}")
                            else:
                                st.success("🎉 所有记录处理成功，无错误！")
                        else:
                            st.error(f"❌ 上传失败: {result['message']}")
                            
                except Exception as e:
                    st.error(f"文件预览失败: {e}")
        
        with tab2:
            st.subheader("当前权限配置")
            
            permissions = permission_manager.get_all_permissions()
            
            if permissions:
                for perm in permissions:
                    with st.expander(f"查询编号: {perm['query_code']} → {perm['store_name']}"):
                        st.write(f"**门店名称:** {perm['store_name']}")
                        st.write(f"**门店ID:** {perm['store_id']}")
                        st.write(f"**门店代码:** {perm.get('store_code', 'N/A')}")
                        st.write(f"**创建时间:** {perm.get('created_at', 'N/A')}")
                        st.write(f"**更新时间:** {perm.get('updated_at', 'N/A')}")
                        
                        if st.button(f"删除权限", key=f"delete_{perm['query_code']}"):
                            if permission_manager.delete_permission(perm['query_code']):
                                st.success("权限配置已删除")
                                st.rerun()
                            else:
                                st.error("删除失败")
            else:
                st.info("暂无权限配置")
            
            # 文件格式说明
            st.markdown("---")
            st.subheader("📋 文件格式说明")
            st.markdown("""
            **权限表文件要求：**
            - 📄 支持Excel(.xlsx/.xls)和CSV格式
            - 📊 至少包含两列数据：查询编号和门店名称
            - 🔍 系统会自动识别列名（支持中英文）
            - 🔗 一个查询编号只对应一个门店（一对一关系）
            - 🔄 如果查询编号重复，新记录会覆盖旧记录
            - 🏪 如果门店不存在，系统会自动创建
            
            **示例格式：**
            ```
            查询编号    门店名称
            QC001      犀牛百货滨江店
            QC002      犀牛百货西湖店
            QC003      犀牛百货萧山店
            ```
            """)
        
        st.markdown("---")
        if st.button("退出管理员登录", type="secondary", key="perm_logout"):
            st.session_state.perm_admin_authenticated = False
            st.rerun()
    
    except Exception as e:
        st.error(f"初始化权限管理器失败: {e}")

def main():
    """主应用入口"""
    
    # 侧边栏
    with st.sidebar:
        st.title("🏪 门店报表系统")
        st.caption("数据查询平台")
        
        app_choice = st.selectbox(
            "选择功能模块",
            ["门店查询系统", "批量上传系统", "权限管理系统"],
            index=0
        )
        
        st.markdown("---")
        st.markdown("### 🔗 连接状态")
        
        # 检查数据库连接
        db_manager = get_db_manager()
        if db_manager.is_connected():
            st.success("✅ 系统正常")
        else:
            st.error("❌ 连接异常")
    
    # 主界面
    try:
        if app_choice == "门店查询系统":
            create_query_app()
        elif app_choice == "批量上传系统":
            create_upload_app()
        elif app_choice == "权限管理系统":
            create_permission_app()
    except Exception as e:
        st.error(f"应用运行出错: {e}")
        with st.expander("查看详细错误信息"):
            st.code(traceback.format_exc())

if __name__ == "__main__":
    main()
