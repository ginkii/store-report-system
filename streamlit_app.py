# streamlit_app.py - Streamlit Cloud完整单文件版本
"""
门店报表查询系统 - 完整功能单文件部署版本
包含查询、上传、权限管理功能
"""

import streamlit as st
import traceback
import os
import pandas as pd
import numpy as np
import time
from datetime import datetime
from typing import Dict, List, Optional, Any
import hashlib

# 页面配置
st.set_page_config(
    page_title="门店报表系统",
    page_icon="🏪",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 配置管理
class ConfigManager:
    """配置管理器"""
    
    @staticmethod
    def get_mongodb_config():
        """获取MongoDB配置"""
        try:
            # 优先从secrets获取
            if hasattr(st, 'secrets') and 'mongodb' in st.secrets:
                return {
                    'uri': st.secrets["mongodb"]["uri"],
                    'database_name': st.secrets["mongodb"]["database_name"]
                }
        except Exception:
            pass
        
        # 环境变量回退
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
            st.error(f"数据库连接失败: {e}")
            self.db = None
            self.client = None
    
    def _create_indexes(self):
        """创建索引"""
        if not self.db:
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
    def create_report_document(store_data: Dict, report_month: str, excel_data: List[Dict], **kwargs) -> Dict:
        """创建标准报表文档"""
        return {
            'store_id': store_data['_id'],
            'store_code': store_data['store_code'],
            'store_name': store_data['store_name'],
            'report_month': report_month,
            'sheet_name': kwargs.get('sheet_name', store_data['store_name']),
            'raw_excel_data': excel_data,
            'financial_data': kwargs.get('financial_data', {}),
            'created_at': kwargs.get('created_at', datetime.now()),
            'updated_at': datetime.now(),
            'uploaded_by': kwargs.get('uploaded_by', 'system')
        }
    
    @staticmethod
    def dataframe_to_dict_list(df: pd.DataFrame) -> List[Dict]:
        """将DataFrame转换为字典列表"""
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
                    row_dict[col_key] = str(value)
            result.append(row_dict)
        return result

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
        if not db:
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
    
    def process_excel_file(self, file_buffer, report_month: str, progress_callback=None) -> Dict:
        """处理Excel文件并上传报表数据"""
        start_time = time.time()
        result = {
            'success_count': 0,
            'failed_count': 0,
            'errors': [],
            'processed_stores': [],
            'failed_stores': [],
            'total_time': 0
        }
        
        try:
            if progress_callback:
                progress_callback(10, "正在读取Excel文件...")
            
            # 读取Excel文件
            excel_data = pd.read_excel(file_buffer, sheet_name=None, engine='openpyxl', header=None)
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
                    
                    # 处理数据
                    df_cleaned = df.dropna(axis=1, how='all')
                    if df_cleaned.empty:
                        result['failed_stores'].append({
                            'store_name': sheet_name,
                            'reason': '数据为空'
                        })
                        result['failed_count'] += 1
                        continue
                    
                    # 转换数据格式
                    excel_data_dict = ReportModel.dataframe_to_dict_list(df_cleaned)
                    financial_data = self._extract_financial_data(df_cleaned)
                    
                    # 创建报表文档
                    report_data = ReportModel.create_report_document(
                        store_data=store,
                        report_month=report_month,
                        excel_data=excel_data_dict,
                        sheet_name=sheet_name,
                        financial_data=financial_data,
                        uploaded_by='bulk_upload'
                    )
                    
                    # 保存到数据库
                    existing_report = self.reports_collection.find_one({
                        'store_id': store['_id'],
                        'report_month': report_month
                    })
                    
                    if existing_report:
                        self.reports_collection.replace_one(
                            {'_id': existing_report['_id']},
                            report_data
                        )
                    else:
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
    
    def _extract_financial_data(self, df: pd.DataFrame) -> Dict:
        """从DataFrame中提取财务数据"""
        financial_data = {
            'revenue': {},
            'cost': {},
            'profit': {},
            'receivables': {},
            'other_metrics': {}
        }
        
        try:
            # 提取第41行第2个合计列的应收未收金额
            if len(df) >= 41:
                target_row_index = 40
                
                # 查找合计列
                total_col_indices = []
                if len(df) > 0:
                    for col_idx in range(len(df.columns)):
                        try:
                            header_value = str(df.iloc[0, col_idx]) if not pd.isna(df.iloc[0, col_idx]) else ""
                            if '合计' in header_value or 'total' in header_value.lower():
                                total_col_indices.append(col_idx)
                        except:
                            continue
                
                # 检查第41行
                if len(df) > target_row_index:
                    try:
                        first_col_value = str(df.iloc[target_row_index, 0]) if not pd.isna(df.iloc[target_row_index, 0]) else ""
                        keywords = ['总部应收未收金额', '应收未收金额', '应收-未收额', '应收未收额', '应收-未收', '应收未收']
                        
                        if any(keyword in first_col_value for keyword in keywords):
                            target_col_idx = None
                            if len(total_col_indices) >= 2:
                                target_col_idx = total_col_indices[1]
                            elif len(total_col_indices) == 1:
                                target_col_idx = total_col_indices[0]
                            
                            if target_col_idx is not None:
                                try:
                                    row_41_value = float(df.iloc[target_row_index, target_col_idx])
                                    financial_data['receivables']['net_amount'] = row_41_value
                                except:
                                    pass
                    except:
                        pass
            
            # 提取其他财务指标
            for idx, row in df.iterrows():
                try:
                    if len(row) < 2:
                        continue
                    
                    metric_name = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ""
                    if not metric_name:
                        continue
                    
                    # 查找数值
                    value = None
                    for col_idx in range(1, len(row)):
                        try:
                            if pd.notna(row.iloc[col_idx]):
                                value = float(row.iloc[col_idx])
                                break
                        except:
                            continue
                    
                    if value is None:
                        value = 0
                    
                    # 分类存储
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
                    
                    financial_data['other_metrics'][f"{idx+1}行_{metric_name}"] = value
                
                except:
                    continue
            
        except Exception as e:
            st.error(f"提取财务数据时出错: {e}")
        
        return financial_data

# 权限管理器
class PermissionManager:
    """权限管理器"""
    
    def __init__(self, db):
        if not db:
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
                # 应收未收看板
                st.subheader("💰 总部应收未收金额")
                
                try:
                    latest_report = reports[0]
                    receivables = latest_report.get('financial_data', {}).get('receivables', {})
                    amount = receivables.get('net_amount', 0)
                    
                    if amount > 0:
                        st.error(f"💰 门店应付: ¥{amount:,.2f}")
                    elif amount < 0:
                        st.success(f"💚 总部应退: ¥{abs(amount):,.2f}")
                    else:
                        st.info("✅ 已结清: ¥0.00")
                except Exception:
                    st.info("暂无应收数据")
                
                # 报表数据
                st.subheader("📋 报表数据")
                
                try:
                    latest_report = reports[0]
                    raw_data = latest_report.get('raw_excel_data', [])
                    
                    if raw_data and len(raw_data) > 0:
                        # 重建DataFrame
                        max_cols = max(len(row) for row in raw_data) if raw_data else 0
                        
                        data_matrix = []
                        for row_data in raw_data:
                            row_values = []
                            for col_idx in range(max_cols):
                                col_key = f"col_{col_idx}"
                                value = row_data.get(col_key, "") if isinstance(row_data, dict) else ""
                                row_values.append(value)
                            data_matrix.append(row_values)
                        
                        if len(data_matrix) > 1:
                            # 使用第一行作为列名
                            df = pd.DataFrame(data_matrix[1:], columns=data_matrix[0])
                            df = df.fillna('')
                            st.dataframe(df, use_container_width=True)
                        else:
                            st.info("暂无详细数据")
                    else:
                        st.info("暂无报表数据")
                        
                except Exception as e:
                    st.error(f"数据显示错误: {e}")
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
                        progress_callback=update_progress
                    )
                    
                    # 显示结果
                    st.subheader("📊 上传结果")
                    
                    col_success, col_failed, col_time = st.columns(3)
                    with col_success:
                        st.metric("成功上传", result['success_count'])
                    with col_failed:
                        st.metric("失败数量", result['failed_count'])
                    with col_time:
                        st.metric("耗时(秒)", f"{result['total_time']:.2f}")
                    
                    if result['processed_stores']:
                        st.subheader("✅ 成功上传的门店")
                        success_df = pd.DataFrame(result['processed_stores'])
                        st.dataframe(success_df, use_container_width=True)
                    
                    if result['failed_stores']:
                        st.subheader("❌ 上传失败")
                        failed_df = pd.DataFrame(result['failed_stores'])
                        st.dataframe(failed_df, use_container_width=True)
                    
                    progress_bar.empty()
                    status_text.empty()
        
        with col2:
            st.subheader("📈 系统统计")
            
            try:
                stores_count = db['stores'].count_documents({})
                reports_count = db['reports'].count_documents({})
                permissions_count = db['permissions'].count_documents({})
                
                st.metric("门店数量", stores_count)
                st.metric("报表数量", reports_count)
                st.metric("权限数量", permissions_count)
                
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
        
        app_choice = st.selectbox(
            "选择功能模块",
            ["门店查询系统", "批量上传系统", "权限管理系统"],
            index=0
        )
        
        st.markdown("---")
        st.markdown("### 📊 系统状态")
        
        # 检查数据库连接
        db_manager = get_db_manager()
        if db_manager.is_connected():
            st.success("✅ 数据库已连接")
        else:
            st.error("❌ 数据库连接失败")
            st.info("请检查MongoDB配置")
    
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
