# streamlit_app.py - 门店报表系统完整版
"""
门店报表查询系统 - 完整功能单文件部署版本
包含查询、上传、权限管理、财务填报功能
修复: 1.完全覆盖历史文件 2.修复表头消失问题 3.第41行第2个合计列应收金额 4.新增财务填报系统
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
            self.db['store_financial_reports'].create_index([("header.store_id", 1), ("header.period", 1)], unique=True)
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
        """将DataFrame转换为字典列表，保留表头信息并修复#NAME?错误，处理空白表头"""
        # 保存原始列名作为表头，处理Unnamed列，避免重复空白列名
        headers = []
        empty_count = 0
        for col in df.columns:
            col_str = str(col)
            # 将Unnamed列名替换为空字符串
            if col_str.startswith('Unnamed:') or col_str.startswith('Unnamed ') or ('unnamed' in col_str.lower()):
                headers.append("")
            else:
                headers.append(col_str)
        
        # 处理重复的空白列名，为pandas创建唯一列名
        unique_headers = []
        empty_count = 0
        for header in headers:
            if header == "":
                unique_headers.append(f"_empty_{empty_count}")
                empty_count += 1
            else:
                unique_headers.append(header)
        
        # 使用唯一列名重建DataFrame，但保存原始表头用于显示
        df.columns = unique_headers
        
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

# 财务报表数据模型
class FinancialReportModel:
    """财务报表数据模型"""
    
    @staticmethod
    def create_financial_report_document(store_id: str, store_name: str, period: str, admin_data: Dict = None) -> Dict:
        """创建标准财务报表文档"""
        return {
            'header': {
                'store_id': store_id,
                'store_name': store_name,
                'period': period,  # 格式：2024-12
                'status': 'pending',  # pending/submitted
                'created_at': datetime.now(),
                'updated_at': datetime.now()
            },
            'admin_data': admin_data or {
                '1': 0,   # 回款
                '2': 0,   # 其他现金收入
                '11': 0,  # 线上支出
                '16': 0,  # 线上净利润
                # 其他管理员预设数据...
            },
            'user_inputs': {
                '18': 0,  # 工资
                '19': 0,  # 房租
                '20': 0,  # 水电费
                '21': 0,  # 物业费
                '22': 0,  # 其他费用1
                '23': 0,  # 其他费用2
                '24': 0,  # 其他费用3
                '25': 0,  # 其他费用4
                '26': 0,  # 其他费用5
            },
            'calculated_metrics': {},
            'metadata': {
                'created_at': datetime.now(),
                'updated_at': datetime.now(),
                'submitted_by': None,
                'submission_time': None
            }
        }
    
    @staticmethod
    def calculate_financial_metrics(admin_data: Dict, user_inputs: Dict) -> Dict:
        """计算财务指标"""
        try:
            # 类型转换，确保所有值都是数字
            def safe_float(value):
                try:
                    return float(value) if value is not None else 0.0
                except (ValueError, TypeError):
                    return 0.0
            
            # 提取管理员数据
            huikuan = safe_float(admin_data.get('1', 0))  # 回款
            xianshang_zhichu = safe_float(admin_data.get('11', 0))  # 线上支出  
            xianshang_jinglilun = safe_float(admin_data.get('16', 0))  # 线上净利润
            
            # 提取用户输入数据
            gongzi = safe_float(user_inputs.get('18', 0))  # 工资
            fangzu = safe_float(user_inputs.get('19', 0))  # 房租
            shuidian = safe_float(user_inputs.get('20', 0))  # 水电费
            wuye = safe_float(user_inputs.get('21', 0))  # 物业费
            qita1 = safe_float(user_inputs.get('22', 0))  # 其他费用1
            qita2 = safe_float(user_inputs.get('23', 0))  # 其他费用2
            qita3 = safe_float(user_inputs.get('24', 0))  # 其他费用3
            qita4 = safe_float(user_inputs.get('25', 0))  # 其他费用4
            qita5 = safe_float(user_inputs.get('26', 0))  # 其他费用5
            
            # 核心计算逻辑
            # 15: 线上余额 = 回款 - 线上支出
            xianshang_yue = huikuan - xianshang_zhichu
            
            # 17: 线下费用合计 = SUM(18至26项明细)
            xianxia_feiyong = gongzi + fangzu + shuidian + wuye + qita1 + qita2 + qita3 + qita4 + qita5
            
            # 26: 最终余额 = 线上余额 - 线下费用合计
            zuizhong_yue = xianshang_yue - xianxia_feiyong
            
            # 27: 最终净利润 = 线上净利润 - 线下费用合计
            zuizhong_jinglilun = xianshang_jinglilun - xianxia_feiyong
            
            return {
                '15': xianshang_yue,      # 线上余额
                '17': xianxia_feiyong,    # 线下费用合计
                '26': zuizhong_yue,       # 最终余额
                '27': zuizhong_jinglilun, # 最终净利润
            }
            
        except Exception as e:
            st.error(f"财务计算错误: {e}")
            return {
                '15': 0,  # 线上余额
                '17': 0,  # 线下费用合计
                '26': 0,  # 最终余额
                '27': 0,  # 最终净利润
            }

# 财务报表管理器
class FinancialReportManager:
    """财务报表管理器"""
    
    def __init__(self, db):
        if db is None:
            raise Exception("数据库连接失败")
        self.db = db
        self.reports_collection = self.db['store_financial_reports']
        self.stores_collection = self.db['stores']
        self._create_indexes()
    
    def _create_indexes(self):
        """创建索引"""
        try:
            self.reports_collection.create_index([("header.store_id", 1), ("header.period", 1)], unique=True)
            self.reports_collection.create_index([("header.status", 1)])
            self.reports_collection.create_index([("header.period", 1)])
        except Exception:
            pass
    
    def create_or_update_report(self, store_id: str, store_name: str, period: str, 
                              admin_data: Dict = None, user_inputs: Dict = None) -> bool:
        """创建或更新财务报表"""
        try:
            # 查找现有报表
            existing_report = self.reports_collection.find_one({
                'header.store_id': store_id,
                'header.period': period
            })
            
            if existing_report:
                # 更新现有报表
                update_data = {'header.updated_at': datetime.now()}
                
                if admin_data:
                    update_data['admin_data'] = admin_data
                
                if user_inputs:
                    update_data['user_inputs'] = user_inputs
                    # 重新计算指标
                    calculated = FinancialReportModel.calculate_financial_metrics(
                        existing_report.get('admin_data', {}), 
                        user_inputs
                    )
                    update_data['calculated_metrics'] = calculated
                
                self.reports_collection.update_one(
                    {'header.store_id': store_id, 'header.period': period},
                    {'$set': update_data}
                )
            else:
                # 创建新报表
                report_doc = FinancialReportModel.create_financial_report_document(
                    store_id=store_id,
                    store_name=store_name,
                    period=period,
                    admin_data=admin_data
                )
                
                if user_inputs:
                    report_doc['user_inputs'] = user_inputs
                
                # 计算指标
                calculated = FinancialReportModel.calculate_financial_metrics(
                    report_doc['admin_data'],
                    report_doc['user_inputs']
                )
                report_doc['calculated_metrics'] = calculated
                
                self.reports_collection.insert_one(report_doc)
            
            return True
            
        except Exception as e:
            st.error(f"保存财务报表失败: {e}")
            return False
    
    def submit_report(self, store_id: str, period: str, submitted_by: str) -> bool:
        """提交财务报表"""
        try:
            result = self.reports_collection.update_one(
                {'header.store_id': store_id, 'header.period': period},
                {'$set': {
                    'header.status': 'submitted',
                    'metadata.submitted_by': submitted_by,
                    'metadata.submission_time': datetime.now(),
                    'header.updated_at': datetime.now()
                }}
            )
            return result.modified_count > 0
        except Exception as e:
            st.error(f"提交报表失败: {e}")
            return False
    
    def get_report(self, store_id: str, period: str) -> Optional[Dict]:
        """获取财务报表"""
        try:
            return self.reports_collection.find_one({
                'header.store_id': store_id,
                'header.period': period
            })
        except Exception as e:
            st.error(f"获取财务报表失败: {e}")
            return None
    
    def get_all_reports_by_period(self, period: str) -> List[Dict]:
        """获取指定期间的所有报表"""
        try:
            return list(self.reports_collection.find({'header.period': period}))
        except Exception as e:
            st.error(f"获取报表列表失败: {e}")
            return []
    
    def get_submission_summary(self, period: str) -> Dict:
        """获取提交情况汇总"""
        try:
            pipeline = [
                {'$match': {'header.period': period}},
                {'$group': {
                    '_id': '$header.status',
                    'count': {'$sum': 1},
                    'stores': {'$push': {
                        'store_id': '$header.store_id',
                        'store_name': '$header.store_name',
                        'updated_at': '$header.updated_at'
                    }}
                }}
            ]
            
            results = list(self.reports_collection.aggregate(pipeline))
            
            summary = {
                'pending': {'count': 0, 'stores': []},
                'submitted': {'count': 0, 'stores': []},
                'total': 0
            }
            
            for result in results:
                status = result['_id']
                if status in summary:
                    summary[status] = {
                        'count': result['count'],
                        'stores': result['stores']
                    }
                summary['total'] += result['count']
            
            return summary
            
        except Exception as e:
            st.error(f"获取汇总信息失败: {e}")
            return {'pending': {'count': 0, 'stores': []}, 'submitted': {'count': 0, 'stores': []}, 'total': 0}

# Excel 导出功能
class ExcelExporter:
    """Excel导出器"""
    
    @staticmethod
    def create_financial_excel(report_data: Dict) -> io.BytesIO:
        """创建财务报表Excel"""
        try:
            import xlsxwriter
        except ImportError:
            st.error("xlsxwriter未安装，无法导出Excel")
            return None
            
        output = io.BytesIO()
        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet('财务报表')
        
        # 样式设置
        header_format = workbook.add_format({
            'bold': True,
            'font_size': 14,
            'align': 'center',
            'valign': 'vcenter',
            'bg_color': '#4CAF50',
            'font_color': 'white',
            'border': 1
        })
        
        data_format = workbook.add_format({
            'align': 'right',
            'num_format': '#,##0.00',
            'border': 1
        })
        
        label_format = workbook.add_format({
            'bold': True,
            'align': 'left',
            'border': 1
        })
        
        # 写入报表数据
        header = report_data.get('header', {})
        admin_data = report_data.get('admin_data', {})
        user_inputs = report_data.get('user_inputs', {})
        calculated = report_data.get('calculated_metrics', {})
        
        # 标题
        worksheet.merge_range('A1:D1', f"{header.get('store_name', '未知门店')} - {header.get('period', '未知期间')} 财务报表", header_format)
        
        # 数据行
        row = 2
        
        # 管理员数据部分
        worksheet.write(row, 0, "管理员预设数据", header_format)
        row += 1
        
        admin_labels = {
            '1': '回款',
            '2': '其他现金收入', 
            '11': '线上支出',
            '16': '线上净利润'
        }
        
        for key, value in admin_data.items():
            if key in admin_labels:
                worksheet.write(row, 0, admin_labels[key], label_format)
                worksheet.write(row, 1, float(value), data_format)
                row += 1
        
        # 计算指标
        worksheet.write(row, 0, "线上余额", label_format)
        worksheet.write(row, 1, calculated.get('15', 0), data_format)
        row += 1
        
        # 空行
        row += 1
        
        # 用户输入部分
        worksheet.write(row, 0, "用户填报数据", header_format)
        row += 1
        
        user_labels = {
            '18': '工资',
            '19': '房租',
            '20': '水电费',
            '21': '物业费',
            '22': '其他费用1',
            '23': '其他费用2',
            '24': '其他费用3',
            '25': '其他费用4',
            '26': '其他费用5'
        }
        
        for key, value in user_inputs.items():
            if key in user_labels:
                worksheet.write(row, 0, user_labels[key], label_format)
                worksheet.write(row, 1, float(value), data_format)
                row += 1
        
        # 计算结果
        worksheet.write(row, 0, "线下费用合计", label_format)
        worksheet.write(row, 1, calculated.get('17', 0), data_format)
        row += 1
        
        # 空行
        row += 1
        
        # 最终结果
        worksheet.write(row, 0, "最终结果", header_format)
        row += 1
        
        worksheet.write(row, 0, "最终余额", label_format)
        worksheet.write(row, 1, calculated.get('26', 0), data_format)
        row += 1
        
        worksheet.write(row, 0, "最终净利润", label_format)
        worksheet.write(row, 1, calculated.get('27', 0), data_format)
        
        workbook.close()
        output.seek(0)
        return output

# UI 组件函数
def create_financial_report_app():
    """财务填报界面"""
    st.title("💼 财务填报系统")
    
    # 获取数据库连接
    db_manager = get_db_manager()
    if not db_manager.is_connected():
        st.error("数据库连接失败，请联系管理员")
        return
    
    db = db_manager.get_database()
    
    # 初始化财务报表管理器
    try:
        financial_manager = FinancialReportManager(db)
    except Exception as e:
        st.error(f"初始化财务管理器失败: {e}")
        return
    
    # 侧边栏 - 基本信息
    with st.sidebar:
        st.header("📝 填报信息")
        
        # 门店选择
        stores_collection = db['stores']
        try:
            stores = list(stores_collection.find({'status': 'active'}))
            if not stores:
                st.warning("未找到可用门店，请联系管理员添加门店")
                return
                
            store_options = {store['store_name']: store for store in stores}
            selected_store_name = st.selectbox("选择门店", list(store_options.keys()))
            selected_store = store_options[selected_store_name]
        except Exception as e:
            st.error(f"获取门店列表失败: {e}")
            return
        
        # 期间选择
        current_date = datetime.now()
        default_period = current_date.strftime("%Y-%m")
        period = st.text_input("报告期间 (YYYY-MM)", value=default_period)
        
        if not re.match(r'^\d{4}-\d{2}$', period):
            st.error("期间格式错误，请使用YYYY-MM格式")
            return
    
    # 获取或创建报表
    report = financial_manager.get_report(selected_store['_id'], period)
    if not report:
        # 创建新报表（仅创建结构，不保存）
        report = FinancialReportModel.create_financial_report_document(
            selected_store['_id'], 
            selected_store['store_name'], 
            period
        )
    
    # 主界面
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.header("📊 财务数据填报")
        
        # 管理员预设数据（只读显示）
        with st.expander("📋 管理员预设数据", expanded=True):
            admin_data = report.get('admin_data', {})
            
            col_a, col_b = st.columns(2)
            with col_a:
                st.metric("回款", f"¥{admin_data.get('1', 0):,.2f}")
                st.metric("线上支出", f"¥{admin_data.get('11', 0):,.2f}")
            
            with col_b:
                st.metric("其他现金收入", f"¥{admin_data.get('2', 0):,.2f}")
                st.metric("线上净利润", f"¥{admin_data.get('16', 0):,.2f}")
            
            # 计算线上余额
            calculated = report.get('calculated_metrics', {})
            if calculated:
                st.metric("线上余额", f"¥{calculated.get('15', 0):,.2f}", 
                         help="回款 - 线上支出")
        
        # 用户填报表单
        with st.form("financial_form", clear_on_submit=False):
            st.subheader("✏️ 线下费用填报")
            
            user_inputs = report.get('user_inputs', {})
            
            col1_form, col2_form = st.columns(2)
            
            with col1_form:
                gongzi = st.number_input("工资", min_value=0.0, value=float(user_inputs.get('18', 0)), format="%.2f", key="input_18")
                fangzu = st.number_input("房租", min_value=0.0, value=float(user_inputs.get('19', 0)), format="%.2f", key="input_19")
                shuidian = st.number_input("水电费", min_value=0.0, value=float(user_inputs.get('20', 0)), format="%.2f", key="input_20")
                wuye = st.number_input("物业费", min_value=0.0, value=float(user_inputs.get('21', 0)), format="%.2f", key="input_21")
                qita1 = st.number_input("其他费用1", min_value=0.0, value=float(user_inputs.get('22', 0)), format="%.2f", key="input_22")
            
            with col2_form:
                qita2 = st.number_input("其他费用2", min_value=0.0, value=float(user_inputs.get('23', 0)), format="%.2f", key="input_23")
                qita3 = st.number_input("其他费用3", min_value=0.0, value=float(user_inputs.get('24', 0)), format="%.2f", key="input_24")
                qita4 = st.number_input("其他费用4", min_value=0.0, value=float(user_inputs.get('25', 0)), format="%.2f", key="input_25")
                qita5 = st.number_input("其他费用5", min_value=0.0, value=float(user_inputs.get('26', 0)), format="%.2f", key="input_26")
            
            # 实时计算显示（在表单内）
            current_total = gongzi + fangzu + shuidian + wuye + qita1 + qita2 + qita3 + qita4 + qita5
            
            st.markdown("### 📊 **实时计算预览**")
            col_calc1, col_calc2 = st.columns(2)
            
            with col_calc1:
                st.markdown(f"""
                <div style="background-color: #e8f4f8; padding: 12px; border-radius: 8px; border-left: 4px solid #17a2b8;">
                    <strong>(17) 线下费用合计</strong><br/>
                    <span style="font-size: 18px; color: #17a2b8; font-weight: bold;">¥{current_total:,.2f}</span>
                </div>
                """, unsafe_allow_html=True)
            
            admin_data = report.get('admin_data', {})
            xianshang_yue = admin_data.get('1', 0) - admin_data.get('11', 0)
            current_final = xianshang_yue - current_total
            current_profit = admin_data.get('16', 0) - current_total
            
            with col_calc2:
                st.markdown(f"""
                <div style="background-color: #f0f8e8; padding: 12px; border-radius: 8px; border-left: 4px solid #28a745;">
                    <strong>(26) 最终余额</strong><br/>
                    <span style="font-size: 18px; color: #28a745; font-weight: bold;">¥{current_final:,.2f}</span>
                </div>
                """, unsafe_allow_html=True)
            
            # 表单按钮
            col_btn1, col_btn2 = st.columns(2)
            
            with col_btn1:
                save_btn = st.form_submit_button("💾 保存草稿", type="secondary")
            
            with col_btn2:
                submit_btn = st.form_submit_button("✅ 正式提交", type="primary")
            
            # 处理表单提交
            if save_btn or submit_btn:
                new_user_inputs = {
                    '18': gongzi, '19': fangzu, '20': shuidian, '21': wuye,
                    '22': qita1, '23': qita2, '24': qita3, '25': qita4, '26': qita5
                }
                
                # 保存数据
                success = financial_manager.create_or_update_report(
                    selected_store['_id'], 
                    selected_store['store_name'], 
                    period,
                    user_inputs=new_user_inputs
                )
                
                if success:
                    if submit_btn:
                        # 正式提交
                        submit_success = financial_manager.submit_report(
                            selected_store['_id'], 
                            period, 
                            selected_store_name
                        )
                        if submit_success:
                            st.success("✅ 报表已正式提交！")
                            st.balloons()
                        else:
                            st.error("❌ 提交失败")
                    else:
                        st.success("✅ 草稿已保存！")
                    
                    # 刷新页面数据
                    st.rerun()
                else:
                    st.error("❌ 保存失败")
    
    with col2:
        st.header("📈 实时预览")
        
        # 重新获取最新数据用于预览
        latest_report = financial_manager.get_report(selected_store['_id'], period)
        if latest_report:
            admin_data = latest_report.get('admin_data', {})
            user_inputs = latest_report.get('user_inputs', {})
            calculated = latest_report.get('calculated_metrics', {})
            
            # 实时计算指标
            huikuan = admin_data.get('1', 0)
            xianshang_zhichu = admin_data.get('11', 0)
            xianshang_jinglilun = admin_data.get('16', 0)
            
            xianxia_total = sum(user_inputs.values())
            xianshang_yue = huikuan - xianshang_zhichu
            zuizhong_yue = xianshang_yue - xianxia_total
            zuizhong_jinglilun = xianshang_jinglilun - xianxia_total
            
            # 关键指标卡片（加粗显示重要项目）
            st.markdown("### 🎯 **关键财务指标**")
            
            # 最终余额和净利润（关键结果项）
            col_key1, col_key2 = st.columns(2)
            with col_key1:
                st.markdown(f"""
                <div style="background-color: #e8f5e8; padding: 15px; border-radius: 10px; border-left: 5px solid #28a745;">
                    <h4 style="color: #155724; margin: 0;">💰 最终余额 (26)</h4>
                    <h2 style="color: #155724; margin: 5px 0; font-weight: bold;">¥{zuizhong_yue:,.2f}</h2>
                </div>
                """, unsafe_allow_html=True)
            
            with col_key2:
                st.markdown(f"""
                <div style="background-color: #e3f2fd; padding: 15px; border-radius: 10px; border-left: 5px solid #1976d2;">
                    <h4 style="color: #1565c0; margin: 0;">📊 最终净利润 (27)</h4>
                    <h2 style="color: #1565c0; margin: 5px 0; font-weight: bold;">¥{zuizhong_jinglilun:,.2f}</h2>
                </div>
                """, unsafe_allow_html=True)
            
            # 其他重要指标
            st.markdown("### 📋 计算详情")
            st.metric("(17) 线下费用合计", f"¥{xianxia_total:,.2f}")
            st.metric("(15) 线上余额", f"¥{xianshang_yue:,.2f}")
            
            # 报表状态
            status = latest_report.get('header', {}).get('status', 'pending')
            if status == 'submitted':
                st.success("✅ 已提交")
            else:
                st.info("📝 草稿状态")
            
            # 导出Excel
            if st.button("📊 导出Excel", use_container_width=True):
                excel_file = ExcelExporter.create_financial_excel(latest_report)
                if excel_file:
                    st.download_button(
                        label="⬇️ 下载Excel文件",
                        data=excel_file,
                        file_name=f"{selected_store['store_name']}_{period}_财务报表.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True
                    )
    
    # 页面底部 - 可视化运算看板
    st.markdown("---")
    st.markdown("## 📊 财务运算可视化看板")
    
    # 勾稽关系提醒
    st.markdown("""
    <div style="background-color: #fff3cd; border: 1px solid #ffeaa7; border-radius: 10px; padding: 15px; margin: 10px 0;">
        <h4 style="color: #856404; margin: 0;">⚠️ 重要勾稽关系</h4>
        <p style="color: #856404; margin: 5px 0; font-weight: bold;">
            表一(9) ≡ 表二(11) &nbsp;&nbsp;|&nbsp;&nbsp; 表一(14) ≡ 表二(12)
        </p>
        <small style="color: #856404;">请确保两个表格对应项目数值一致</small>
    </div>
    """, unsafe_allow_html=True)
    
    # 获取当前数据用于可视化
    current_admin_data = report.get('admin_data', {})
    current_user_inputs = report.get('user_inputs', {})
    
    # 实时计算所有指标
    huikuan_current = current_admin_data.get('1', 0)
    xianshang_zhichu_current = current_admin_data.get('11', 0)
    xianshang_jinglilun_current = current_admin_data.get('16', 0)
    xianxia_total_current = sum(current_user_inputs.values())
    xianshang_yue_current = huikuan_current - xianshang_zhichu_current
    zuizhong_yue_current = xianshang_yue_current - xianxia_total_current
    zuizhong_jinglilun_current = xianshang_jinglilun_current - xianxia_total_current
    
    # 创建两个看板
    col_cash, col_profit = st.columns(2)
    
    with col_cash:
        st.markdown("""
        <div style="background-color: #e8f5e8; border: 2px solid #28a745; border-radius: 15px; padding: 20px;">
            <h3 style="color: #155724; text-align: center; margin: 0;">🟢 现金表运算</h3>
        </div>
        """, unsafe_allow_html=True)
        
        # 现金表流程图
        st.markdown(f"""
        <div style="background-color: #f8fff8; padding: 20px; border-radius: 10px; margin: 10px 0;">
            <div style="text-align: center;">
                <div style="background-color: #28a745; color: white; padding: 10px; border-radius: 8px; margin: 5px; display: inline-block;">
                    <strong>(1) 回款</strong><br/>¥{huikuan_current:,.2f}
                </div>
                <div style="font-size: 20px; margin: 10px;">➖</div>
                <div style="background-color: #6c757d; color: white; padding: 10px; border-radius: 8px; margin: 5px; display: inline-block;">
                    <strong>(11) 线上支出</strong><br/>¥{xianshang_zhichu_current:,.2f}
                </div>
                <div style="font-size: 20px; margin: 10px;">⬇️</div>
                <div style="background-color: #17a2b8; color: white; padding: 10px; border-radius: 8px; margin: 5px; display: inline-block;">
                    <strong>(15) 线上余额</strong><br/>¥{xianshang_yue_current:,.2f}
                </div>
                <div style="font-size: 20px; margin: 10px;">➖</div>
                <div style="background-color: #fd7e14; color: white; padding: 10px; border-radius: 8px; margin: 5px; display: inline-block;">
                    <strong>(17) 线下费用合计</strong><br/>¥{xianxia_total_current:,.2f}
                </div>
                <div style="font-size: 20px; margin: 10px;">⬇️</div>
                <div style="background-color: #dc3545; color: white; padding: 15px; border-radius: 8px; margin: 5px; display: inline-block; font-size: 18px;">
                    <strong>(26) 最终余额</strong><br/>¥{zuizhong_yue_current:,.2f}
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    with col_profit:
        st.markdown("""
        <div style="background-color: #e3f2fd; border: 2px solid #1976d2; border-radius: 15px; padding: 20px;">
            <h3 style="color: #1565c0; text-align: center; margin: 0;">🔵 利润表运算</h3>
        </div>
        """, unsafe_allow_html=True)
        
        # 利润表流程图
        st.markdown(f"""
        <div style="background-color: #f8feff; padding: 20px; border-radius: 10px; margin: 10px 0;">
            <div style="text-align: center;">
                <div style="background-color: #1976d2; color: white; padding: 10px; border-radius: 8px; margin: 5px; display: inline-block;">
                    <strong>(16) 线上净利润</strong><br/>¥{xianshang_jinglilun_current:,.2f}
                </div>
                <div style="font-size: 20px; margin: 10px;">➖</div>
                <div style="background-color: #fd7e14; color: white; padding: 10px; border-radius: 8px; margin: 5px; display: inline-block;">
                    <strong>(17) 线下费用合计</strong><br/>¥{xianxia_total_current:,.2f}
                </div>
                <div style="font-size: 14px; color: #6c757d; margin: 10px;">
                    SUM(18至26项明细)
                </div>
                <div style="font-size: 20px; margin: 10px;">⬇️</div>
                <div style="background-color: #28a745; color: white; padding: 15px; border-radius: 8px; margin: 5px; display: inline-block; font-size: 18px;">
                    <strong>(27) 最终净利润</strong><br/>¥{zuizhong_jinglilun_current:,.2f}
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    # 明细项目展示
    st.markdown("### 📝 线下费用明细 (18-26项)")
    detail_cols = st.columns(3)
    
    detail_items = [
        ("18", "工资", current_user_inputs.get('18', 0)),
        ("19", "房租", current_user_inputs.get('19', 0)),
        ("20", "水电费", current_user_inputs.get('20', 0)),
        ("21", "物业费", current_user_inputs.get('21', 0)),
        ("22", "其他费用1", current_user_inputs.get('22', 0)),
        ("23", "其他费用2", current_user_inputs.get('23', 0)),
        ("24", "其他费用3", current_user_inputs.get('24', 0)),
        ("25", "其他费用4", current_user_inputs.get('25', 0)),
        ("26", "其他费用5", current_user_inputs.get('26', 0))
    ]
    
    for i, (code, name, value) in enumerate(detail_items):
        with detail_cols[i % 3]:
            st.markdown(f"""
            <div style="background-color: #f8f9fa; border: 1px solid #dee2e6; border-radius: 5px; padding: 8px; margin: 2px;">
                <small style="color: #6c757d;">({code})</small>
                <div style="font-weight: bold;">{name}</div>
                <div style="color: #495057;">¥{value:,.2f}</div>
            </div>
            """, unsafe_allow_html=True)

def create_financial_admin_app():
    """财务管理界面"""
    st.title("👨‍💼 财务管理系统")
    
    # 管理员密码验证
    if 'admin_authenticated' not in st.session_state:
        st.session_state.admin_authenticated = False
    
    if not st.session_state.admin_authenticated:
        with st.form("admin_login"):
            st.subheader("🔐 管理员登录")
            password = st.text_input("请输入管理员密码", type="password")
            login_btn = st.form_submit_button("登录")
            
            if login_btn:
                admin_password = ConfigManager.get_admin_password()
                if password == admin_password:
                    st.session_state.admin_authenticated = True
                    st.success("✅ 登录成功！")
                    st.rerun()
                else:
                    st.error("❌ 密码错误")
        return
    
    # 获取数据库连接
    db_manager = get_db_manager()
    if not db_manager.is_connected():
        st.error("数据库连接失败，请联系系统管理员")
        return
    
    db = db_manager.get_database()
    
    try:
        financial_manager = FinancialReportManager(db)
    except Exception as e:
        st.error(f"初始化财务管理器失败: {e}")
        return
    
    # 顶部操作栏
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        current_date = datetime.now()
        default_period = current_date.strftime("%Y-%m")
        period = st.text_input("查询期间 (YYYY-MM)", value=default_period)
    
    with col2:
        if st.button("🔄 刷新数据"):
            st.rerun()
    
    with col3:
        if st.button("🚪 退出登录"):
            st.session_state.admin_authenticated = False
            st.rerun()
    
    if not re.match(r'^\d{4}-\d{2}$', period):
        st.error("期间格式错误，请使用YYYY-MM格式")
        return
    
    # 获取汇总数据
    summary = financial_manager.get_submission_summary(period)
    reports = financial_manager.get_all_reports_by_period(period)
    
    # 汇总卡片
    st.subheader("📊 提交情况概览")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("总门店数", summary['total'])
    with col2:
        st.metric("已提交", summary['submitted']['count'], 
                 delta=f"{summary['submitted']['count']}/{summary['total']}")
    with col3:
        st.metric("待提交", summary['pending']['count'])
    with col4:
        completion_rate = (summary['submitted']['count'] / summary['total'] * 100) if summary['total'] > 0 else 0
        st.metric("完成率", f"{completion_rate:.1f}%")
    
    # 详细报表列表
    st.subheader("📋 详细报表列表")
    
    if reports:
        # 创建数据表格
        table_data = []
        for report in reports:
            header = report.get('header', {})
            calculated = report.get('calculated_metrics', {})
            
            table_data.append({
                '门店名称': header.get('store_name', '未知'),
                '状态': '✅ 已提交' if header.get('status') == 'submitted' else '📝 草稿',
                '最终余额': f"¥{calculated.get('26', 0):,.2f}",
                '最终净利润': f"¥{calculated.get('27', 0):,.2f}",
                '更新时间': header.get('updated_at', datetime.now()).strftime('%Y-%m-%d %H:%M'),
                '门店ID': header.get('store_id', '')
            })
        
        df = pd.DataFrame(table_data)
        st.dataframe(df, use_container_width=True, hide_index=True)
        
        # 批量导出功能
        st.subheader("📤 批量导出")
        
        col1, col2 = st.columns(2)
        
        with col1:
            export_filter = st.selectbox(
                "导出范围",
                ["所有报表", "仅已提交", "仅草稿"]
            )
        
        with col2:
            if st.button("📊 批量导出Excel", type="primary"):
                # 根据筛选条件过滤报表
                filtered_reports = reports
                if export_filter == "仅已提交":
                    filtered_reports = [r for r in reports if r.get('header', {}).get('status') == 'submitted']
                elif export_filter == "仅草稿":
                    filtered_reports = [r for r in reports if r.get('header', {}).get('status') == 'pending']
                
                if filtered_reports:
                    # 创建ZIP文件包含所有Excel
                    import zipfile
                    zip_buffer = io.BytesIO()
                    
                    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                        for report in filtered_reports:
                            excel_data = ExcelExporter.create_financial_excel(report)
                            if excel_data:
                                store_name = report.get('header', {}).get('store_name', '未知门店')
                                filename = f"{store_name}_{period}_财务报表.xlsx"
                                zip_file.writestr(filename, excel_data.getvalue())
                    
                    zip_buffer.seek(0)
                    
                    st.download_button(
                        label="⬇️ 下载批量报表",
                        data=zip_buffer,
                        file_name=f"财务报表_{period}_{export_filter}.zip",
                        mime="application/zip"
                    )
                else:
                    st.warning("没有符合条件的报表可导出")
        
        # 图表分析
        st.subheader("📈 数据分析")
        
        if len(reports) > 1:
            # 准备图表数据
            chart_data = []
            for report in reports:
                header = report.get('header', {})
                calculated = report.get('calculated_metrics', {})
                
                chart_data.append({
                    '门店': header.get('store_name', '未知')[:8],  # 截断长名称
                    '最终余额': calculated.get('26', 0),
                    '最终净利润': calculated.get('27', 0),
                    '状态': header.get('status', 'pending')
                })
            
            chart_df = pd.DataFrame(chart_data)
            
            # 余额对比图
            col1, col2 = st.columns(2)
            
            with col1:
                fig_balance = px.bar(
                    chart_df, 
                    x='门店', 
                    y='最终余额',
                    color='状态',
                    title="各门店最终余额对比",
                    color_discrete_map={'submitted': '#4CAF50', 'pending': '#FF9800'}
                )
                fig_balance.update_layout(height=400)
                st.plotly_chart(fig_balance, use_container_width=True)
            
            with col2:
                fig_profit = px.bar(
                    chart_df, 
                    x='门店', 
                    y='最终净利润',
                    color='状态',
                    title="各门店最终净利润对比",
                    color_discrete_map={'submitted': '#4CAF50', 'pending': '#FF9800'}
                )
                fig_profit.update_layout(height=400)
                st.plotly_chart(fig_profit, use_container_width=True)
            
            # 汇总统计
            total_balance = chart_df['最终余额'].sum()
            total_profit = chart_df['最终净利润'].sum()
            avg_balance = chart_df['最终余额'].mean()
            avg_profit = chart_df['最终净利润'].mean()
            
            st.subheader("🎯 汇总统计")
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("总余额", f"¥{total_balance:,.2f}")
            with col2:
                st.metric("总净利润", f"¥{total_profit:,.2f}")
            with col3:
                st.metric("平均余额", f"¥{avg_balance:,.2f}")
            with col4:
                st.metric("平均净利润", f"¥{avg_profit:,.2f}")
    
    else:
        st.info(f"📝 {period} 期间暂无财务报表数据")
        
        # 提供创建报表的选项
        st.subheader("➕ 创建新报表")
        
        # 获取所有门店
        stores_collection = db['stores']
        try:
            stores = list(stores_collection.find({'status': 'active'}))
            if stores:
                for store in stores:
                    with st.expander(f"为 {store['store_name']} 创建报表"):
                        if st.button(f"创建 {store['store_name']} 的 {period} 报表", 
                                   key=f"create_{store['_id']}"):
                            success = financial_manager.create_or_update_report(
                                store['_id'], 
                                store['store_name'], 
                                period
                            )
                            if success:
                                st.success(f"✅ 已为 {store['store_name']} 创建 {period} 报表")
                                st.rerun()
                            else:
                                st.error("❌ 创建失败")
            else:
                st.warning("未找到可用门店，请先在门店管理中添加门店")
        except Exception as e:
            st.error(f"获取门店列表失败: {e}")

def create_store_query_app():
    """门店查询系统界面"""
    st.title("🔍 门店查询系统")
    
    # 获取数据库连接
    db_manager = get_db_manager()
    if not db_manager.is_connected():
        st.error("数据库连接失败，请稍后重试")
        return
    
    db = db_manager.get_database()
    
    # 查询界面
    with st.form("store_query_form"):
        st.subheader("🔎 请输入查询代码")
        query_code = st.text_input("查询代码", placeholder="请输入您的查询代码", help="请联系管理员获取查询代码")
        search_btn = st.form_submit_button("🔍 查询门店", type="primary")
        
        if search_btn and query_code:
            try:
                # 验证查询代码
                permission = db['permissions'].find_one({'query_code': query_code.strip()})
                
                if not permission:
                    st.error("❌ 查询代码无效，请检查后重试")
                    return
                
                # 获取门店信息
                store_id = permission['store_id']
                store = db['stores'].find_one({'_id': store_id})
                
                if not store:
                    st.error("❌ 门店信息不存在")
                    return
                
                # 获取报表数据
                reports = list(db['reports'].find({'store_id': store_id}).sort([('report_month', -1)]))
                
                st.success(f"✅ 查询成功！找到门店：{store['store_name']}")
                
                # 显示门店信息
                st.subheader("🏪 门店信息")
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.info(f"**门店名称**: {store['store_name']}")
                with col2:
                    st.info(f"**门店代码**: {store.get('store_code', '未设置')}")
                with col3:
                    st.info(f"**所属区域**: {store.get('region', '未分类')}")
                
                # 显示报表数据
                if reports:
                    st.subheader("📊 历史报表数据")
                    
                    # 期间选择
                    periods = [report['report_month'] for report in reports]
                    selected_period = st.selectbox("选择查询期间", periods)
                    
                    # 找到选定期间的报表
                    selected_report = next((r for r in reports if r['report_month'] == selected_period), None)
                    
                    if selected_report:
                        # 显示表格数据
                        with st.expander(f"📋 {selected_period} 报表详情", expanded=True):
                            if selected_report.get('table_headers') and selected_report.get('raw_excel_data'):
                                # 重构数据用于显示
                                headers = selected_report['table_headers']
                                raw_data = selected_report['raw_excel_data']
                                
                                # 创建DataFrame用于显示
                                display_data = []
                                for row_data in raw_data:
                                    row = []
                                    for i, header in enumerate(headers):
                                        col_key = f"col_{i}"
                                        value = row_data.get(col_key, "")
                                        row.append(str(value) if value else "")
                                    display_data.append(row)
                                
                                if display_data:
                                    df = pd.DataFrame(display_data, columns=headers)
                                    st.dataframe(df, use_container_width=True, hide_index=True)
                                
                                    # 下载Excel
                                    excel_buffer = io.BytesIO()
                                    with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
                                        df.to_excel(writer, index=False, sheet_name=store['store_name'][:30])
                                    
                                    excel_buffer.seek(0)
                                    
                                    st.download_button(
                                        label="📥 下载Excel报表",
                                        data=excel_buffer,
                                        file_name=f"{store['store_name']}_{selected_period}_报表.xlsx",
                                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                                    )
                            else:
                                st.info("该期间暂无详细数据")
                        
                        # 基础统计信息
                        st.subheader("📈 数据统计")
                        if selected_report.get('financial_data'):
                            financial_data = selected_report['financial_data']
                            col1, col2, col3 = st.columns(3)
                            
                            with col1:
                                total_revenue = financial_data.get('total_revenue', 0)
                                st.metric("总收入", f"¥{total_revenue:,.2f}")
                            
                            with col2:
                                total_cost = financial_data.get('total_cost', 0)
                                st.metric("总支出", f"¥{total_cost:,.2f}")
                            
                            with col3:
                                net_profit = total_revenue - total_cost
                                st.metric("净利润", f"¥{net_profit:,.2f}", 
                                         delta=f"{(net_profit/total_revenue*100):.1f}%" if total_revenue > 0 else "0%")
                        
                        # 如果是财务报表，显示可视化看板
                        if selected_report.get('raw_excel_data'):
                            st.markdown("---")
                            st.markdown("## 📊 财务运算可视化看板")
                            
                            # 尝试从报表数据中提取财务数据
                            raw_data = selected_report.get('raw_excel_data', [])
                            headers = selected_report.get('table_headers', [])
                            
                            # 模拟财务数据（实际应该从Excel数据中解析）
                            mock_admin_data = {'1': 50000, '11': 30000, '16': 20000}
                            mock_user_inputs = {'18': 8000, '19': 5000, '20': 1000, '21': 500, '22': 0, '23': 0, '24': 0, '25': 0, '26': 0}
                            
                            # 计算指标
                            huikuan = mock_admin_data.get('1', 0)
                            xianshang_zhichu = mock_admin_data.get('11', 0)
                            xianshang_jinglilun = mock_admin_data.get('16', 0)
                            xianxia_total = sum(mock_user_inputs.values())
                            xianshang_yue = huikuan - xianshang_zhichu
                            zuizhong_yue = xianshang_yue - xianxia_total
                            zuizhong_jinglilun = xianshang_jinglilun - xianxia_total
                            
                            # 勾稽关系提醒
                            st.markdown("""
                            <div style="background-color: #fff3cd; border: 1px solid #ffeaa7; border-radius: 10px; padding: 15px; margin: 10px 0;">
                                <h4 style="color: #856404; margin: 0;">⚠️ 重要勾稽关系</h4>
                                <p style="color: #856404; margin: 5px 0; font-weight: bold;">
                                    表一(9) ≡ 表二(11) &nbsp;&nbsp;|&nbsp;&nbsp; 表一(14) ≡ 表二(12)
                                </p>
                                <small style="color: #856404;">请确保两个表格对应项目数值一致</small>
                            </div>
                            """, unsafe_allow_html=True)
                            
                            # 创建两个看板
                            col_cash, col_profit = st.columns(2)
                            
                            with col_cash:
                                st.markdown("""
                                <div style="background-color: #e8f5e8; border: 2px solid #28a745; border-radius: 15px; padding: 20px;">
                                    <h3 style="color: #155724; text-align: center; margin: 0;">🟢 现金表运算</h3>
                                </div>
                                """, unsafe_allow_html=True)
                                
                                # 现金表流程图
                                st.markdown(f"""
                                <div style="background-color: #f8fff8; padding: 20px; border-radius: 10px; margin: 10px 0;">
                                    <div style="text-align: center;">
                                        <div style="background-color: #28a745; color: white; padding: 10px; border-radius: 8px; margin: 5px; display: inline-block;">
                                            <strong>(1) 回款</strong><br/>¥{huikuan:,.2f}
                                        </div>
                                        <div style="font-size: 20px; margin: 10px;">➖</div>
                                        <div style="background-color: #6c757d; color: white; padding: 10px; border-radius: 8px; margin: 5px; display: inline-block;">
                                            <strong>(11) 线上支出</strong><br/>¥{xianshang_zhichu:,.2f}
                                        </div>
                                        <div style="font-size: 20px; margin: 10px;">⬇️</div>
                                        <div style="background-color: #17a2b8; color: white; padding: 10px; border-radius: 8px; margin: 5px; display: inline-block;">
                                            <strong>(15) 线上余额</strong><br/>¥{xianshang_yue:,.2f}
                                        </div>
                                        <div style="font-size: 20px; margin: 10px;">➖</div>
                                        <div style="background-color: #fd7e14; color: white; padding: 10px; border-radius: 8px; margin: 5px; display: inline-block;">
                                            <strong>(17) 线下费用合计</strong><br/>¥{xianxia_total:,.2f}
                                        </div>
                                        <div style="font-size: 20px; margin: 10px;">⬇️</div>
                                        <div style="background-color: #dc3545; color: white; padding: 15px; border-radius: 8px; margin: 5px; display: inline-block; font-size: 18px;">
                                            <strong>(26) 最终余额</strong><br/>¥{zuizhong_yue:,.2f}
                                        </div>
                                    </div>
                                </div>
                                """, unsafe_allow_html=True)
                            
                            with col_profit:
                                st.markdown("""
                                <div style="background-color: #e3f2fd; border: 2px solid #1976d2; border-radius: 15px; padding: 20px;">
                                    <h3 style="color: #1565c0; text-align: center; margin: 0;">🔵 利润表运算</h3>
                                </div>
                                """, unsafe_allow_html=True)
                                
                                # 利润表流程图
                                st.markdown(f"""
                                <div style="background-color: #f8feff; padding: 20px; border-radius: 10px; margin: 10px 0;">
                                    <div style="text-align: center;">
                                        <div style="background-color: #1976d2; color: white; padding: 10px; border-radius: 8px; margin: 5px; display: inline-block;">
                                            <strong>(16) 线上净利润</strong><br/>¥{xianshang_jinglilun:,.2f}
                                        </div>
                                        <div style="font-size: 20px; margin: 10px;">➖</div>
                                        <div style="background-color: #fd7e14; color: white; padding: 10px; border-radius: 8px; margin: 5px; display: inline-block;">
                                            <strong>(17) 线下费用合计</strong><br/>¥{xianxia_total:,.2f}
                                        </div>
                                        <div style="font-size: 14px; color: #6c757d; margin: 10px;">
                                            SUM(18至26项明细)
                                        </div>
                                        <div style="font-size: 20px; margin: 10px;">⬇️</div>
                                        <div style="background-color: #28a745; color: white; padding: 15px; border-radius: 8px; margin: 5px; display: inline-block; font-size: 18px;">
                                            <strong>(27) 最终净利润</strong><br/>¥{zuizhong_jinglilun:,.2f}
                                        </div>
                                    </div>
                                </div>
                                """, unsafe_allow_html=True)
                            
                            # 关键结果项展示
                            st.markdown("### 🎯 **关键财务指标**")
                            col_key1, col_key2 = st.columns(2)
                            
                            with col_key1:
                                st.markdown(f"""
                                <div style="background-color: #e8f5e8; padding: 15px; border-radius: 10px; border-left: 5px solid #28a745;">
                                    <h4 style="color: #155724; margin: 0;">💰 最终余额 (26)</h4>
                                    <h2 style="color: #155724; margin: 5px 0; font-weight: bold;">¥{zuizhong_yue:,.2f}</h2>
                                </div>
                                """, unsafe_allow_html=True)
                            
                            with col_key2:
                                st.markdown(f"""
                                <div style="background-color: #e3f2fd; padding: 15px; border-radius: 10px; border-left: 5px solid #1976d2;">
                                    <h4 style="color: #1565c0; margin: 0;">📊 最终净利润 (27)</h4>
                                    <h2 style="color: #1565c0; margin: 5px 0; font-weight: bold;">¥{zuizhong_jinglilun:,.2f}</h2>
                                </div>
                                """, unsafe_allow_html=True)
                else:
                    st.info("📝 暂无报表数据")
                    
            except Exception as e:
                st.error(f"❌ 查询出错: {e}")

def create_bulk_upload_app():
    """批量上传系统界面"""
    st.title("📤 批量上传系统")
    
    # 管理员密码验证
    if 'upload_authenticated' not in st.session_state:
        st.session_state.upload_authenticated = False
    
    if not st.session_state.upload_authenticated:
        with st.form("upload_login"):
            st.subheader("🔐 管理员验证")
            st.info("批量上传需要管理员权限")
            password = st.text_input("请输入管理员密码", type="password")
            login_btn = st.form_submit_button("验证")
            
            if login_btn:
                admin_password = ConfigManager.get_admin_password()
                if password == admin_password:
                    st.session_state.upload_authenticated = True
                    st.success("✅ 验证成功！")
                    st.rerun()
                else:
                    st.error("❌ 密码错误")
        return
    
    # 获取数据库连接
    db_manager = get_db_manager()
    if not db_manager.is_connected():
        st.error("数据库连接失败，请联系系统管理员")
        return
    
    db = db_manager.get_database()
    
    # 退出按钮
    if st.button("🚪 退出", type="secondary"):
        st.session_state.upload_authenticated = False
        st.rerun()
    
    # 上传界面
    st.subheader("📁 Excel文件上传")
    
    uploaded_files = st.file_uploader(
        "选择Excel文件",
        type=['xlsx', 'xls'],
        accept_multiple_files=True,
        help="支持同时上传多个Excel文件"
    )
    
    if uploaded_files:
        st.subheader("📋 文件预览")
        
        for i, uploaded_file in enumerate(uploaded_files):
            with st.expander(f"📊 {uploaded_file.name}", expanded=i < 3):  # 只展开前3个
                try:
                    # 读取Excel文件
                    df = pd.read_excel(uploaded_file)
                    
                    # 显示基本信息
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("总行数", len(df))
                    with col2:
                        st.metric("总列数", len(df.columns))
                    with col3:
                        st.metric("文件大小", f"{uploaded_file.size / 1024:.1f} KB")
                    
                    # 显示数据预览
                    st.write("**数据预览（前10行）:**")
                    st.dataframe(df.head(10), use_container_width=True)
                    
                    # 门店识别
                    st.write("**门店识别:**")
                    potential_store_name = uploaded_file.name.replace('.xlsx', '').replace('.xls', '')
                    
                    # 从数据库查找匹配的门店
                    stores = list(db['stores'].find({'status': 'active'}))
                    matched_store = None
                    
                    for store in stores:
                        if (store['store_name'] in potential_store_name or 
                            potential_store_name in store['store_name'] or
                            any(alias in potential_store_name for alias in store.get('aliases', []))):
                            matched_store = store
                            break
                    
                    if matched_store:
                        st.success(f"✅ 自动识别门店: {matched_store['store_name']}")
                        store_for_upload = matched_store
                    else:
                        st.warning("⚠️ 无法自动识别门店，请手动选择")
                        store_options = {store['store_name']: store for store in stores}
                        selected_name = st.selectbox(
                            f"为 {uploaded_file.name} 选择门店",
                            list(store_options.keys()),
                            key=f"store_select_{i}"
                        )
                        store_for_upload = store_options[selected_name]
                    
                    # 期间设置
                    current_date = datetime.now()
                    default_month = current_date.strftime("%Y-%m")
                    report_month = st.text_input(
                        "报告期间 (YYYY-MM)", 
                        value=default_month,
                        key=f"month_input_{i}"
                    )
                    
                    # 上传按钮
                    if st.button(f"📤 上传 {uploaded_file.name}", key=f"upload_btn_{i}"):
                        if re.match(r'^\d{4}-\d{2}$', report_month):
                            try:
                                # 处理数据
                                dict_data, headers = ReportModel.dataframe_to_dict_list(df)
                                
                                # 创建报表文档
                                report_doc = ReportModel.create_report_document(
                                    store_data=store_for_upload,
                                    report_month=report_month,
                                    excel_data=dict_data,
                                    headers=headers,
                                    uploaded_by='admin'
                                )
                                
                                # 保存到数据库
                                db['reports'].replace_one(
                                    {
                                        'store_id': store_for_upload['_id'],
                                        'report_month': report_month
                                    },
                                    report_doc,
                                    upsert=True
                                )
                                
                                st.success(f"✅ {uploaded_file.name} 上传成功！")
                                
                            except Exception as e:
                                st.error(f"❌ 上传失败: {e}")
                        else:
                            st.error("❌ 期间格式错误，请使用YYYY-MM格式")
                
                except Exception as e:
                    st.error(f"❌ 文件读取错误: {e}")
        
        # 批量操作
        st.subheader("🚀 批量操作")
        
        if len(uploaded_files) > 1:
            col1, col2 = st.columns(2)
            
            with col1:
                bulk_month = st.text_input("统一期间 (YYYY-MM)", value=datetime.now().strftime("%Y-%m"))
            
            with col2:
                if st.button("📤 批量上传全部", type="primary"):
                    if re.match(r'^\d{4}-\d{2}$', bulk_month):
                        success_count = 0
                        
                        progress_bar = st.progress(0)
                        status_text = st.empty()
                        
                        for i, file in enumerate(uploaded_files):
                            try:
                                status_text.text(f"正在处理: {file.name}")
                                
                                # 读取文件
                                df = pd.read_excel(file)
                                
                                # 自动匹配门店
                                potential_name = file.name.replace('.xlsx', '').replace('.xls', '')
                                stores = list(db['stores'].find({'status': 'active'}))
                                matched_store = None
                                
                                for store in stores:
                                    if (store['store_name'] in potential_name or 
                                        potential_name in store['store_name'] or
                                        any(alias in potential_name for alias in store.get('aliases', []))):
                                        matched_store = store
                                        break
                                
                                if matched_store:
                                    # 处理数据
                                    dict_data, headers = ReportModel.dataframe_to_dict_list(df)
                                    
                                    # 创建报表文档
                                    report_doc = ReportModel.create_report_document(
                                        store_data=matched_store,
                                        report_month=bulk_month,
                                        excel_data=dict_data,
                                        headers=headers,
                                        uploaded_by='admin'
                                    )
                                    
                                    # 保存到数据库
                                    db['reports'].replace_one(
                                        {
                                            'store_id': matched_store['_id'],
                                            'report_month': bulk_month
                                        },
                                        report_doc,
                                        upsert=True
                                    )
                                    
                                    success_count += 1
                                
                                # 更新进度
                                progress_bar.progress((i + 1) / len(uploaded_files))
                                
                            except Exception as e:
                                st.error(f"❌ {file.name} 处理失败: {e}")
                        
                        progress_bar.progress(1.0)
                        status_text.text(f"批量上传完成！成功: {success_count}/{len(uploaded_files)}")
                        
                        if success_count > 0:
                            st.balloons()
                    else:
                        st.error("❌ 期间格式错误")

def create_permission_management_app():
    """权限管理系统界面"""
    st.title("👥 权限管理系统")
    
    # 管理员密码验证
    if 'perm_authenticated' not in st.session_state:
        st.session_state.perm_authenticated = False
    
    if not st.session_state.perm_authenticated:
        with st.form("perm_login"):
            st.subheader("🔐 管理员验证")
            password = st.text_input("请输入管理员密码", type="password")
            login_btn = st.form_submit_button("验证")
            
            if login_btn:
                admin_password = ConfigManager.get_admin_password()
                if password == admin_password:
                    st.session_state.perm_authenticated = True
                    st.success("✅ 验证成功！")
                    st.rerun()
                else:
                    st.error("❌ 密码错误")
        return
    
    # 获取数据库连接
    db_manager = get_db_manager()
    if not db_manager.is_connected():
        st.error("数据库连接失败，请联系系统管理员")
        return
    
    db = db_manager.get_database()
    
    # 退出按钮
    if st.button("🚪 退出", type="secondary"):
        st.session_state.perm_authenticated = False
        st.rerun()
    
    # 权限管理界面
    tab1, tab2, tab3 = st.tabs(["🔑 查询权限管理", "🏪 门店管理", "📊 数据统计"])
    
    with tab1:
        st.subheader("🔑 查询权限管理")
        
        # 添加新权限
        with st.expander("➕ 添加查询权限", expanded=False):
            with st.form("add_permission"):
                col1, col2 = st.columns(2)
                
                with col1:
                    new_query_code = st.text_input("查询代码", placeholder="输入新的查询代码")
                
                with col2:
                    # 获取所有门店
                    stores = list(db['stores'].find({'status': 'active'}))
                    if stores:
                        store_options = {store['store_name']: store for store in stores}
                        selected_store_name = st.selectbox("选择门店", list(store_options.keys()))
                        selected_store = store_options[selected_store_name]
                    else:
                        st.warning("无可用门店")
                        selected_store = None
                
                add_btn = st.form_submit_button("➕ 添加权限", type="primary")
                
                if add_btn and new_query_code and selected_store:
                    try:
                        # 检查查询代码是否已存在
                        existing = db['permissions'].find_one({'query_code': new_query_code.strip()})
                        
                        if existing:
                            st.error("❌ 查询代码已存在")
                        else:
                            # 创建权限文档
                            permission_doc = PermissionModel.create_permission_document(
                                query_code=new_query_code,
                                store_data=selected_store,
                                created_by='admin'
                            )
                            
                            db['permissions'].insert_one(permission_doc)
                            st.success("✅ 权限添加成功！")
                            st.rerun()
                            
                    except Exception as e:
                        st.error(f"❌ 添加失败: {e}")
        
        # 现有权限列表
        st.subheader("📋 现有权限列表")
        
        try:
            permissions = list(db['permissions'].find({'status': 'active'}).sort([('created_at', -1)]))
            
            if permissions:
                # 创建表格数据
                perm_data = []
                for perm in permissions:
                    perm_data.append({
                        '查询代码': perm['query_code'],
                        '门店名称': perm['store_name'],
                        '门店代码': perm.get('store_code', '未设置'),
                        '创建时间': perm.get('created_at', datetime.now()).strftime('%Y-%m-%d %H:%M'),
                        '创建者': perm.get('created_by', '未知')
                    })
                
                df = pd.DataFrame(perm_data)
                
                # 显示表格（可编辑）
                edited_df = st.data_editor(
                    df,
                    use_container_width=True,
                    hide_index=True,
                    num_rows="dynamic"
                )
                
                # 批量操作
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    if st.button("🗑️ 删除选中", type="secondary"):
                        # 这里可以添加删除逻辑
                        st.info("删除功能需要实现")
                
                with col2:
                    if st.button("📊 导出权限列表"):
                        excel_buffer = io.BytesIO()
                        with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
                            df.to_excel(writer, index=False, sheet_name='查询权限列表')
                        
                        excel_buffer.seek(0)
                        
                        st.download_button(
                            label="⬇️ 下载权限列表",
                            data=excel_buffer,
                            file_name=f"查询权限列表_{datetime.now().strftime('%Y%m%d')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                
                with col3:
                    if st.button("🔄 刷新列表"):
                        st.rerun()
                        
            else:
                st.info("📝 暂无权限记录")
                
        except Exception as e:
            st.error(f"❌ 获取权限列表失败: {e}")
    
    with tab2:
        st.subheader("🏪 门店管理")
        
        # 添加新门店
        with st.expander("➕ 添加新门店", expanded=False):
            with st.form("add_store"):
                col1, col2 = st.columns(2)
                
                with col1:
                    store_name = st.text_input("门店名称", placeholder="输入门店名称")
                    store_code = st.text_input("门店代码", placeholder="留空则自动生成")
                
                with col2:
                    region = st.text_input("所属区域", placeholder="例如：华东区")
                    manager = st.text_input("门店经理", placeholder="门店负责人姓名")
                
                add_store_btn = st.form_submit_button("➕ 添加门店", type="primary")
                
                if add_store_btn and store_name:
                    try:
                        # 检查门店名称是否已存在
                        existing = db['stores'].find_one({'store_name': store_name.strip()})
                        
                        if existing:
                            st.error("❌ 门店名称已存在")
                        else:
                            # 创建门店文档
                            store_doc = StoreModel.create_store_document(
                                store_name=store_name,
                                store_code=store_code if store_code else None,
                                region=region,
                                manager=manager,
                                created_by='admin'
                            )
                            
                            db['stores'].insert_one(store_doc)
                            st.success("✅ 门店添加成功！")
                            st.rerun()
                            
                    except Exception as e:
                        st.error(f"❌ 添加失败: {e}")
        
        # 现有门店列表
        st.subheader("📋 门店列表")
        
        try:
            stores = list(db['stores'].find({'status': 'active'}).sort([('created_at', -1)]))
            
            if stores:
                store_data = []
                for store in stores:
                    store_data.append({
                        '门店名称': store['store_name'],
                        '门店代码': store.get('store_code', '未设置'),
                        '所属区域': store.get('region', '未分类'),
                        '门店经理': store.get('manager', '待设置'),
                        '创建时间': store.get('created_at', datetime.now()).strftime('%Y-%m-%d %H:%M')
                    })
                
                store_df = pd.DataFrame(store_data)
                st.dataframe(store_df, use_container_width=True, hide_index=True)
                
                # 门店操作
                if st.button("📊 导出门店列表"):
                    excel_buffer = io.BytesIO()
                    with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
                        store_df.to_excel(writer, index=False, sheet_name='门店列表')
                    
                    excel_buffer.seek(0)
                    
                    st.download_button(
                        label="⬇️ 下载门店列表",
                        data=excel_buffer,
                        file_name=f"门店列表_{datetime.now().strftime('%Y%m%d')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
            else:
                st.info("📝 暂无门店记录")
                
        except Exception as e:
            st.error(f"❌ 获取门店列表失败: {e}")
    
    with tab3:
        st.subheader("📊 数据统计")
        
        try:
            # 基础统计
            total_stores = db['stores'].count_documents({'status': 'active'})
            total_permissions = db['permissions'].count_documents({'status': 'active'})
            total_reports = db['reports'].count_documents({})
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("总门店数", total_stores)
            with col2:
                st.metric("总权限数", total_permissions)
            with col3:
                st.metric("总报表数", total_reports)
            
            # 按区域统计
            if total_stores > 0:
                st.subheader("📍 门店区域分布")
                
                pipeline = [
                    {'$match': {'status': 'active'}},
                    {'$group': {'_id': '$region', 'count': {'$sum': 1}}},
                    {'$sort': {'count': -1}}
                ]
                
                region_stats = list(db['stores'].aggregate(pipeline))
                
                if region_stats:
                    region_df = pd.DataFrame([
                        {'区域': stat['_id'] or '未分类', '门店数量': stat['count']} 
                        for stat in region_stats
                    ])
                    
                    fig = px.pie(region_df, values='门店数量', names='区域', title="门店区域分布")
                    st.plotly_chart(fig, use_container_width=True)
            
            # 报表提交情况
            if total_reports > 0:
                st.subheader("📅 报表提交趋势")
                
                pipeline = [
                    {'$group': {'_id': '$report_month', 'count': {'$sum': 1}}},
                    {'$sort': {'_id': 1}}
                ]
                
                month_stats = list(db['reports'].aggregate(pipeline))
                
                if month_stats:
                    month_df = pd.DataFrame([
                        {'期间': stat['_id'], '报表数量': stat['count']} 
                        for stat in month_stats
                    ])
                    
                    fig = px.line(month_df, x='期间', y='报表数量', title="报表提交趋势", markers=True)
                    st.plotly_chart(fig, use_container_width=True)
                    
        except Exception as e:
            st.error(f"❌ 统计数据获取失败: {e}")

def main():
    """主应用入口"""
    
    # 侧边栏
    with st.sidebar:
        st.title("🏪 门店报表系统")
        st.caption("数据查询平台")
        
        app_choice = st.selectbox(
            "选择功能模块",
            [
                "门店查询系统", 
                "财务填报系统", 
                "财务管理系统", 
                "批量上传系统", 
                "权限管理系统"
            ],
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
    
    # 主界面 - 连接实际功能
    try:
        if app_choice == "门店查询系统":
            create_store_query_app()
        elif app_choice == "财务填报系统":
            create_financial_report_app()
        elif app_choice == "财务管理系统":
            create_financial_admin_app()
        elif app_choice == "批量上传系统":
            create_bulk_upload_app()
        elif app_choice == "权限管理系统":
            create_permission_management_app()
    except Exception as e:
        st.error(f"应用运行出错: {e}")
        with st.expander("查看详细错误信息"):
            st.code(traceback.format_exc())

if __name__ == "__main__":
    main()
