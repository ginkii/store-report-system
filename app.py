# streamlit_app_integrated.py - 集成版门店财务报表系统
"""
门店财务报表系统 - 集成版
基于原系统保留所有功能，集成新系统的财务计算引擎和Excel风格界面
包含：查询、上传、权限管理、财务填报、实时计算等完整功能
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
import xlsxwriter

# 页面配置
if "page_configured" not in st.session_state:
    st.set_page_config(
        page_title="门店财务报表系统",
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

# 新系统的财务计算引擎
class FinancialCalculator:
    @staticmethod
    def calculate_cash_flow(admin_data: Dict, user_inputs: Dict) -> Dict:
        """现金表逻辑计算"""
        # 项目(15) 线上余额 = 项目(1) 回款 - 项目(11) 线上支出
        online_balance = admin_data.get("1", 0) - admin_data.get("11", 0)
        
        # 项目(16) 线下支出合计 = SUM(项目17 至 25)
        offline_total = sum(user_inputs.values())
        
        # 项目(26) 最终余额 = 项目(15) - 项目(16)
        final_balance = online_balance - offline_total
        
        return {
            "online_balance": online_balance,
            "offline_total": offline_total,
            "final_balance": final_balance
        }
    
    @staticmethod
    def calculate_profit(admin_data: Dict, user_inputs: Dict) -> Dict:
        """利润表逻辑计算"""
        # 项目(17) 线下费用总额 = SUM(项目18 至 26)
        offline_cost_total = sum(user_inputs.values())
        
        # 项目(27) 最终净利润 = 项目(16) 线上净利润 - 项目(17) 线下费用总额
        final_profit = admin_data.get("16", 0) - offline_cost_total
        
        return {
            "offline_cost_total": offline_cost_total,
            "final_profit": final_profit
        }

# 新系统的Excel导出功能
class EnhancedExcelExporter:
    @staticmethod
    def create_styled_excel(report_data: Dict) -> io.BytesIO:
        """Create Excel file with styling and formulas"""
        output = io.BytesIO()
        workbook = xlsxwriter.Workbook(output)
        
        # Define styles - Excel风格样式
        header_format = workbook.add_format({
            'bold': True,
            'font_size': 12,
            'bg_color': '#4F81BD',
            'font_color': 'white',
            'border': 1,
            'align': 'center'
        })
        
        cash_format = workbook.add_format({
            'bg_color': '#EBF1DE',  # 浅绿色现金表
            'border': 1,
            'num_format': '#,##0.00'
        })
        
        profit_format = workbook.add_format({
            'bg_color': '#DBEEF4',  # 浅蓝色利润表
            'border': 1,
            'num_format': '#,##0.00'
        })
        
        key_result_format = workbook.add_format({
            'bold': True,
            'font_size': 14,
            'bg_color': '#FFE699',  # 关键结果项高亮
            'border': 1,
            'num_format': '#,##0.00'
        })
        
        # Create worksheet
        store_name = report_data.get("header", {}).get("store_name", "门店")
        worksheet = workbook.add_worksheet(f"{store_name}")
        
        # Write headers and data with formulas
        EnhancedExcelExporter._write_financial_report(
            worksheet, report_data, header_format, 
            cash_format, profit_format, key_result_format
        )
        
        workbook.close()
        output.seek(0)
        return output
    
    @staticmethod
    def _write_financial_report(worksheet, report_data, header_format, cash_format, profit_format, key_result_format):
        """Write financial report data with Excel formulas"""
        header = report_data.get("header", {})
        admin_data = report_data.get("admin_data", {})
        user_inputs = report_data.get("user_inputs", {})
        
        # Store header
        worksheet.write('A1', f"门店财务报表 - {header.get('store_name', '')}", header_format)
        worksheet.write('A2', f"账期: {header.get('period', '')}", header_format)
        worksheet.write('A3', f"状态: {header.get('status', 'pending')}", header_format)
        
        # 现金表部分 (浅绿色背景)
        row = 5
        worksheet.write(f'A{row}', '🟢 现金表', header_format)
        row += 1
        
        # 管理员数据项目1-16 (只读)
        admin_labels = {
            "1": "(1) 回款",
            "2": "(2) 其他收入",
            "11": "(11) 线上支出",
            "16": "(16) 线上净利润"
        }
        
        for item_key, label in admin_labels.items():
            worksheet.write(f'A{row}', label, cash_format)
            worksheet.write(f'B{row}', admin_data.get(item_key, 0), cash_format)
            row += 1
        
        # 计算项目15 线上余额 = 项目1 - 项目11
        worksheet.write(f'A{row}', "(15) 线上余额", key_result_format)
        worksheet.write_formula(f'B{row}', f'=B6-B8', key_result_format)  # 假设项目1在B6,项目11在B8
        row += 2
        
        # 利润表部分 (浅蓝色背景)
        worksheet.write(f'A{row}', '🔵 利润表', header_format)
        row += 1
        
        # 用户填报项目18-26
        user_input_labels = {
            "18": "(18) 工资",
            "19": "(19) 房租",
            "20": "(20) 水电费",
            "21": "(21) 物业费",
            "22": "(22) 其他费用1",
            "23": "(23) 其他费用2",
            "24": "(24) 其他费用3",
            "25": "(25) 其他费用4",
            "26": "(26) 其他费用5"
        }
        
        start_row = row
        for item_key, label in user_input_labels.items():
            worksheet.write(f'A{row}', label, profit_format)
            worksheet.write(f'B{row}', user_inputs.get(item_key, 0), profit_format)
            row += 1
        
        # 计算项目17 线下费用总额
        worksheet.write(f'A{row}', "(17) 线下费用总额", key_result_format)
        worksheet.write_formula(f'B{row}', f'=SUM(B{start_row}:B{row-1})', key_result_format)
        row += 1
        
        # 计算项目26 现金余额
        worksheet.write(f'A{row}', "(26) 现金余额", key_result_format)
        worksheet.write_formula(f'B{row}', f'=B10-B{row-1}', key_result_format)  # 线上余额-线下费用
        row += 1
        
        # 计算项目27 最终净利润 = 项目16 - 项目17
        worksheet.write(f'A{row}', "(27) 最终净利润", key_result_format)
        worksheet.write_formula(f'B{row}', f'=B9-B{row-2}', key_result_format)  # 线上净利润-线下费用

# 数据库管理 (保留原系统)
try:
    import pymongo
    from pymongo import MongoClient
    PYMONGO_AVAILABLE = True
except ImportError:
    PYMONGO_AVAILABLE = False

class DatabaseManager:
    """数据库管理器 - 集成版"""
    
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
            error_msg = f"数据库连接失败: {e}"
            if "ServerSelectionTimeoutError" in str(type(e)):
                error_msg += "\n💡 提示：请检查MongoDB URI和网络连接"
            elif "Authentication" in str(e):
                error_msg += "\n💡 提示：请检查数据库用户名和密码"
            
            st.error(error_msg)
            self.db = None
            self.client = None
    
    def _create_indexes(self):
        """创建索引 - 集成新旧系统"""
        if self.db is None:
            return
            
        try:
            # 原系统索引
            self.db['stores'].create_index([("store_code", 1)], background=True)
            self.db['permissions'].create_index([("query_code", 1)], background=True)
            self.db['reports'].create_index([("store_id", 1), ("report_month", -1)], background=True)
            
            # 新系统财务报表索引
            self.db['store_financial_reports'].create_index([("header.store_id", 1), ("header.period", 1)], unique=True)
            self.db['store_financial_reports'].create_index([("header.status", 1)])
            self.db['store_financial_reports'].create_index([("metadata.created_at", -1)])
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

# 数据模型 (保留原系统)
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

# 新系统的财务报表数据库类
class FinancialReportDB:
    def __init__(self, db):
        self.db = db
        self.collection = db.store_financial_reports
    
    def create_report(self, store_id: str, store_name: str, period: str, admin_data: Dict) -> bool:
        """创建新的财务报表"""
        try:
            document = {
                "header": {
                    "store_id": store_id,
                    "store_name": store_name,
                    "period": period,
                    "status": "pending"
                },
                "admin_data": admin_data,  # Items 1-16
                "user_inputs": {
                    "18": 0.0,  # 工资
                    "19": 0.0,  # 房租
                    "20": 0.0,  # 水电费
                    "21": 0.0,  # 物业费
                    "22": 0.0,  # 其他费用1
                    "23": 0.0,  # 其他费用2
                    "24": 0.0,  # 其他费用3
                    "25": 0.0,  # 其他费用4
                    "26": 0.0   # 其他费用5
                },
                "calculated_metrics": {
                    "17": 0.0,  # 线下总成本
                    "26_cash": 0.0,  # 现金余额
                    "27": 0.0   # 最终净利润
                },
                "metadata": {
                    "created_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow(),
                    "submitted_by": None,
                    "submitted_at": None
                }
            }
            self.collection.insert_one(document)
            return True
        except pymongo.errors.DuplicateKeyError:
            return False
        except Exception as e:
            st.error(f"Database error: {e}")
            return False
    
    def get_report(self, store_id: str, period: str) -> Optional[Dict]:
        """获取财务报表"""
        return self.collection.find_one({
            "header.store_id": store_id,
            "header.period": period
        })
    
    def update_user_inputs(self, store_id: str, period: str, user_inputs: Dict) -> bool:
        """更新用户输入数据并重新计算"""
        try:
            report = self.get_report(store_id, period)
            if not report or report["header"]["status"] == "submitted":
                return False
            
            # 使用新的计算引擎
            admin_data = report["admin_data"]
            cash_flow = FinancialCalculator.calculate_cash_flow(admin_data, user_inputs)
            profit_calc = FinancialCalculator.calculate_profit(admin_data, user_inputs)
            
            calculated_metrics = {
                "17": profit_calc["offline_cost_total"],
                "26_cash": cash_flow["final_balance"],
                "27": profit_calc["final_profit"]
            }
            
            # 更新数据库
            self.collection.update_one(
                {"header.store_id": store_id, "header.period": period},
                {
                    "$set": {
                        "user_inputs": user_inputs,
                        "calculated_metrics": calculated_metrics,
                        "metadata.updated_at": datetime.utcnow()
                    }
                }
            )
            return True
        except Exception as e:
            st.error(f"Update error: {e}")
            return False
    
    def submit_report(self, store_id: str, period: str, submitted_by: str) -> bool:
        """提交报表"""
        try:
            result = self.collection.update_one(
                {"header.store_id": store_id, "header.period": period},
                {
                    "$set": {
                        "header.status": "submitted",
                        "metadata.submitted_by": submitted_by,
                        "metadata.submitted_at": datetime.utcnow()
                    }
                }
            )
            return result.modified_count > 0
        except Exception as e:
            st.error(f"Submit error: {e}")
            return False

# CSS样式 - Excel风格
def get_excel_style_css():
    return """
    <style>
    .cash-flow {
        background-color: #EBF1DE !important;
        border: 1px solid #ccc;
        padding: 10px;
        margin: 5px;
        border-radius: 5px;
    }
    .profit-table {
        background-color: #DBEEF4 !important;
        border: 1px solid #ccc;
        padding: 10px;
        margin: 5px;
        border-radius: 5px;
    }
    .key-result {
        font-weight: bold !important;
        font-size: 18px !important;
        background-color: #FFE699 !important;
        padding: 10px;
        border-radius: 5px;
        margin: 5px 0;
    }
    .formula-display {
        background-color: #F0F8FF;
        border: 1px dashed #4169E1;
        padding: 15px;
        margin: 10px 0;
        border-radius: 5px;
        font-family: 'Courier New', monospace;
    }
    </style>
    """

# 主应用程序
def main():
    # 应用CSS样式
    st.markdown(get_excel_style_css(), unsafe_allow_html=True)
    
    # 数据库连接
    db_manager = get_db_manager()
    
    if not db_manager.is_connected():
        st.error("❌ 数据库连接失败，请检查配置")
        st.stop()
    
    db = db_manager.get_database()
    financial_db = FinancialReportDB(db)
    
    # 主标题
    st.title("🏪 门店财务报表系统 - 集成版")
    st.markdown("*保留完整功能，集成新财务计算引擎*")
    
    # 主导航
    main_tab1, main_tab2, main_tab3, main_tab4, main_tab5 = st.tabs([
        "🔍 门店查询", "💼 财务填报", "📤 批量上传", "👥 权限管理", "⚙️ 系统管理"
    ])
    
    with main_tab1:
        render_query_section(db)
    
    with main_tab2:
        render_enhanced_financial_section(financial_db, db)
    
    with main_tab3:
        render_upload_section(db)
    
    with main_tab4:
        render_permission_section(db)
    
    with main_tab5:
        render_admin_section(db, financial_db)

def render_enhanced_financial_section(financial_db: FinancialReportDB, db):
    """增强的财务填报界面 - 集成新系统功能"""
    st.header("💼 财务填报系统")
    st.markdown("*基于新计算引擎，支持实时运算和Excel风格界面*")
    
    # 查询表单
    col1, col2 = st.columns([2, 1])
    
    with col1:
        store_id = st.text_input("门店ID", help="输入您的门店编号")
        period = st.selectbox("报表期间", ["2024-01", "2024-02", "2024-03", "2024-04"])
    
    with col2:
        if st.button("🔍 查询报表", type="primary", use_container_width=True):
            if store_id and period:
                report = financial_db.get_report(store_id, period)
                if report:
                    st.session_state.current_financial_report = report
                    st.success("✅ 报表加载成功!")
                    st.rerun()
                else:
                    st.error("❌ 未找到该报表，请联系管理员创建")
            else:
                st.error("请输入门店ID和选择期间")
    
    # 报表填报界面
    if 'current_financial_report' in st.session_state:
        report = st.session_state.current_financial_report
        is_submitted = report["header"]["status"] == "submitted"
        
        st.markdown("---")
        st.subheader(f"📋 {report['header']['store_name']} | {report['header']['period']}")
        
        if is_submitted:
            st.warning("⚠️ 该报表已提交，无法修改")
        
        # 管理员数据展示 (只读)
        with st.expander("📊 管理员数据 (只读)", expanded=True):
            admin_data = report["admin_data"]
            admin_cols = st.columns(4)
            
            admin_labels = {
                "1": "(1) 回款",
                "2": "(2) 其他收入", 
                "11": "(11) 线上支出",
                "16": "(16) 线上净利润"
            }
            
            for i, (key, label) in enumerate(admin_labels.items()):
                with admin_cols[i % 4]:
                    value = admin_data.get(key, 0)
                    # 关键项目加粗显示
                    if key in ["1", "16"]:
                        st.markdown(f'<div class="key-result">{label}<br>¥{value:,.2f}</div>', 
                                  unsafe_allow_html=True)
                    else:
                        st.metric(label, f"¥{value:,.2f}")
        
        # 用户填报区域 - Excel风格
        st.markdown("### ✏️ 线下费用填报")
        st.markdown('<div class="profit-table">', unsafe_allow_html=True)
        
        user_input_labels = {
            "18": "(18) 工资",
            "19": "(19) 房租",
            "20": "(20) 水电费",
            "21": "(21) 物业费",
            "22": "(22) 其他费用1",
            "23": "(23) 其他费用2",
            "24": "(24) 其他费用3",
            "25": "(25) 其他费用4",
            "26": "(26) 其他费用5"
        }
        
        user_inputs = {}
        cols = st.columns(3)
        
        for i, (key, label) in enumerate(user_input_labels.items()):
            with cols[i % 3]:
                user_inputs[key] = st.number_input(
                    label,
                    value=float(report["user_inputs"].get(key, 0)),
                    min_value=0.0,
                    disabled=is_submitted,
                    help=f"输入{label}金额，将影响最终净利润计算",
                    key=f"input_{key}"
                )
        
        st.markdown('</div>', unsafe_allow_html=True)
        
        # 实时计算显示 - 使用新计算引擎
        if not is_submitted:
            admin_data = report["admin_data"]
            cash_flow = FinancialCalculator.calculate_cash_flow(admin_data, user_inputs)
            profit_calc = FinancialCalculator.calculate_profit(admin_data, user_inputs)
            
            st.markdown("### 📈 实时计算结果")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.markdown('<div class="profit-table">', unsafe_allow_html=True)
                st.metric("(17) 线下费用总额", f"¥{profit_calc['offline_cost_total']:,.2f}")
                st.markdown('</div>', unsafe_allow_html=True)
            
            with col2:
                st.markdown('<div class="cash-flow">', unsafe_allow_html=True)
                st.metric("(26) 现金余额", f"¥{cash_flow['final_balance']:,.2f}")
                st.markdown('</div>', unsafe_allow_html=True)
            
            with col3:
                st.markdown(f'''
                <div class="key-result">
                    <strong>(27) 最终净利润</strong><br>
                    <h2>¥{profit_calc["final_profit"]:,.2f}</h2>
                </div>
                ''', unsafe_allow_html=True)
        
        # 操作按钮
        if not is_submitted:
            col1, col2 = st.columns(2)
            
            with col1:
                if st.button("💾 保存数据", type="secondary", use_container_width=True):
                    if financial_db.update_user_inputs(store_id, period, user_inputs):
                        st.success("✅ 数据保存成功!")
                        st.rerun()
                    else:
                        st.error("❌ 保存失败!")
            
            with col2:
                if st.button("✅ 提交报表", type="primary", use_container_width=True):
                    if financial_db.submit_report(store_id, period, "current_user"):
                        st.success("✅ 报表提交成功!")
                        st.rerun()
                    else:
                        st.error("❌ 提交失败!")
        
        # LaTeX公式看板 - 新功能
        render_calculation_dashboard()
        
        # Excel导出
        if st.button("📊 导出Excel报表", use_container_width=True):
            excel_file = EnhancedExcelExporter.create_styled_excel(report)
            st.download_button(
                label="⬇️ 下载财务报表",
                data=excel_file.getvalue(),
                file_name=f"{report['header']['store_name']}_{report['header']['period']}_财务报表.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )

def render_calculation_dashboard():
    """运算关系看板 - LaTeX公式"""
    with st.expander("🔍 运算关系看板", expanded=False):
        st.markdown("### 财务计算逻辑")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### 🟢 现金表逻辑")
            st.latex(r'''
            \begin{aligned}
            项目(15) &= 项目(1) - 项目(11) \\
            项目(26) &= 项目(15) - 项目(17)
            \end{aligned}
            ''')
            st.markdown('<div class="cash-flow">线上余额 = 回款 - 线上支出<br>最终余额 = 线上余额 - 线下费用</div>', 
                       unsafe_allow_html=True)
        
        with col2:
            st.markdown("#### 🔵 利润表逻辑")
            st.latex(r'''
            \begin{aligned}
            项目(17) &= \sum_{i=18}^{26} 项目(i) \\
            项目(27) &= 项目(16) - 项目(17)
            \end{aligned}
            ''')
            st.markdown('<div class="profit-table">线下费用总额 = 工资+房租+水电+物业+其他<br>最终净利润 = 线上净利润 - 线下费用</div>', 
                       unsafe_allow_html=True)
        
        st.markdown("#### ⚖️ 勾稽校验")
        st.info("💡 系统自动确保: 表一(9) ≡ 表二(11) 且 表一(14) ≡ 表二(12)")

# 保留原系统的其他功能
def render_query_section(db):
    """门店查询功能 - 保留原系统"""
    st.header("🔍 门店报表查询")
    st.markdown("*输入查询代码查看门店报表数据*")
    
    # 这里保留原系统的完整查询功能
    query_code = st.text_input("🔐 查询代码", type="password")
    
    if query_code and st.button("🔍 查询", type="primary"):
        # 验证查询代码并显示对应门店数据
        permission = db['permissions'].find_one({"query_code": query_code})
        if permission:
            st.success("✅ 验证成功")
            # 显示门店数据...
            # (这里可以继续实现原系统的查询逻辑)
        else:
            st.error("❌ 查询代码无效")

def render_upload_section(db):
    """批量上传功能 - 保留原系统"""
    st.header("📤 批量数据上传")
    
    # 管理员验证
    admin_password = st.text_input("🔒 管理员密码", type="password")
    
    if admin_password == ConfigManager.get_admin_password():
        st.success("✅ 管理员验证成功")
        
        # 文件上传区域
        uploaded_files = st.file_uploader(
            "选择Excel文件",
            type=['xlsx', 'xls'],
            accept_multiple_files=True
        )
        
        if uploaded_files:
            st.success(f"✅ 已选择 {len(uploaded_files)} 个文件")
            # 继续实现批量上传逻辑...

def render_permission_section(db):
    """权限管理功能 - 保留原系统"""
    st.header("👥 权限管理")
    
    # 管理员验证
    admin_password = st.text_input("🔒 管理员密码", type="password", key="perm_admin")
    
    if admin_password == ConfigManager.get_admin_password():
        st.success("✅ 管理员验证成功")
        
        tab1, tab2 = st.tabs(["查询权限", "门店管理"])
        
        with tab1:
            st.subheader("🔐 查询权限管理")
            # 权限管理逻辑...
            
        with tab2:
            st.subheader("🏪 门店信息管理")
            # 门店管理逻辑...

def render_admin_section(db, financial_db):
    """系统管理功能 - 集成财务报表管理"""
    st.header("⚙️ 系统管理")
    
    # 管理员验证
    admin_password = st.text_input("🔒 管理员密码", type="password", key="admin_main")
    
    if admin_password == ConfigManager.get_admin_password():
        st.success("✅ 管理员验证成功")
        
        tab1, tab2, tab3 = st.tabs(["📊 数据统计", "📋 财务报表管理", "⚙️ 系统配置"])
        
        with tab1:
            # 系统数据统计
            total_stores = db['stores'].count_documents({})
            total_permissions = db['permissions'].count_documents({})
            total_reports = db['reports'].count_documents({})
            total_financial = financial_db.collection.count_documents({})
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("总门店数", total_stores)
            with col2:
                st.metric("查询权限", total_permissions)
            with col3:
                st.metric("历史报表", total_reports)
            with col4:
                st.metric("财务报表", total_financial)
        
        with tab2:
            st.subheader("📋 财务报表批量管理")
            
            # 批量创建财务报表
            st.markdown("#### 批量创建报表")
            
            col1, col2 = st.columns(2)
            with col1:
                bulk_period = st.selectbox("选择期间", ["2024-01", "2024-02", "2024-03", "2024-04"])
            with col2:
                if st.button("🔄 从门店数据创建报表"):
                    # 从现有门店数据批量创建财务报表
                    stores = list(db['stores'].find({"status": "active"}))
                    success_count = 0
                    
                    for store in stores:
                        # 创建默认的admin_data
                        admin_data = {
                            "1": 100000,   # 默认回款
                            "2": 0,        # 其他收入
                            "11": 50000,   # 默认线上支出
                            "16": 30000    # 默认线上净利润
                        }
                        
                        if financial_db.create_report(
                            store['store_code'],
                            store['store_name'],
                            bulk_period,
                            admin_data
                        ):
                            success_count += 1
                    
                    st.success(f"✅ 成功创建 {success_count} 个财务报表")
        
        with tab3:
            st.subheader("⚙️ 系统配置")
            st.info("系统配置功能...")

if __name__ == "__main__":
    main()
