import pandas as pd
import pymongo
from pymongo import MongoClient
import streamlit as st
from datetime import datetime
import json
import re
import os
from typing import Dict, List, Tuple
import hashlib
import time

class BulkReportUploader:
    def __init__(self, mongo_uri: str = None, db_name: str = None):
        """初始化批量上传器"""
        # 优先使用传入参数，然后使用Streamlit secrets，最后使用默认值
        if mongo_uri is None:
            if hasattr(st, 'secrets') and 'mongodb' in st.secrets:
                mongo_uri = st.secrets["mongodb"]["uri"]
            else:
                mongo_uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
        
        if db_name is None:
            if hasattr(st, 'secrets') and 'mongodb' in st.secrets:
                db_name = st.secrets["mongodb"]["database_name"]
            else:
                db_name = os.getenv('DATABASE_NAME', 'store_reports')
        
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.stores_collection = self.db['stores']
        self.reports_collection = self.db['reports']
        
        # 创建索引以提高查询性能
        self._create_indexes()
    
    def _create_indexes(self):
        """创建数据库索引"""
        try:
            # 门店集合索引
            self.stores_collection.create_index([("store_code", 1)], unique=True)
            self.stores_collection.create_index([("store_name", 1)])
            
            # 报表集合索引
            self.reports_collection.create_index([
                ("store_id", 1), 
                ("report_month", -1)
            ])
            self.reports_collection.create_index([("store_code", 1)])
            self.reports_collection.create_index([("report_month", -1)])
        except Exception as e:
            print(f"创建索引时发生错误: {e}")
    
    def normalize_store_name(self, sheet_name: str) -> str:
        """标准化门店名称，去除特殊字符和空格"""
        # 移除常见的前缀和后缀
        name = sheet_name.strip()
        name = re.sub(r'^(犀牛百货|门店|店铺)[\(（]?', '', name)
        name = re.sub(r'[\)）]?店?$', '', name)
        name = re.sub(r'\s+', '', name)  # 移除所有空格
        return name
    
    def find_or_create_store(self, sheet_name: str) -> Dict:
        """通过sheet名称查找门店，如果不存在则创建"""
        normalized_name = self.normalize_store_name(sheet_name)
        
        # 首先尝试查找现有门店
        search_patterns = [
            sheet_name,  # 完全匹配
            normalized_name,  # 标准化后匹配
            f".*{normalized_name}.*",  # 包含匹配
            f".*{sheet_name}.*"  # 原名包含匹配
        ]
        
        for pattern in search_patterns:
            store = self.stores_collection.find_one({
                "$or": [
                    {"store_name": {"$regex": pattern, "$options": "i"}},
                    {"store_code": {"$regex": pattern, "$options": "i"}},
                    {"aliases": {"$regex": pattern, "$options": "i"}}
                ]
            })
            if store:
                return store
        
        # 如果没有找到，创建新门店
        return self._create_store_from_sheet_name(sheet_name)
    
    def _create_store_from_sheet_name(self, sheet_name: str) -> Dict:
        """从工作表名称创建新门店"""
        try:
            normalized_name = self.normalize_store_name(sheet_name)
            
            # 生成门店代码（使用标准化名称的拼音首字母或数字）
            store_code = self._generate_store_code(normalized_name)
            
            store_data = {
                '_id': f"store_{store_code}_{int(time.time())}",
                'store_name': sheet_name.strip(),  # 使用原始名称
                'store_code': store_code,
                'region': '未分类',
                'manager': '待设置',
                'aliases': [sheet_name.strip(), normalized_name],
                'created_at': datetime.now(),
                'created_by': 'auto_upload',
                'status': 'active'
            }
            
            # 插入到数据库
            self.stores_collection.insert_one(store_data)
            return store_data
            
        except Exception as e:
            print(f"创建门店失败: {e}")
            return None
    
    def _generate_store_code(self, store_name: str) -> str:
        """生成门店代码"""
        try:
            import hashlib
            # 使用门店名称生成短码
            hash_obj = hashlib.md5(store_name.encode('utf-8'))
            short_hash = hash_obj.hexdigest()[:6].upper()
            return f"AUTO_{short_hash}"
        except Exception:
            # 如果出错，使用时间戳
            return f"AUTO_{int(time.time()) % 100000}"
    
    def process_excel_file(self, file_buffer, report_month: str, progress_callback=None) -> Dict:
        """处理Excel文件并上传报表数据"""
        start_time = time.time()
        result = {
            'success_count': 0,
            'failed_count': 0,
            'errors': [],
            'processed_stores': [],
            'failed_stores': [],  # 存储失败的门店信息
            'total_time': 0
        }
        
        try:
            # 读取所有sheet
            if progress_callback:
                progress_callback(10, "正在读取Excel文件...")
            
            # 检查文件大小，防止内存溢出
            file_buffer.seek(0, 2)  # 移到文件末尾
            file_size = file_buffer.tell()
            file_buffer.seek(0)  # 重置到开头
            
            if file_size > 50 * 1024 * 1024:  # 50MB限制
                result['errors'].append("文件过大（超过50MB），请分批上传")
                return result
            
            excel_data = pd.read_excel(file_buffer, sheet_name=None, engine='openpyxl')
            total_sheets = len(excel_data)
            
            if total_sheets > 200:  # 限制工作表数量
                result['errors'].append(f"工作表数量过多（{total_sheets}个），请分批上传（建议每次不超过200个）")
                return result
            
            if progress_callback:
                progress_callback(20, f"发现 {total_sheets} 个工作表，开始处理...")
            
            processed = 0
            
            for sheet_name, df in excel_data.items():
                try:
                    # 更新进度
                    processed += 1
                    progress = 20 + (processed / total_sheets) * 70
                    if progress_callback:
                        progress_callback(progress, f"正在处理: {sheet_name}")
                    
                    # 查找或创建对应门店
                    store = self.find_or_create_store(sheet_name)
                    
                    if not store:
                        result['failed_stores'].append({
                            'store_name': sheet_name,
                            'reason': '无法创建门店记录'
                        })
                        result['failed_count'] += 1
                        result['errors'].append(f"{sheet_name}: 无法创建门店记录")
                        continue
                    
                    # 处理报表数据
                    report_data = self._process_sheet_data(df, store, report_month, sheet_name)
                    
                    if report_data:
                        # 检查是否已存在相同月份的报表
                        existing_report = self.reports_collection.find_one({
                            'store_id': store['_id'],
                            'report_month': report_month
                        })
                        
                        if existing_report:
                            # 更新现有报表
                            self.reports_collection.replace_one(
                                {'_id': existing_report['_id']},
                                report_data
                            )
                        else:
                            # 插入新报表
                            self.reports_collection.insert_one(report_data)
                        
                        result['success_count'] += 1
                        result['processed_stores'].append({
                            'sheet_name': sheet_name,
                            'store_name': store['store_name'],
                            'store_code': store['store_code']
                        })
                    else:
                        result['failed_stores'].append({
                            'store_name': sheet_name,
                            'reason': '数据处理失败'
                        })
                        result['failed_count'] += 1
                        result['errors'].append(f"{sheet_name}: 数据处理失败")
                
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
    
    def _process_sheet_data(self, df: pd.DataFrame, store: Dict, report_month: str, sheet_name: str) -> Dict:
        """处理单个工作表的数据"""
        try:
            # 数据清洗和预处理
            df_cleaned = df.dropna(how='all').dropna(axis=1, how='all')
            
            if df_cleaned.empty:
                return None
            
            # 保存原始Excel数据（转换为可序列化的格式）
            raw_excel_data = df_cleaned.to_dict('records')
            
            # 构建报表数据结构
            report_data = {
                'store_id': store['_id'],
                'store_code': store['store_code'],
                'store_name': store['store_name'],
                'report_month': report_month,
                'sheet_name': sheet_name,
                'raw_excel_data': raw_excel_data,  # 存储原始Excel数据
                'financial_data': {},
                'uploaded_at': datetime.now(),
                'uploaded_by': 'bulk_upload'
            }
            
            # 解析财务数据 - 根据实际Excel格式调整
            financial_data = self._extract_financial_data(df_cleaned)
            report_data['financial_data'] = financial_data
            
            return report_data
            
        except Exception as e:
            print(f"处理sheet {sheet_name} 数据时出错: {e}")
            return None
    
    def _extract_financial_data(self, df: pd.DataFrame) -> Dict:
        """从DataFrame中提取财务数据"""
        financial_data = {
            'revenue': {},
            'cost': {},
            'profit': {},
            'receivables': {},  # 应收账款相关
            'other_metrics': {}
        }
        
        try:
            # 提取第82行合计列的应收未收金额
            row_82_value = None
            if len(df) >= 82:  # 确保有第82行
                # 查找"合计"列
                total_col_idx = None
                for col_idx, col_name in enumerate(df.columns):
                    if '合计' in str(col_name) or 'total' in str(col_name).lower():
                        total_col_idx = col_idx
                        break
                
                # 如果找到合计列，提取第82行的值
                if total_col_idx is not None and len(df) > 81:  # 第82行的索引是81
                    try:
                        row_82_value = float(df.iloc[81, total_col_idx])
                        financial_data['receivables']['net_amount'] = row_82_value
                        financial_data['other_metrics']['第82行合计'] = row_82_value
                    except (ValueError, TypeError, IndexError):
                        pass
            
            # 遍历所有数据提取其他财务指标
            for idx, row in df.iterrows():
                if len(row) < 2:
                    continue
                
                metric_name = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ""
                
                # 尝试从不同列获取数值
                value = None
                for col_idx in range(1, len(row)):
                    try:
                        if pd.notna(row.iloc[col_idx]):
                            value = float(row.iloc[col_idx])
                            break
                    except (ValueError, TypeError):
                        continue
                
                if value is None:
                    value = 0
                
                # 根据指标名称分类
                if any(keyword in metric_name for keyword in ['收入', '营收', '销售额', '营业收入']):
                    if '线上' in metric_name or '网上' in metric_name:
                        financial_data['revenue']['online_revenue'] = value
                    elif '线下' in metric_name or '门店' in metric_name:
                        financial_data['revenue']['offline_revenue'] = value
                    elif '总' in metric_name or '合计' in metric_name:
                        financial_data['revenue']['total_revenue'] = value
                    else:
                        financial_data['revenue']['total_revenue'] = value
                
                elif any(keyword in metric_name for keyword in ['成本', '费用', '支出']):
                    if '商品' in metric_name or '货物' in metric_name:
                        financial_data['cost']['product_cost'] = value
                    elif '租金' in metric_name or '房租' in metric_name:
                        financial_data['cost']['rent_cost'] = value
                    elif '人工' in metric_name or '工资' in metric_name or '薪酬' in metric_name:
                        financial_data['cost']['labor_cost'] = value
                    elif '总' in metric_name or '合计' in metric_name:
                        financial_data['cost']['total_cost'] = value
                    else:
                        financial_data['cost']['other_cost'] = value
                
                elif any(keyword in metric_name for keyword in ['利润', '盈利', '净利', '毛利']):
                    if '毛利' in metric_name:
                        financial_data['profit']['gross_profit'] = value
                    elif '净利' in metric_name:
                        financial_data['profit']['net_profit'] = value
                    else:
                        financial_data['profit']['total_profit'] = value
                
                elif any(keyword in metric_name for keyword in ['应收', '未收', '欠款', '应付', '待付']):
                    if '应收' in metric_name:
                        financial_data['receivables']['accounts_receivable'] = value
                    elif '未收' in metric_name:
                        financial_data['receivables']['uncollected_amount'] = value
                    elif '逾期' in metric_name:
                        financial_data['receivables']['overdue_amount'] = value
                    elif '应付' in metric_name:
                        financial_data['receivables']['accounts_payable'] = value
                
                # 存储所有指标到other_metrics用于调试
                if metric_name:
                    financial_data['other_metrics'][f"{idx+1}行_{metric_name}"] = value
            
            # 计算派生指标
            total_revenue = financial_data['revenue'].get('total_revenue', 0)
            if total_revenue == 0:
                total_revenue = (financial_data['revenue'].get('online_revenue', 0) + 
                               financial_data['revenue'].get('offline_revenue', 0))
                if total_revenue > 0:
                    financial_data['revenue']['total_revenue'] = total_revenue
            
            total_cost = financial_data['cost'].get('total_cost', 0)
            if total_cost == 0:
                total_cost = (financial_data['cost'].get('product_cost', 0) + 
                             financial_data['cost'].get('rent_cost', 0) + 
                             financial_data['cost'].get('labor_cost', 0) + 
                             financial_data['cost'].get('other_cost', 0))
                if total_cost > 0:
                    financial_data['cost']['total_cost'] = total_cost
            
            if total_revenue > 0 and total_cost > 0:
                financial_data['profit']['profit_margin'] = (total_revenue - total_cost) / total_revenue
            
            # 如果没有找到第82行的值，尝试从其他方式获取应收未收金额
            if row_82_value is None:
                for key, value in financial_data['other_metrics'].items():
                    if ('82' in key and '合计' in key) or ('应收' in key and '合计' in key):
                        financial_data['receivables']['net_amount'] = value
                        break
            
        except Exception as e:
            print(f"提取财务数据时出错: {e}")
        
        return financial_data
    
    def add_store_with_aliases(self, store_data: Dict, aliases: List[str] = None) -> bool:
        """添加门店信息，包含别名"""
        try:
            if aliases:
                store_data['aliases'] = aliases
            
            result = self.stores_collection.insert_one(store_data)
            return True
        except Exception as e:
            print(f"添加门店失败: {e}")
            return False
    
    def get_upload_statistics(self, report_month: str = None) -> Dict:
        """获取上传统计信息"""
        try:
            pipeline = []
            
            if report_month:
                pipeline.append({'$match': {'report_month': report_month}})
            
            pipeline.extend([
                {
                    '$group': {
                        '_id': None,
                        'total_reports': {'$sum': 1},
                        'total_revenue': {'$sum': '$financial_data.revenue.total_revenue'},
                        'total_receivables': {'$sum': '$financial_data.receivables.accounts_receivable'},
                        'total_uncollected': {'$sum': '$financial_data.receivables.uncollected_amount'}
                    }
                }
            ])
            
            result = list(self.reports_collection.aggregate(pipeline))
            
            if result:
                return result[0]
            else:
                return {
                    'total_reports': 0,
                    'total_revenue': 0,
                    'total_receivables': 0,
                    'total_uncollected': 0
                }
        
        except Exception as e:
            print(f"获取统计信息失败: {e}")
            return {}
    
    def close_connection(self):
        """关闭数据库连接"""
        self.client.close()

# 管理员验证
def verify_admin_password(password: str) -> bool:
    """验证管理员密码"""
    try:
        # 从Streamlit secrets获取管理员密码
        admin_password = st.secrets.get("security", {}).get("admin_password", "admin123")
        return password == admin_password
    except Exception:
        return password == "admin123"  # 默认密码

# Streamlit 上传界面
def create_upload_interface():
    """创建上传界面"""
    st.title("📤 批量报表上传系统")
    
    # 检查管理员登录状态
    if 'admin_authenticated' not in st.session_state:
        st.session_state.admin_authenticated = False
    
    if not st.session_state.admin_authenticated:
        # 管理员登录页面
        st.subheader("🔐 管理员登录")
        
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            admin_password = st.text_input(
                "管理员密码", 
                type="password", 
                placeholder="请输入管理员密码"
            )
            
            if st.button("登录", use_container_width=True):
                if admin_password:
                    if verify_admin_password(admin_password):
                        st.session_state.admin_authenticated = True
                        st.success("管理员登录成功！")
                        st.rerun()
                    else:
                        st.error("管理员密码错误")
                else:
                    st.warning("请输入管理员密码")
        return  # 未登录时直接返回，不显示上传界面
    
    # 初始化上传器
    uploader = BulkReportUploader()
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("上传设置")
        
        # 月份选择
        report_month = st.text_input(
            "报表月份",
            value=datetime.now().strftime("%Y-%m"),
            help="格式：YYYY-MM，例如：2024-08"
        )
        
        # 文件上传
        uploaded_file = st.file_uploader(
            "选择Excel文件",
            type=['xlsx', 'xls'],
            help="选择包含所有门店报表的Excel文件，每个工作表对应一个门店"
        )
        
        if uploaded_file and report_month:
            if st.button("开始上传", type="primary", use_container_width=True):
                # 创建进度条
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                def update_progress(progress, message):
                    progress_bar.progress(progress / 100)
                    status_text.text(message)
                
                # 处理文件
                with st.spinner("正在处理文件..."):
                    result = uploader.process_excel_file(
                        uploaded_file, 
                        report_month, 
                        progress_callback=update_progress
                    )
                
                # 显示结果
                st.subheader("📊 上传结果")
                
                col_success, col_failed, col_time = st.columns(3)
                
                with col_success:
                    st.metric("成功上传", result['success_count'], delta=None)
                
                with col_failed:
                    st.metric("失败数量", result['failed_count'], delta=None)
                
                with col_time:
                    st.metric("耗时(秒)", f"{result['total_time']:.2f}", delta=None)
                
                # 成功上传的门店列表
                if result['processed_stores']:
                    st.subheader("✅ 成功上传的门店")
                    success_df = pd.DataFrame(result['processed_stores'])
                    st.dataframe(success_df, use_container_width=True)
                
                # 上传失败信息
                if result['failed_stores']:
                    st.subheader("❌ 上传失败")
                    st.error(f"共 {result['failed_count']} 个门店上传失败")
                    
                    # 显示失败的门店列表
                    failed_df = pd.DataFrame(result['failed_stores'])
                    st.dataframe(failed_df, use_container_width=True)
                
                # 清理进度条
                progress_bar.empty()
                status_text.empty()
    
    with col2:
        st.subheader("📈 上传统计")
        
        # 获取当前月份统计
        current_stats = uploader.get_upload_statistics(report_month)
        
        if current_stats:
            st.metric("本月报表数", current_stats.get('total_reports', 0))
            st.metric("总收入", f"¥{current_stats.get('total_revenue', 0):,.2f}")
            st.metric("应收账款", f"¥{current_stats.get('total_receivables', 0):,.2f}")
            st.metric("未收金额", f"¥{current_stats.get('total_uncollected', 0):,.2f}")
        
        # 门店管理
        st.subheader("🏪 门店管理")
        if st.button("查看门店列表"):
            stores = list(uploader.stores_collection.find({}, {'password': 0}))
            if stores:
                stores_df = pd.DataFrame(stores)
                st.dataframe(stores_df[['store_name', 'store_code', 'region']], use_container_width=True)
            else:
                st.info("暂无门店数据")
        
        # 管理员退出登录
        st.markdown("---")
        if st.button("退出管理员登录", type="secondary"):
            st.session_state.admin_authenticated = False
            st.rerun()
    
    uploader.close_connection()

if __name__ == "__main__":
    create_upload_interface()
