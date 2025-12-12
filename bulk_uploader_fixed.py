# bulk_uploader_fixed.py - 修复版批量上传器
import pandas as pd
import streamlit as st
from datetime import datetime
import time
import numpy as np
from typing import Dict, List, Tuple
from database_manager import get_database
from data_models import StoreModel, ReportModel
from config import ConfigManager

class BulkReportUploader:
    def __init__(self, db=None):
        """初始化批量上传器（兼容版本）"""
        self.db = db or get_database()
        self.stores_collection = self.db['stores']
        self.reports_collection = self.db['reports']
        
        # 创建索引以提高查询性能（如果不存在）
        self._create_indexes()
    
    def _create_indexes(self):
        """创建数据库索引"""
        try:
            # 这些索引可能已经在database_manager中创建，这里做防重复处理
            try:
                self.stores_collection.create_index([("store_code", 1)], unique=True, background=True)
            except Exception:
                pass  # 索引已存在
            
            try:
                self.stores_collection.create_index([("store_name", 1)], background=True)
            except Exception:
                pass
            
            try:
                self.reports_collection.create_index([
                    ("store_id", 1), 
                    ("report_month", -1)
                ], background=True)
            except Exception:
                pass
                
        except Exception as e:
            print(f"创建索引时发生错误: {e}")
    
    def normalize_store_name(self, sheet_name: str) -> str:
        """标准化门店名称，去除特殊字符和空格"""
        # 移除常见的前缀和后缀
        name = sheet_name.strip()
        name = name.replace('犀牛百货', '').replace('门店', '').replace('店', '')
        name = name.replace('(', '').replace(')', '').replace('（', '').replace('）', '')
        name = ''.join(name.split())  # 移除所有空格
        return name
    
    def find_or_create_store(self, sheet_name: str) -> Dict:
        """通过sheet名称查找门店，如果不存在则创建（兼容版本）"""
        normalized_name = self.normalize_store_name(sheet_name)
        
        # 首先尝试查找现有门店
        search_patterns = [
            {"store_name": sheet_name},  # 完全匹配
            {"store_name": {"$regex": normalized_name, "$options": "i"}},  # 标准化后匹配
            {"store_code": {"$regex": normalized_name, "$options": "i"}},  # 代码匹配
            {"aliases": {"$in": [sheet_name, normalized_name]}},  # 别名匹配
        ]
        
        for pattern in search_patterns:
            store = self.stores_collection.find_one(pattern)
            if store:
                return store
        
        # 如果没有找到，创建新门店
        return self._create_store_from_sheet_name(sheet_name)
    
    def _create_store_from_sheet_name(self, sheet_name: str) -> Dict:
        """从工作表名称创建新门店（使用统一数据模型）"""
        try:
            # 使用统一的数据模型创建门店
            store_data = StoreModel.create_store_document(
                store_name=sheet_name.strip(),
                aliases=[sheet_name.strip(), self.normalize_store_name(sheet_name)],
                created_by='bulk_upload'
            )
            
            # 插入到数据库
            self.stores_collection.insert_one(store_data)
            return store_data
            
        except Exception as e:
            print(f"创建门店失败: {e}")
            return None
    
    def process_excel_file(self, file_buffer, report_month: str, progress_callback=None) -> Dict:
        """处理Excel文件并上传报表数据（兼容版本）"""
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
            # 读取所有sheet
            if progress_callback:
                progress_callback(10, "正在读取Excel文件...")
            
            # 检查文件大小，防止内存溢出
            file_buffer.seek(0, 2)
            file_size = file_buffer.tell()
            file_buffer.seek(0)
            
            if file_size > 50 * 1024 * 1024:  # 50MB限制
                result['errors'].append("文件过大（超过50MB），请分批上传")
                return result
            
            excel_data = pd.read_excel(file_buffer, sheet_name=None, engine='openpyxl', header=None)
            total_sheets = len(excel_data)
            
            if total_sheets > 200:
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
        """处理单个工作表的数据（兼容版本）"""
        try:
            # 数据清洗和预处理 - 保留所有行，只删除完全空的列
            df_cleaned = df.dropna(axis=1, how='all')
            
            if df_cleaned.empty:
                return None
            
            # 使用统一的数据模型处理Excel数据
            standardized_data = ReportModel._dataframe_to_standard_format(df_cleaned)
            
            # 提取财务数据
            financial_data = self._extract_financial_data(df_cleaned)
            
            # 使用统一的数据模型创建报表文档
            report_data = ReportModel.create_report_document(
                store_data=store,
                report_month=report_month,
                excel_data=standardized_data,
                sheet_name=sheet_name,
                financial_data=financial_data,
                uploaded_by='bulk_upload'
            )
            
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
            'receivables': {},
            'other_metrics': {}
        }
        
        try:
            # 提取第41行第2个合计列的应收未收金额（适应新的查找逻辑）
            row_41_value = None
            if len(df) >= 41:  # 确保有第41行
                target_row_index = 40  # 第41行的索引是40
                
                # 查找"合计"列
                total_col_indices = []
                for col_idx in range(len(df.columns)):
                    if len(df) > 0:  # 检查是否有表头行
                        header_value = df.iloc[0, col_idx] if not pd.isna(df.iloc[0, col_idx]) else ""
                        if '合计' in str(header_value) or 'total' in str(header_value).lower():
                            total_col_indices.append(col_idx)
                
                # 检查第41行是否包含应收未收关键词
                if len(df) > target_row_index:
                    first_col_value = str(df.iloc[target_row_index, 0]) if not pd.isna(df.iloc[target_row_index, 0]) else ""
                    keywords = ['总部应收未收金额', '应收未收金额', '应收-未收额', '应收未收额', '应收-未收', '应收未收']
                    
                    if any(keyword in first_col_value for keyword in keywords):
                        # 使用第2个合计列（如果存在）
                        target_col_idx = None
                        if len(total_col_indices) >= 2:
                            target_col_idx = total_col_indices[1]  # 第2个合计列
                        elif len(total_col_indices) == 1:
                            target_col_idx = total_col_indices[0]  # 只有1个合计列
                        
                        if target_col_idx is not None:
                            try:
                                row_41_value = float(df.iloc[target_row_index, target_col_idx])
                                financial_data['receivables']['net_amount'] = row_41_value
                                financial_data['other_metrics']['第41行第2个合计列'] = row_41_value
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
            
        except Exception as e:
            print(f"提取财务数据时出错: {e}")
        
        return financial_data
    
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

# 管理员验证
def verify_admin_password(password: str) -> bool:
    """验证管理员密码"""
    return password == ConfigManager.get_admin_password()

# Streamlit 上传界面
def create_upload_interface():
    """创建上传界面"""
    st.title("📤 批量报表上传系统")
    
    # 检查管理员登录状态
    if 'admin_authenticated_bulk' not in st.session_state:
        st.session_state.admin_authenticated_bulk = False
    
    if not st.session_state.admin_authenticated_bulk:
        # 管理员登录页面
        st.subheader("🔐 管理员登录")
        
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            admin_password = st.text_input(
                "管理员密码", 
                type="password", 
                placeholder="请输入管理员密码",
                key="bulk_admin_password"
            )
            
            if st.button("登录", use_container_width=True, key="bulk_admin_login"):
                if admin_password:
                    if verify_admin_password(admin_password):
                        st.session_state.admin_authenticated_bulk = True
                        st.success("管理员登录成功！")
                        st.rerun()
                    else:
                        st.error("管理员密码错误")
                else:
                    st.warning("请输入管理员密码")
        return
    
    # 初始化上传器
    db = get_database()
    uploader = BulkReportUploader(db)
    
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
            stores = list(uploader.stores_collection.find({}, {'_id': 1, 'store_name': 1, 'store_code': 1, 'region': 1}))
            if stores:
                stores_df = pd.DataFrame(stores)
                st.dataframe(stores_df[['store_name', 'store_code', 'region']], use_container_width=True)
            else:
                st.info("暂无门店数据")
        
        # 管理员退出登录
        st.markdown("---")
        if st.button("退出管理员登录", type="secondary"):
            st.session_state.admin_authenticated_bulk = False
            st.rerun()

if __name__ == "__main__":
    create_upload_interface()
