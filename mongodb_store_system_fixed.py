import streamlit as st
import pandas as pd
import pymongo
from pymongo import MongoClient
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import hashlib
import json
import os
from typing import Dict, List, Optional
import numpy as np
from config_manager import ConfigManager

# 页面配置（仅在直接运行时设置）
if __name__ == "__main__":
    st.set_page_config(
        page_title="门店报表查询系统",
        page_icon="🏪",
        layout="wide",
        initial_sidebar_state="expanded"
    )

# MongoDB连接配置
from database_manager import get_database, get_database_client
from data_models import StoreModel, ReportModel, PermissionModel
from config import ConfigManager

def init_mongodb():
    """初始化MongoDB连接 - 使用统一的数据库管理"""
    try:
        return get_database_client()
    except Exception as e:
        st.error(f"数据库连接失败: {e}")
        return None, None

# 管理员功能：清除所有历史数据并上传新数据
def clear_all_data_and_upload(db, collection_name, new_data_list):
    """完全清除指定集合的所有数据并插入新数据"""
    try:
        collection = db[collection_name]
        
        # 删除集合中的所有文档
        delete_result = collection.delete_many({})
        st.info(f"已删除 {collection_name} 集合中的 {delete_result.deleted_count} 条历史记录")
        
        # 插入新数据
        if new_data_list and len(new_data_list) > 0:
            insert_result = collection.insert_many(new_data_list)
            st.success(f"已向 {collection_name} 集合插入 {len(insert_result.inserted_ids)} 条新记录")
        
        return True
    except Exception as e:
        st.error(f"清理并上传数据失败: {e}")
        return False

# 管理员界面
def show_admin_panel(db):
    """显示管理员控制面板"""
    st.subheader("👨‍💼 管理员控制面板")
    st.warning("⚠️ 上传新数据将完全替换所有历史数据")
    
    # 权限数据上传
    st.markdown("**权限数据管理**")
    permissions_file = st.file_uploader("上传权限数据 (Excel)", type=['xlsx', 'xls'], key="admin_permissions")
    
    if permissions_file:
        try:
            df = pd.read_excel(permissions_file)
            st.write("权限数据预览:")
            st.dataframe(df.head(), use_container_width=True)
            
            if st.button("完全替换权限数据", type="primary"):
                with st.spinner("正在清理历史数据并上传新数据..."):
                    # 转换为MongoDB文档格式
                    permission_docs = []
                    for _, row in df.iterrows():
                        permission_docs.append({
                            'query_code': str(row.iloc[1]).strip() if len(row) > 1 else '',
                            'store_id': str(row.iloc[0]).strip() if len(row) > 0 else '',
                            'created_at': datetime.now()
                        })
                    
                    if clear_all_data_and_upload(db, 'permissions', permission_docs):
                        st.balloons()
        except Exception as e:
            st.error(f"处理权限文件失败: {e}")
    
    st.divider()
    
    # 门店数据上传
    st.markdown("**门店数据管理**")
    stores_file = st.file_uploader("上传门店数据 (Excel)", type=['xlsx', 'xls'], key="admin_stores")
    
    if stores_file:
        try:
            df = pd.read_excel(stores_file)
            st.write("门店数据预览:")
            st.dataframe(df.head(), use_container_width=True)
            
            if st.button("完全替换门店数据", type="primary"):
                with st.spinner("正在清理历史数据并上传新数据..."):
                    # 转换为MongoDB文档格式
                    store_docs = []
                    for _, row in df.iterrows():
                        store_docs.append({
                            '_id': str(row.iloc[0]).strip() if len(row) > 0 else '',
                            'store_code': str(row.iloc[0]).strip() if len(row) > 0 else '',
                            'store_name': str(row.iloc[1]).strip() if len(row) > 1 else '',
                            'created_at': datetime.now()
                        })
                    
                    if clear_all_data_and_upload(db, 'stores', store_docs):
                        st.balloons()
        except Exception as e:
            st.error(f"处理门店文件失败: {e}")
    
    st.divider()
    
    # 报表数据上传
    st.markdown("**报表数据管理**")
    reports_file = st.file_uploader("上传报表数据 (Excel)", type=['xlsx', 'xls'], key="admin_reports")
    
    if reports_file:
        try:
            # 读取所有工作表
            excel_file = pd.ExcelFile(reports_file)
            sheet_names = excel_file.sheet_names
            
            st.write(f"发现 {len(sheet_names)} 个工作表:")
            for sheet in sheet_names:
                st.write(f"- {sheet}")
            
            if st.button("完全替换报表数据", type="primary"):
                with st.spinner("正在清理历史数据并上传新数据..."):
                    report_docs = []
                    
                    for sheet_name in sheet_names:
                        try:
                            # 读取Excel，使用标准化处理
                            df = pd.read_excel(reports_file, sheet_name=sheet_name, header=None)
                            
                            # 确保数据框不为空
                            if len(df) == 0:
                                continue
                            
                            # 使用统一的数据模型处理Excel数据
                            standardized_data = ReportModel._dataframe_to_standard_format(df)
                            
                            # 创建门店数据（如果不存在）
                            store_data = StoreModel.create_store_document(
                                store_name=sheet_name,
                                created_by='admin_upload'
                            )
                            
                            # 创建报表文档
                            report_doc = ReportModel.create_report_document(
                                store_data=store_data,
                                report_month=datetime.now().strftime('%Y-%m'),
                                excel_data=standardized_data,
                                sheet_name=sheet_name,
                                uploaded_by='admin_upload'
                            )
                            
                            report_docs.append(report_doc)
                            
                        except Exception as e:
                            st.warning(f"处理工作表 {sheet_name} 失败: {e}")
                            continue
                    
                    if clear_all_data_and_upload(db, 'reports', report_docs):
                        st.balloons()
        except Exception as e:
            st.error(f"处理报表文件失败: {e}")
    
    st.divider()
    
    # 数据库状态
    st.markdown("**数据库状态**")
    try:
        permissions_count = db['permissions'].count_documents({})
        stores_count = db['stores'].count_documents({})
        reports_count = db['reports'].count_documents({})
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("权限数据", permissions_count)
        with col2:
            st.metric("门店数据", stores_count)
        with col3:
            st.metric("报表数据", reports_count)
    except Exception as e:
        st.error(f"获取数据库状态失败: {e}")

# 查询码验证
def verify_query_code(query_code: str, db) -> Optional[Dict]:
    """验证查询码并返回对应的门店（一对一关系）"""
    try:
        permissions_collection = db['permissions']
        permission = permissions_collection.find_one({'query_code': query_code})
        if permission:
            store_id = permission.get('store_id')
            if store_id:
                stores_collection = db['stores']
                store = stores_collection.find_one({'_id': store_id})
                return store
        return None
    except Exception as e:
        st.error(f"验证失败: {e}")
        return None

# 获取门店信息
def get_store_info(store_code: str, db) -> Optional[Dict]:
    """获取门店基本信息"""
    try:
        stores_collection = db['stores']
        store = stores_collection.find_one({'store_code': store_code})
        return store
    except Exception as e:
        st.error(f"获取门店信息失败: {e}")
        return None

# 获取报表数据（带限制）
def get_report_data(store_id: str, months: List[str], db) -> List[Dict]:
    """获取指定月份的报表数据"""
    try:
        # 限制最多查询12个月的数据，防止内存溢出
        if len(months) > 12:
            months = months[:12]
            st.warning("为避免内存超限，最多显示12个月的数据")
        
        reports_collection = db['reports']
        query = {
            'store_id': store_id,
            'report_month': {'$in': months}
        }
        reports = list(reports_collection.find(query).sort('report_month', -1))
        return reports
    except Exception as e:
        st.error(f"获取报表数据失败: {e}")
        return []

# 获取可用月份
def get_available_months(store_id: str, db) -> List[str]:
    """获取该门店所有可用的报表月份"""
    try:
        reports_collection = db['reports']
        months = reports_collection.distinct('report_month', {'store_id': store_id})
        return sorted(months, reverse=True)
    except Exception as e:
        st.error(f"获取可用月份失败: {e}")
        return []

# 解析应收未收金额 - 修改为查找第41行和第2个合计列
def parse_receivables_amount(report: Dict) -> Dict:
    """从报表数据中解析应收金额（第41行找第2个合计列取数值）"""
    try:
        amount = 0
        found = False
        
        # 从原始Excel数据中查找
        raw_data = report.get('raw_excel_data', [])
        
        if raw_data and len(raw_data) > 40:  # 确保有第41行
            # 重新构建数据矩阵
            max_cols = 0
            for row in raw_data:
                max_cols = max(max_cols, len(row))
            
            data_matrix = []
            for row_data in raw_data:
                row_values = []
                for col_idx in range(max_cols):
                    col_key = f"col_{col_idx}"
                    value = row_data.get(col_key, "")
                    row_values.append(value)
                data_matrix.append(row_values)
            
            # 第一步：在表头行（第1行，索引0）找到所有"合计"列的位置
            total_column_indices = []
            if len(data_matrix) > 0:
                header_row = data_matrix[0]  # 第1行作为表头
                for col_idx, header_value in enumerate(header_row):
                    if header_value and isinstance(header_value, str):
                        header_str = str(header_value).strip()
                        if '合计' in header_str or 'total' in header_str.lower() or '小计' in header_str:
                            total_column_indices.append(col_idx)
            
            # 第二步：查找第41行（索引40）
            if len(data_matrix) > 40:
                target_row = data_matrix[40]  # 第41行（索引40）
                
                # 获取第41行第一列的值
                first_col_value = ""
                if len(target_row) > 0 and target_row[0]:
                    first_col_value = str(target_row[0]).strip()
                
                # 检查关键词
                keywords = ['总部应收未收金额', '应收未收金额', '应收-未收额', '应收未收额', '应收-未收', '应收未收']
                
                keyword_found = False
                for keyword in keywords:
                    if keyword in first_col_value:
                        keyword_found = True
                        break
                
                if keyword_found:
                    # 查找第2个合计列
                    target_col_idx = None
                    if len(total_column_indices) >= 2:
                        target_col_idx = total_column_indices[1]  # 第2个合计列
                    elif len(total_column_indices) == 1:
                        target_col_idx = total_column_indices[0]  # 只有1个合计列
                    else:
                        # 没有找到合计列，查找最后一个有数据的列
                        for col_idx in range(len(target_row) - 1, -1, -1):
                            if col_idx < len(target_row) and target_row[col_idx] is not None:
                                value_str = str(target_row[col_idx]).strip()
                                if value_str != "":
                                    try:
                                        float(value_str)
                                        target_col_idx = col_idx
                                        break
                                    except (ValueError, TypeError):
                                        continue
                    
                    if target_col_idx is not None and target_col_idx < len(target_row):
                        value = target_row[target_col_idx]
                        if value is not None and str(value).strip() != '':
                            try:
                                cleaned = str(value).replace(',', '').replace('¥', '').replace('￥', '').strip()
                                
                                if cleaned.startswith('(') and cleaned.endswith(')'):
                                    cleaned = '-' + cleaned[1:-1]
                                
                                amount = float(cleaned)
                                found = True
                            except (ValueError, TypeError):
                                pass
            
            # 备用查找逻辑：如果第41行没找到，在其他行查找
            if not found:
                keywords = ['总部应收未收金额', '应收未收金额', '应收-未收额', '应收未收额', '应收-未收', '应收未收']
                
                for row_idx, row in enumerate(data_matrix):
                    if len(row) == 0:
                        continue
                    
                    first_col_value = ""
                    if row[0] is not None:
                        first_col_value = str(row[0]).strip()
                    
                    keyword_found = False
                    for keyword in keywords:
                        if keyword in first_col_value:
                            keyword_found = True
                            break
                    
                    if keyword_found:
                        # 查找第2个合计列（同样逻辑）
                        target_col_idx = None
                        if len(total_column_indices) >= 2:
                            target_col_idx = total_column_indices[1]  # 第2个合计列
                        elif len(total_column_indices) == 1:
                            target_col_idx = total_column_indices[0]  # 只有1个合计列
                        else:
                            # 没有找到合计列，查找最后一个有数据的列
                            for col_idx in range(len(row) - 1, -1, -1):
                                if col_idx < len(row) and row[col_idx] is not None:
                                    value_str = str(row[col_idx]).strip()
                                    if value_str != "":
                                        try:
                                            float(value_str)
                                            target_col_idx = col_idx
                                            break
                                        except (ValueError, TypeError):
                                            continue
                        
                        if target_col_idx is not None and target_col_idx < len(row):
                            value = row[target_col_idx]
                            if value is not None and str(value).strip() != '':
                                try:
                                    cleaned = str(value).replace(',', '').replace('¥', '').replace('￥', '').strip()
                                    
                                    if cleaned.startswith('(') and cleaned.endswith(')'):
                                        cleaned = '-' + cleaned[1:-1]
                                    
                                    amount = float(cleaned)
                                    found = True
                                    break
                                except (ValueError, TypeError):
                                    continue
        
        # 如果原始数据中没找到，从financial_data中获取
        if not found:
            financial_data = report.get('financial_data', {})
            receivables = financial_data.get('receivables', {})
            
            if 'net_amount' in receivables and receivables['net_amount'] != 0:
                amount = receivables['net_amount']
                found = True
            elif 'accounts_receivable' in receivables and receivables['accounts_receivable'] != 0:
                amount = receivables['accounts_receivable']
                found = True
        
        # 根据金额正负判断类型
        if amount < 0:
            return {
                'amount': abs(amount),
                'type': '总部应退',
                'color': 'red',
                'icon': '💰'
            }
        elif amount > 0:
            return {
                'amount': amount,
                'type': '门店应付',
                'color': 'orange',
                'icon': '💰'
            }
        else:
            return {
                'amount': 0,
                'type': '已结清',
                'color': 'green',
                'icon': '✅'
            }
    
    except Exception as e:
        return {
            'amount': 0,
            'type': '数据异常',
            'color': 'gray',
            'icon': '❓'
        }

# 显示应收未收看板
def display_receivables_dashboard(reports: List[Dict]):
    """显示应收未收金额看板（简化版）"""
    if not reports:
        st.warning("暂无数据")
        return
    
    # 解析最新报表的应收金额（不累计，只取一个报表的数据）
    display_type = "已结清"
    display_icon = "✅"
    display_amount = 0
    
    if reports:
        # 取最新的报表（按月份倒序排列后取第一个）
        latest_report = sorted(reports, key=lambda x: x['report_month'], reverse=True)[0]
        receivables_info = parse_receivables_amount(latest_report)
        
        # 直接使用解析结果
        display_type = receivables_info['type']
        display_icon = receivables_info['icon']
        display_amount = receivables_info['amount']
    
    # 显示大字体的金额指标，带背景渐变
    if display_amount > 0:
        if display_type == '总部应退':
            st.markdown(f"""
            <div style="
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                border-radius: 15px;
                padding: 30px;
                margin: 20px 0;
                text-align: center;
                box-shadow: 0 8px 32px rgba(102, 126, 234, 0.3);
            ">
                <h1 style="color: white; margin: 0; font-size: 2.5rem; font-weight: bold; text-shadow: 2px 2px 4px rgba(0,0,0,0.3);">
                    💰 总部应退
                </h1>
                <h2 style="margin: 15px 0 0 0; color: white; font-size: 2.2rem; text-shadow: 1px 1px 2px rgba(0,0,0,0.3);">
                    ¥{display_amount:,.2f}
                </h2>
                <p style="margin: 10px 0 0 0; color: white; opacity: 0.9; font-size: 0.9rem;">
                    数据来源: 第41行第2个合计列
                </p>
            </div>
            """, unsafe_allow_html=True)
        elif display_type == '门店应付':
            st.markdown(f"""
            <div style="
                background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
                border-radius: 15px;
                padding: 30px;
                margin: 20px 0;
                text-align: center;
                box-shadow: 0 8px 32px rgba(245, 87, 108, 0.3);
            ">
                <h1 style="color: white; margin: 0; font-size: 2.5rem; font-weight: bold; text-shadow: 2px 2px 4px rgba(0,0,0,0.3);">
                    💰 门店应付
                </h1>
                <h2 style="margin: 15px 0 0 0; color: white; font-size: 2.2rem; text-shadow: 1px 1px 2px rgba(0,0,0,0.3);">
                    ¥{display_amount:,.2f}
                </h2>
                <p style="margin: 10px 0 0 0; color: white; opacity: 0.9; font-size: 0.9rem;">
                    数据来源: 第41行第2个合计列
                </p>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown(f"""
            <div style="
                background: linear-gradient(135deg, #00cc88 0%, #00a86b 100%);
                border-radius: 15px;
                padding: 30px;
                margin: 20px 0;
                text-align: center;
                box-shadow: 0 8px 32px rgba(0, 204, 136, 0.3);
            ">
                <h1 style="color: white; margin: 0; font-size: 2.5rem; font-weight: bold; text-shadow: 2px 2px 4px rgba(0,0,0,0.3);">
                    ✅ 已结清
                </h1>
                <h2 style="margin: 15px 0 0 0; color: white; font-size: 2.2rem; text-shadow: 1px 1px 2px rgba(0,0,0,0.3);">
                    ¥{display_amount:,.2f}
                </h2>
                <p style="margin: 10px 0 0 0; color: white; opacity: 0.9; font-size: 0.9rem;">
                    数据来源: 第41行第2个合计列
                </p>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.markdown(f"""
        <div style="
            background: linear-gradient(135deg, #00cc88 0%, #00a86b 100%);
            border-radius: 15px;
            padding: 30px;
            margin: 20px 0;
            text-align: center;
            box-shadow: 0 8px 32px rgba(0, 204, 136, 0.3);
        ">
            <h1 style="color: white; margin: 0; font-size: 2.5rem; font-weight: bold; text-shadow: 2px 2px 4px rgba(0,0,0,0.3);">
                ✅ 已结清
            </h1>
            <h2 style="margin: 15px 0 0 0; color: white; font-size: 2.2rem; text-shadow: 1px 1px 2px rgba(0,0,0,0.3);">
                ¥0.00
            </h2>
            <p style="margin: 10px 0 0 0; color: white; opacity: 0.9; font-size: 0.9rem;">
                数据来源: 第41行第2个合计列
            </p>
        </div>
        """, unsafe_allow_html=True)

# 显示完整门店报表（原始Excel数据）- 修复表头问题
def display_complete_report(reports: List[Dict], store_info: Dict):
    """显示完整门店报表原始数据，正确保留表头"""
    st.subheader("📊 门店报表数据")
    
    if not reports:
        st.warning("暂无报表数据")
        return None
    
    # 直接显示最新报表的原始Excel数据
    # 按月份倒序排列，显示最新的报表
    reports_sorted = sorted(reports, key=lambda x: x['report_month'], reverse=True)
    latest_report = reports_sorted[0]
    
    # 获取原始Excel数据
    raw_data = latest_report.get('raw_excel_data')
    
    if raw_data and isinstance(raw_data, list):
        try:
            # 重新构建DataFrame，保持原始Excel结构
            # 确定最大列数
            max_cols = 0
            for row in raw_data:
                max_cols = max(max_cols, len(row))
            
            # 重新构建数据矩阵，保持原始行列结构
            data_matrix = []
            for row_data in raw_data:
                row_values = []
                for col_idx in range(max_cols):
                    col_key = f"col_{col_idx}"
                    value = row_data.get(col_key, "")
                    # 保持原始值，不做过度处理
                    if value is None or (isinstance(value, str) and value.strip() == ""):
                        row_values.append("")
                    else:
                        row_values.append(value)
                data_matrix.append(row_values)
            
            # 创建DataFrame，使用第一行作为表头
            if len(data_matrix) > 1:
                # 第一行作为表头
                header_row = data_matrix[0]
                data_rows = data_matrix[1:]
                
                # 处理表头，确保列名有效且唯一
                processed_headers = []
                for i, header in enumerate(header_row):
                    if header is None or str(header).strip() == "" or str(header).lower() in ['nan', 'none']:
                        header_name = f"列{i+1}" if i > 0 else "项目名称"
                    else:
                        header_name = str(header).strip()
                    
                    # 处理重复表头
                    original_header = header_name
                    counter = 1
                    while header_name in processed_headers:
                        header_name = f"{original_header}_{counter}"
                        counter += 1
                    
                    processed_headers.append(header_name)
                
                # 创建DataFrame
                df = pd.DataFrame(data_rows, columns=processed_headers)
            else:
                # 如果只有一行，直接使用默认列名
                df = pd.DataFrame(data_matrix)
                df.columns = [f"列{i+1}" for i in range(len(df.columns))]
            
            # 格式化数值显示，保留2位小数
            df_display = df.copy()
            for col in df_display.columns:
                try:
                    # 尝试转换为数值类型
                    numeric_series = pd.to_numeric(df_display[col], errors='coerce')
                    
                    # 如果整列都能转换为数值，则格式化
                    if not numeric_series.isna().all():
                        # 对每个单元格单独处理
                        new_values = []
                        for original_val, numeric_val in zip(df_display[col], numeric_series):
                            if pd.notna(numeric_val) and str(original_val).strip() != "":
                                # 如果是数值且不为空，格式化为2位小数
                                new_values.append(round(numeric_val, 2))
                            else:
                                # 保持原值（文字或空值）
                                new_values.append(original_val if pd.notna(original_val) else "")
                        df_display[col] = new_values
                    else:
                        # 如果不是数值列，清理空值
                        df_display[col] = df_display[col].fillna("")
                except:
                    # 如果处理失败，保持原样
                    df_display[col] = df_display[col].fillna("")
            
            st.dataframe(df_display, use_container_width=True)
            return df_display
            
        except Exception as e:
            st.error(f"显示报表数据时出错: {e}")
            return None
            
    else:
        st.warning("暂无详细数据")
        return None

def create_fallback_dataframe(report: Dict, store_info: Dict, month: str) -> pd.DataFrame:
    """创建备选数据框（当原始Excel数据不可用时）"""
    try:
        # 从financial_data的other_metrics中获取所有数据
        financial_data = report.get('financial_data', {})
        other_metrics = financial_data.get('other_metrics', {})
        
        if other_metrics:
            # 创建显示所有other_metrics数据的DataFrame
            data_rows = []
            for key, value in other_metrics.items():
                data_rows.append({
                    '项目': key,
                    '数值': value if pd.notna(value) else 0
                })
            
            df = pd.DataFrame(data_rows)
            
            # 添加基础信息
            df.insert(0, '报表月份', month)
            df.insert(0, '门店名称', store_info['store_name'])
            
            return df
        
        return None
        
    except Exception:
        return None

# 显示收入报表
def display_revenue_report(reports: List[Dict]):
    """显示收入相关报表"""
    st.subheader("📈 收入分析")
    
    if not reports:
        st.warning("暂无收入数据")
        return
    
    # 准备数据
    revenue_data = []
    for report in reports:
        financial_data = report.get('financial_data', {})
        revenue = financial_data.get('revenue', {})
        revenue_data.append({
            '月份': report['report_month'],
            '总收入': revenue.get('total_revenue', 0),
            '线上收入': revenue.get('online_revenue', 0),
            '线下收入': revenue.get('offline_revenue', 0),
            '增长率': revenue.get('growth_rate', 0)
        })
    
    df = pd.DataFrame(revenue_data)
    
    col1, col2 = st.columns(2)
    
    with col1:
        # 收入趋势图
        fig = px.line(df, x='月份', y=['总收入', '线上收入', '线下收入'], 
                     title='收入趋势分析')
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # 收入结构饼图
        if len(df) > 0:
            latest_data = df.iloc[0]
            if latest_data['线上收入'] > 0 or latest_data['线下收入'] > 0:
                fig = px.pie(values=[latest_data['线上收入'], latest_data['线下收入']], 
                            names=['线上收入', '线下收入'],
                            title=f'{latest_data["月份"]} 收入结构')
                st.plotly_chart(fig, use_container_width=True)
    
    # 数据表格
    st.subheader("收入明细表")
    st.dataframe(df, use_container_width=True)

# 显示成本报表
def display_cost_report(reports: List[Dict]):
    """显示成本相关报表"""
    st.subheader("💰 成本分析")
    
    if not reports:
        st.warning("暂无成本数据")
        return
    
    # 准备数据
    cost_data = []
    for report in reports:
        financial_data = report.get('financial_data', {})
        cost = financial_data.get('cost', {})
        cost_data.append({
            '月份': report['report_month'],
            '商品成本': cost.get('product_cost', 0),
            '租金成本': cost.get('rent_cost', 0),
            '人工成本': cost.get('labor_cost', 0),
            '其他成本': cost.get('other_cost', 0),
            '总成本': cost.get('total_cost', 0)
        })
    
    df = pd.DataFrame(cost_data)
    
    col1, col2 = st.columns(2)
    
    with col1:
        # 成本趋势图
        fig = px.line(df, x='月份', y='总成本', title='总成本趋势')
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # 成本结构图
        if len(df) > 0:
            latest_data = df.iloc[0]
            cost_breakdown = {
                '商品成本': latest_data['商品成本'],
                '租金成本': latest_data['租金成本'],
                '人工成本': latest_data['人工成本'],
                '其他成本': latest_data['其他成本']
            }
            # 过滤掉零值
            cost_breakdown = {k: v for k, v in cost_breakdown.items() if v > 0}
            if cost_breakdown:
                fig = px.bar(x=list(cost_breakdown.keys()), 
                            y=list(cost_breakdown.values()),
                            title=f'{latest_data["月份"]} 成本结构')
                st.plotly_chart(fig, use_container_width=True)
    
    # 数据表格
    st.subheader("成本明细表")
    st.dataframe(df, use_container_width=True)

# 显示利润报表
def display_profit_report(reports: List[Dict]):
    """显示利润相关报表"""
    st.subheader("📊 利润分析")
    
    if not reports:
        st.warning("暂无利润数据")
        return
    
    # 准备数据
    profit_data = []
    for report in reports:
        financial_data = report.get('financial_data', {})
        profit = financial_data.get('profit', {})
        profit_data.append({
            '月份': report['report_month'],
            '毛利润': profit.get('gross_profit', 0),
            '净利润': profit.get('net_profit', 0),
            '利润率': profit.get('profit_margin', 0)
        })
    
    df = pd.DataFrame(profit_data)
    
    col1, col2 = st.columns(2)
    
    with col1:
        # 利润趋势图
        fig = px.line(df, x='月份', y=['毛利润', '净利润'], 
                     title='利润趋势分析')
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # 利润率趋势
        fig = px.line(df, x='月份', y='利润率', 
                     title='利润率趋势')
        fig.update_yaxis(tickformat='.2%')
        st.plotly_chart(fig, use_container_width=True)
    
    # 数据表格
    st.subheader("利润明细表")
    st.dataframe(df, use_container_width=True)

# 主函数
def main():
    # 只在独立运行时显示标题
    if __name__ == "__main__":
        st.title("🏪 门店报表查询系统")
    
    # 验证配置
    if not ConfigManager.validate_config():
        st.info("💡 配置说明：请在 `.streamlit/secrets.toml` 文件中配置MongoDB连接信息")
        st.code("""
[mongodb]
uri = "mongodb+srv://username:password@cluster.mongodb.net/"
database_name = "store_reports"
        """)
    
    # 初始化数据库连接
    db, client = init_mongodb()
    if db is None:
        st.stop()
    
    # 侧边栏管理员功能
    with st.sidebar:
        st.title("⚙️ 系统管理")
        
        # 管理员登录
        admin_password = st.text_input("管理员密码", type="password", key="admin_pass")
        
        if admin_password == ConfigManager.get_admin_password():  # 使用统一配置
            st.success("👨‍💼 管理员模式")
            if st.checkbox("显示管理员面板"):
                show_admin_panel(db)
        elif admin_password:
            st.error("密码错误")
    
    # 检查是否已登录
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False
    
    if not st.session_state.authenticated:
        # 查询码登录页面
        # 居中显示标题
        st.markdown("<h2 style='text-align: center;'>🔐 门店查询系统</h2>", unsafe_allow_html=True)
        
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            query_code = st.text_input("查询编号", placeholder="请输入查询编号")
            
            if st.button("查询", use_container_width=True):
                if query_code:
                    store = verify_query_code(query_code, db)
                    if store:
                        st.session_state.authenticated = True
                        st.session_state.store_info = store
                        st.session_state.query_code = query_code
                        st.success(f"验证成功！进入 {store['store_name']} 报表系统")
                        st.rerun()
                    else:
                        st.error("查询编号无效")
                else:
                    st.warning("请输入查询编号")
    
    else:
        # 已登录，显示报表页面
        store_info = st.session_state.store_info
        query_code = st.session_state.query_code
        
        # 侧边栏
        with st.sidebar:
            st.info(f"当前门店: {store_info['store_name']}")
            
            if st.button("退出登录"):
                st.session_state.authenticated = False
                st.session_state.store_info = None
                st.session_state.query_code = None
                st.rerun()
        
        # 主内容区域
        st.title(f"📊 {store_info['store_name']}")
        
        # 自动获取所有可用月份的数据
        available_months = get_available_months(store_info['_id'], db)
        
        if available_months:
            reports = get_report_data(store_info['_id'], available_months, db)
            
            if reports:
                # 顶部：应收未收看板
                display_receivables_dashboard(reports)
                
                st.divider()
                
                # 中部：完整门店报表
                df = display_complete_report(reports, store_info)
                
                # 底部：下载功能 - 修复表头问题
                if df is not None and len(df) > 0:
                    st.divider()
                    st.subheader("📥 报表下载")
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        # CSV下载 - 确保列名正确
                        df_csv = df.copy()
                        
                        # 确保列名不为空且唯一
                        final_columns = []
                        for i, col in enumerate(df_csv.columns):
                            col_str = str(col) if pd.notna(col) else f'列{i+1}'
                            if col_str in ['nan', 'None', ''] or col_str.lower().startswith('unnamed'):
                                col_str = f'列{i+1}' if i > 0 else '项目名称'
                            
                            # 处理重复列名
                            original_col = col_str
                            counter = 1
                            while col_str in final_columns:
                                col_str = f"{original_col}_{counter}"
                                counter += 1
                            final_columns.append(col_str)
                        
                        df_csv.columns = final_columns
                        
                        # 格式化数值为2位小数
                        for col in df_csv.columns:
                            if df_csv[col].dtype in ['float64', 'float32']:
                                df_csv[col] = df_csv[col].round(2)
                            else:
                                # 尝试将可转换的字符串转为数值并格式化
                                try:
                                    numeric_series = pd.to_numeric(df_csv[col], errors='coerce')
                                    if not numeric_series.isna().all():  # 如果有数值
                                        df_csv[col] = numeric_series.round(2)
                                except:
                                    pass
                        
                        csv_data = df_csv.to_csv(index=False, encoding='utf-8-sig')
                        st.download_button(
                            label="📄 下载完整报表 (CSV)",
                            data=csv_data,
                            file_name=f"{store_info['store_name']}_报表.csv",
                            mime="text/csv",
                            use_container_width=True
                        )
                    
                    with col2:
                        # Excel下载 - 确保列名正确
                        try:
                            if len(df) > 1000:
                                st.info("数据量较大，建议使用CSV格式")
                            
                            df_excel = df.copy()
                            
                            # 确保列名不为空且唯一
                            final_columns = []
                            for i, col in enumerate(df_excel.columns):
                                col_str = str(col) if pd.notna(col) else f'列{i+1}'
                                if col_str in ['nan', 'None', ''] or col_str.lower().startswith('unnamed'):
                                    col_str = f'列{i+1}' if i > 0 else '项目名称'
                                
                                # 处理重复列名
                                original_col = col_str
                                counter = 1
                                while col_str in final_columns:
                                    col_str = f"{original_col}_{counter}"
                                    counter += 1
                                final_columns.append(col_str)
                            
                            df_excel.columns = final_columns
                            
                            # 格式化数值为2位小数（与CSV下载保持一致）
                            for col in df_excel.columns:
                                if df_excel[col].dtype in ['float64', 'float32']:
                                    df_excel[col] = df_excel[col].round(2)
                                else:
                                    # 尝试将可转换的字符串转为数值并格式化
                                    try:
                                        numeric_series = pd.to_numeric(df_excel[col], errors='coerce')
                                        if not numeric_series.isna().all():  # 如果有数值
                                            df_excel[col] = numeric_series.round(2)
                                    except:
                                        pass
                            
                            import io
                            excel_buffer = io.BytesIO()
                            with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                                df_excel.to_excel(writer, sheet_name='门店报表', index=False)
                            excel_data = excel_buffer.getvalue()
                            
                            st.download_button(
                                label="📊 下载完整报表 (Excel)",
                                data=excel_data,
                                file_name=f"{store_info['store_name']}_报表.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                use_container_width=True
                            )
                        except Exception as e:
                            st.error(f"Excel生成失败: {e}")
            else:
                st.warning("暂无报表数据")
        else:
            st.info("该门店暂无可用报表数据")

if __name__ == "__main__":
    main()
