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
@st.cache_resource
def init_mongodb():
    """初始化MongoDB连接"""
    try:
        # 使用配置管理器获取MongoDB配置
        mongodb_config = ConfigManager.get_mongodb_config()
        
        client = MongoClient(mongodb_config['uri'])
        db = client[mongodb_config['database_name']]
        
        # 测试连接
        db.command('ping')
        return db
    except Exception as e:
        st.error(f"数据库连接失败: {e}")
        return None

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

# 解析应收未收金额
def parse_receivables_amount(report: Dict) -> Dict:
    """从报表数据中解析应收未收金额（第82行合计列）"""
    try:
        amount = 0
        
        # 优先从原始Excel数据中查找第82行的合计列
        raw_data = report.get('raw_excel_data', [])
        if raw_data and len(raw_data) >= 82:  # 确保有第82行数据
            # 查找第82行数据（索引为81）
            row_82 = raw_data[81] if len(raw_data) > 81 else {}
            
            # 在第82行中查找"合计"列或相关列
            for key, value in row_82.items():
                key_str = str(key).lower()
                if '合计' in key_str or 'total' in key_str or '小计' in key_str:
                    try:
                        amount = float(value) if value is not None else 0
                        break
                    except (ValueError, TypeError):
                        continue
        
        # 如果原始数据中没找到，从financial_data中获取
        if amount == 0:
            financial_data = report.get('financial_data', {})
            receivables = financial_data.get('receivables', {})
            
            if 'net_amount' in receivables:
                amount = receivables['net_amount']
            elif 'accounts_receivable' in receivables:
                amount = receivables['accounts_receivable']
            else:
                # 从other_metrics中查找
                other_metrics = financial_data.get('other_metrics', {})
                for key, value in other_metrics.items():
                    if '第82行' in key and '合计' in key:
                        try:
                            amount = float(value)
                            break
                        except (ValueError, TypeError):
                            continue
        
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
                'icon': '💳'
            }
        else:
            return {
                'amount': 0,
                'type': '已结清',
                'color': 'green',
                'icon': '✅'
            }
    
    except Exception as e:
        st.error(f"解析应收未收金额失败: {e}")
        return {
            'amount': 0,
            'type': '数据异常',
            'color': 'gray',
            'icon': '❓'
        }

# 显示应收未收看板
def display_receivables_dashboard(reports: List[Dict]):
    """显示应收未收金额看板（简化版）"""
    st.subheader("💰 应收未收金额")
    
    if not reports:
        st.warning("暂无数据")
        return
    
    # 解析所有报表的应收未收数据并累计
    total_should_pay = 0  # 门店应付总额
    total_should_return = 0  # 总部应退总额
    
    for report in reports:
        receivables_info = parse_receivables_amount(report)
        
        # 累计总额
        if receivables_info['type'] == '门店应付':
            total_should_pay += receivables_info['amount']
        elif receivables_info['type'] == '总部应退':
            total_should_return += receivables_info['amount']
    
    # 显示汇总金额
    col1, col2 = st.columns(2)
    
    with col1:
        if total_should_pay > 0:
            st.metric(
                label="💳 门店应付",
                value=f"¥{total_should_pay:,.2f}"
            )
    
    with col2:
        if total_should_return > 0:
            st.metric(
                label="💰 总部应退",
                value=f"¥{total_should_return:,.2f}"
            )

# 显示完整门店报表（原始Excel数据）
def display_complete_report(reports: List[Dict], store_info: Dict):
    """显示完整门店报表原始数据"""
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
        # 直接显示原始Excel数据，不添加额外列
        try:
            df = pd.DataFrame(raw_data)
            st.dataframe(df, use_container_width=True)
            return df
            
        except Exception as e:
            st.error(f"显示报表数据时出错: {e}")
            # 使用备选方案
            df_backup = create_fallback_dataframe(latest_report, store_info, latest_report['report_month'])
            if df_backup is not None:
                st.dataframe(df_backup, use_container_width=True)
                return df_backup
            
    elif raw_data and isinstance(raw_data, dict):
        # 兼容旧的dict格式
        try:
            df = pd.DataFrame(raw_data)
            st.dataframe(df, use_container_width=True)
            return df
            
        except Exception as e:
            st.error(f"显示报表数据时出错: {e}")
            # 使用备选方案
            df_backup = create_fallback_dataframe(latest_report, store_info, latest_report['report_month'])
            if df_backup is not None:
                st.dataframe(df_backup, use_container_width=True)
                return df_backup
    else:
        # 如果没有原始数据，创建备选显示
        df_backup = create_fallback_dataframe(latest_report, store_info, latest_report['report_month'])
        if df_backup is not None:
            st.dataframe(df_backup, use_container_width=True)
            return df_backup
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
    db = init_mongodb()
    if db is None:
        st.stop()
    
    # 检查是否已登录
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False
    
    if not st.session_state.authenticated:
        # 查询码登录页面
        st.subheader("🔐 门店查询系统")
        
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
                
                # 底部：下载功能
                if df is not None and len(df) > 0:
                    st.divider()
                    st.subheader("📥 报表下载")
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        # CSV下载
                        csv_data = df.to_csv(index=False, encoding='utf-8-sig')
                        st.download_button(
                            label="📄 下载完整报表 (CSV)",
                            data=csv_data,
                            file_name=f"{store_info['store_name']}_报表.csv",
                            mime="text/csv",
                            use_container_width=True
                        )
                    
                    with col2:
                        # Excel下载
                        try:
                            if len(df) > 1000:
                                st.info("数据量较大，建议使用CSV格式")
                            
                            import io
                            excel_buffer = io.BytesIO()
                            with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                                df.to_excel(writer, sheet_name='门店报表', index=False)
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
