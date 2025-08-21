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

# 密码验证
def verify_password(store_code: str, password: str, db) -> bool:
    """验证门店密码"""
    try:
        stores_collection = db['stores']
        store = stores_collection.find_one({'store_code': store_code})
        if store:
            return store.get('password') == password
        return False
    except Exception as e:
        st.error(f"验证失败: {e}")
        return False

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

# 获取报表数据
def get_report_data(store_id: str, months: List[str], db) -> List[Dict]:
    """获取指定月份的报表数据"""
    try:
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
        # 从financial_data中获取应收未收金额
        financial_data = report.get('financial_data', {})
        receivables = financial_data.get('receivables', {})
        
        # 如果有直接存储的应收未收金额
        if 'net_amount' in receivables:
            amount = receivables['net_amount']
        elif 'accounts_receivable' in receivables:
            amount = receivables['accounts_receivable']
        else:
            # 如果没有直接数据，尝试从other_metrics中查找
            other_metrics = financial_data.get('other_metrics', {})
            amount = 0
            for key, value in other_metrics.items():
                if '第82行' in key or '合计' in key or '应收' in key or '未收' in key:
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
    """显示应收未收金额看板"""
    st.subheader("💰 应收未收金额看板")
    
    if not reports:
        st.warning("暂无数据")
        return
    
    # 解析所有月份的应收未收数据
    receivables_data = []
    total_payable = 0  # 门店应付总额
    total_refundable = 0  # 总部应退总额
    
    for report in reports:
        receivables_info = parse_receivables_amount(report)
        receivables_data.append({
            'month': report['report_month'],
            'amount': receivables_info['amount'],
            'type': receivables_info['type'],
            'color': receivables_info['color'],
            'icon': receivables_info['icon']
        })
        
        if receivables_info['type'] == '门店应付':
            total_payable += receivables_info['amount']
        elif receivables_info['type'] == '总部应退':
            total_refundable += receivables_info['amount']
    
    # 顶部汇总指标
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="💳 门店应付总额",
            value=f"¥{total_payable:,.2f}",
            delta=None
        )
    
    with col2:
        st.metric(
            label="💰 总部应退总额", 
            value=f"¥{total_refundable:,.2f}",
            delta=None
        )
    
    with col3:
        net_amount = total_payable - total_refundable
        st.metric(
            label="📊 净应收金额",
            value=f"¥{net_amount:,.2f}",
            delta=f"{'门店净应付' if net_amount > 0 else '总部净应退' if net_amount < 0 else '已平衡'}"
        )
    
    with col4:
        latest_data = receivables_data[0] if receivables_data else None
        if latest_data:
            st.metric(
                label=f"{latest_data['icon']} 最新状态",
                value=f"¥{latest_data['amount']:,.2f}",
                delta=latest_data['type']
            )
    
    # 可视化图表
    col1, col2 = st.columns(2)
    
    with col1:
        # 应收未收趋势图
        if receivables_data:
            df = pd.DataFrame(receivables_data)
            
            # 为不同类型设置不同颜色
            colors = []
            for _, row in df.iterrows():
                if row['type'] == '门店应付':
                    colors.append('orange')
                elif row['type'] == '总部应退':
                    colors.append('red')
                else:
                    colors.append('green')
            
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=df['month'],
                y=df['amount'],
                marker_color=colors,
                text=df['type'],
                textposition='auto',
                name='应收未收金额'
            ))
            
            fig.update_layout(
                title='应收未收金额趋势',
                xaxis_title='月份',
                yaxis_title='金额 (¥)',
                showlegend=False
            )
            
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # 应收未收类型分布饼图
        if receivables_data:
            type_summary = {}
            for item in receivables_data:
                if item['type'] in type_summary:
                    type_summary[item['type']] += item['amount']
                else:
                    type_summary[item['type']] = item['amount']
            
            if type_summary:
                fig = px.pie(
                    values=list(type_summary.values()),
                    names=list(type_summary.keys()),
                    title='应收未收类型分布',
                    color_discrete_map={
                        '门店应付': 'orange',
                        '总部应退': 'red',
                        '已结清': 'green'
                    }
                )
                st.plotly_chart(fig, use_container_width=True)
    
    # 详细数据表
    st.subheader("📋 应收未收明细")
    if receivables_data:
        df = pd.DataFrame(receivables_data)
        df['金额'] = df['amount'].apply(lambda x: f"¥{x:,.2f}")
        df['状态'] = df.apply(lambda row: f"{row['icon']} {row['type']}", axis=1)
        
        display_df = df[['month', '金额', '状态']].copy()
        display_df.columns = ['月份', '金额', '状态']
        
        st.dataframe(display_df, use_container_width=True)

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
        # 登录页面
        st.subheader("🔐 门店登录")
        
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            store_code = st.text_input("门店代码", placeholder="请输入门店代码")
            password = st.text_input("查询密码", type="password", placeholder="请输入查询密码")
            
            if st.button("登录", use_container_width=True):
                if store_code and password:
                    if verify_password(store_code, password, db):
                        store_info = get_store_info(store_code, db)
                        if store_info:
                            st.session_state.authenticated = True
                            st.session_state.store_info = store_info
                            st.success("登录成功！")
                            st.rerun()
                        else:
                            st.error("获取门店信息失败")
                    else:
                        st.error("门店代码或密码错误")
                else:
                    st.warning("请输入门店代码和密码")
    
    else:
        # 已登录，显示报表页面
        store_info = st.session_state.store_info
        
        # 侧边栏
        with st.sidebar:
            st.subheader(f"欢迎 {store_info['store_name']}")
            st.write(f"门店代码: {store_info['store_code']}")
            st.write(f"区域: {store_info.get('region', '未知')}")
            
            # 获取可用月份
            available_months = get_available_months(store_info['_id'], db)
            
            if available_months:
                st.subheader("查询选项")
                selected_months = st.multiselect(
                    "选择查询月份",
                    options=available_months,
                    default=available_months[:3] if len(available_months) >= 3 else available_months
                )
                
                report_type = st.selectbox(
                    "选择报表类型",
                    options=["应收未收看板", "收入分析", "成本分析", "利润分析", "综合报表"]
                )
            else:
                st.warning("暂无可用报表数据")
                selected_months = []
                report_type = "应收未收看板"
            
            if st.button("退出登录"):
                st.session_state.authenticated = False
                st.session_state.store_info = None
                st.rerun()
        
        # 主内容区域
        if selected_months:
            reports = get_report_data(store_info['_id'], selected_months, db)
            
            if reports:
                if report_type == "应收未收看板":
                    display_receivables_dashboard(reports)
                elif report_type == "收入分析":
                    display_revenue_report(reports)
                elif report_type == "成本分析":
                    display_cost_report(reports)
                elif report_type == "利润分析":
                    display_profit_report(reports)
                else:  # 综合报表
                    display_receivables_dashboard(reports)
                    st.divider()
                    display_revenue_report(reports)
                    st.divider()
                    display_cost_report(reports)
                    st.divider()
                    display_profit_report(reports)
            else:
                st.warning("选定月份暂无报表数据")
        else:
            st.info("请在左侧选择要查询的月份")

if __name__ == "__main__":
    main()
