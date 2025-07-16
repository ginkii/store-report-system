import streamlit as st
import pandas as pd
import json
from datetime import datetime
import time
import gspread
from google.oauth2.service_account import Credentials
import logging
from typing import Optional, Dict, Any, List
import traceback

class Enhanced403DiagnosticTool:
    """增强版403错误诊断工具 - 深度分析"""
    
    def __init__(self):
        self.client = None
        self.test_results = []
        self.service_account_info = {}
    
    def log_test_result(self, test_name: str, success: bool, details: str, error_info: str = None):
        """记录测试结果"""
        result = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'test_name': test_name,
            'success': success,
            'details': details,
            'error_info': error_info
        }
        self.test_results.append(result)
    
    def display_progress(self, message: str, status: str = "info"):
        """显示测试进度"""
        if status == "success":
            st.success(f"✅ {message}")
        elif status == "error":
            st.error(f"❌ {message}")
        elif status == "warning":
            st.warning(f"⚠️ {message}")
        else:
            st.info(f"🔍 {message}")
    
    def verify_service_account_identity(self):
        """验证服务账户身份信息"""
        st.subheader("🔐 服务账户身份验证")
        
        try:
            if "google_sheets" not in st.secrets:
                self.display_progress("未找到 google_sheets 配置", "error")
                return False
            
            config = st.secrets["google_sheets"]
            
            # 显示当前服务账户信息
            st.markdown("### 📋 当前服务账户信息")
            
            client_email = config.get('client_email', '未知')
            project_id = config.get('project_id', '未知')
            client_id = config.get('client_id', '未知')
            private_key_id = config.get('private_key_id', '未知')
            
            self.service_account_info = {
                'client_email': client_email,
                'project_id': project_id,
                'client_id': client_id,
                'private_key_id': private_key_id[:10] + '...' if private_key_id != '未知' else '未知'
            }
            
            # 创建信息表格
            info_data = [
                ['项目ID', project_id],
                ['服务账户邮箱', client_email],
                ['客户端ID', client_id],
                ['私钥ID', private_key_id[:10] + '...' if private_key_id != '未知' else '未知']
            ]
            
            df = pd.DataFrame(info_data, columns=['配置项', '值'])
            st.dataframe(df, use_container_width=True)
            
            # 检查是否是新账户
            if 'v2' in client_email or 'new' in client_email:
                st.success("✅ 检测到这可能是新创建的服务账户")
            else:
                st.warning("⚠️ 这可能仍是旧的服务账户")
            
            self.log_test_result("服务账户验证", True, f"当前使用账户: {client_email}", None)
            return True
            
        except Exception as e:
            error_msg = f"服务账户验证失败: {str(e)}"
            self.log_test_result("服务账户验证", False, error_msg, traceback.format_exc())
            self.display_progress(error_msg, "error")
            return False
    
    def test_authentication_detailed(self):
        """详细的认证测试"""
        st.subheader("🔐 详细认证测试")
        
        try:
            config = st.secrets["google_sheets"]
            
            # 测试不同的认证范围
            scope_tests = [
                {
                    'name': '基础Sheets权限',
                    'scopes': ["https://www.googleapis.com/auth/spreadsheets"]
                },
                {
                    'name': '基础Drive权限', 
                    'scopes': ["https://www.googleapis.com/auth/drive"]
                },
                {
                    'name': '完整权限组合',
                    'scopes': [
                        "https://www.googleapis.com/auth/spreadsheets",
                        "https://www.googleapis.com/auth/drive",
                        "https://www.googleapis.com/auth/drive.file"
                    ]
                }
            ]
            
            for scope_test in scope_tests:
                try:
                    self.display_progress(f"测试 {scope_test['name']}...", "info")
                    
                    credentials = Credentials.from_service_account_info(config, scopes=scope_test['scopes'])
                    client = gspread.authorize(credentials)
                    
                    self.log_test_result(f"认证-{scope_test['name']}", True, "认证成功", None)
                    self.display_progress(f"{scope_test['name']} 认证成功", "success")
                    
                    # 保存最完整的客户端
                    if scope_test['name'] == '完整权限组合':
                        self.client = client
                    
                except Exception as e:
                    error_msg = f"{scope_test['name']} 认证失败: {str(e)}"
                    self.log_test_result(f"认证-{scope_test['name']}", False, error_msg, traceback.format_exc())
                    self.display_progress(error_msg, "error")
            
            return self.client is not None
            
        except Exception as e:
            error_msg = f"认证测试失败: {str(e)}"
            self.log_test_result("详细认证测试", False, error_msg, traceback.format_exc())
            self.display_progress(error_msg, "error")
            return False
    
    def test_api_quotas(self):
        """测试API配额限制"""
        st.subheader("📊 API配额测试")
        
        if not self.client:
            self.display_progress("需要先通过认证", "error")
            return False
        
        try:
            # 测试1: 快速连续请求（测试每分钟限制）
            self.display_progress("测试每分钟请求限制...", "info")
            
            start_time = time.time()
            success_count = 0
            error_count = 0
            
            for i in range(10):  # 尝试10次快速请求
                try:
                    # 尝试创建和立即删除文件
                    test_sheet = self.client.create(f"配额测试_{i}_{int(time.time())}")
                    self.client.del_spreadsheet(test_sheet.id)
                    success_count += 1
                    time.sleep(0.1)  # 短暂延迟
                    
                except Exception as e:
                    error_count += 1
                    if "quota" in str(e).lower() or "limit" in str(e).lower():
                        self.display_progress(f"检测到配额限制: {str(e)}", "warning")
                        break
                    elif "403" in str(e):
                        self.display_progress(f"403错误 (第{i+1}次): {str(e)}", "error")
                        # 分析这个403错误
                        self.analyze_specific_403_error(str(e), f"配额测试第{i+1}次")
                        break
            
            end_time = time.time()
            duration = end_time - start_time
            
            result_msg = f"完成 {success_count} 次成功请求，{error_count} 次失败，耗时 {duration:.2f} 秒"
            
            if error_count == 0:
                self.log_test_result("API配额测试", True, result_msg, None)
                self.display_progress("API配额测试通过", "success")
            else:
                self.log_test_result("API配额测试", False, result_msg, f"失败次数: {error_count}")
                self.display_progress(f"API配额可能有限制: {result_msg}", "warning")
            
            return error_count == 0
            
        except Exception as e:
            error_msg = f"API配额测试失败: {str(e)}"
            self.log_test_result("API配额测试", False, error_msg, traceback.format_exc())
            self.display_progress(error_msg, "error")
            return False
    
    def test_project_billing_status(self):
        """测试项目计费状态"""
        st.subheader("💳 项目计费状态检查")
        
        # 这是一个间接测试，通过尝试特定操作来判断计费状态
        try:
            self.display_progress("检查项目计费状态...", "info")
            
            # 某些操作可能需要启用计费
            test_operations = [
                {
                    'name': '创建文件',
                    'action': lambda: self.client.create(f"计费测试_{int(time.time())}")
                },
            ]
            
            billing_issues = []
            
            for op in test_operations:
                try:
                    self.display_progress(f"测试 {op['name']}...", "info")
                    result = op['action']()
                    
                    # 清理测试文件
                    if hasattr(result, 'id'):
                        try:
                            self.client.del_spreadsheet(result.id)
                        except:
                            pass
                    
                    self.display_progress(f"{op['name']} 测试成功", "success")
                    
                except Exception as e:
                    error_str = str(e).lower()
                    if 'billing' in error_str or 'payment' in error_str or 'quota' in error_str:
                        billing_issues.append(f"{op['name']}: {str(e)}")
                        self.display_progress(f"{op['name']} 可能需要计费: {str(e)}", "warning")
                    else:
                        self.display_progress(f"{op['name']} 失败: {str(e)}", "error")
                        # 分析403错误
                        if "403" in str(e):
                            self.analyze_specific_403_error(str(e), f"计费测试-{op['name']}")
            
            if billing_issues:
                self.log_test_result("计费状态检查", False, f"发现 {len(billing_issues)} 个计费相关问题", str(billing_issues))
                return False
            else:
                self.log_test_result("计费状态检查", True, "未发现计费问题", None)
                return True
                
        except Exception as e:
            error_msg = f"计费状态检查失败: {str(e)}"
            self.log_test_result("计费状态检查", False, error_msg, traceback.format_exc())
            self.display_progress(error_msg, "error")
            return False
    
    def analyze_specific_403_error(self, error_str: str, context: str):
        """分析具体的403错误类型"""
        st.subheader(f"🔍 403错误深度分析 - {context}")
        
        error_lower = error_str.lower()
        error_type = "未知403错误"
        possible_solutions = []
        
        # 分析不同类型的403错误
        if "storage quota" in error_lower or "quota exceeded" in error_lower:
            error_type = "存储配额超限"
            possible_solutions = [
                "清理服务账户的文件",
                "创建新的服务账户",
                "升级Google Workspace计划"
            ]
        elif "rate limit" in error_lower or "too many requests" in error_lower:
            error_type = "请求频率限制"
            possible_solutions = [
                "减少API请求频率",
                "添加请求间延迟",
                "检查每分钟/每日配额"
            ]
        elif "insufficient permissions" in error_lower or "access denied" in error_lower:
            error_type = "权限不足"
            possible_solutions = [
                "检查服务账户IAM权限",
                "确认API已启用",
                "验证OAuth范围"
            ]
        elif "billing" in error_lower or "payment" in error_lower:
            error_type = "计费问题"
            possible_solutions = [
                "启用项目计费",
                "检查付款方式",
                "确认账户状态"
            ]
        elif "project" in error_lower:
            error_type = "项目配置问题"
            possible_solutions = [
                "确认项目ID正确",
                "检查项目状态",
                "验证API启用状态"
            ]
        
        # 显示分析结果
        st.error(f"**错误类型**: {error_type}")
        st.code(error_str)
        
        if possible_solutions:
            st.markdown("**建议的解决方案**:")
            for i, solution in enumerate(possible_solutions, 1):
                st.write(f"{i}. {solution}")
        
        # 记录到测试结果中
        self.log_test_result(f"403错误分析-{context}", False, f"错误类型: {error_type}", error_str)
        
        return error_type
    
    def comprehensive_403_test(self):
        """综合403错误测试"""
        st.subheader("🔬 综合403错误测试")
        
        if not self.client:
            self.display_progress("需要先通过认证", "error")
            return
        
        # 测试各种可能触发403的操作
        test_operations = [
            {
                'name': '创建简单文件',
                'action': lambda: self.client.create(f"简单测试_{int(time.time())}")
            },
            {
                'name': '创建大型文件',
                'action': lambda: self._create_large_file()
            },
            {
                'name': '快速连续操作',
                'action': lambda: self._rapid_operations()
            },
            {
                'name': '批量数据写入',
                'action': lambda: self._batch_write_test()
            }
        ]
        
        for op in test_operations:
            try:
                self.display_progress(f"执行 {op['name']}...", "info")
                result = op['action']()
                
                # 清理
                if hasattr(result, 'id'):
                    try:
                        self.client.del_spreadsheet(result.id)
                    except:
                        pass
                
                self.log_test_result(f"综合测试-{op['name']}", True, "操作成功", None)
                self.display_progress(f"{op['name']} 成功", "success")
                
            except Exception as e:
                self.log_test_result(f"综合测试-{op['name']}", False, f"操作失败: {str(e)}", traceback.format_exc())
                
                if "403" in str(e):
                    self.analyze_specific_403_error(str(e), op['name'])
                else:
                    self.display_progress(f"{op['name']} 失败: {str(e)}", "error")
    
    def _create_large_file(self):
        """创建大型文件测试"""
        sheet = self.client.create(f"大型测试_{int(time.time())}")
        worksheet = sheet.sheet1
        
        # 创建大量数据
        large_data = []
        for i in range(1000):
            large_data.append([f'数据{i}', f'内容{i}', f'测试{i}', datetime.now().strftime('%Y-%m-%d %H:%M:%S')])
        
        worksheet.update('A1', large_data)
        return sheet
    
    def _rapid_operations(self):
        """快速连续操作测试"""
        for i in range(5):
            sheet = self.client.create(f"快速测试_{i}_{int(time.time())}")
            self.client.del_spreadsheet(sheet.id)
            time.sleep(0.1)
        return None
    
    def _batch_write_test(self):
        """批量写入测试"""
        sheet = self.client.create(f"批量测试_{int(time.time())}")
        worksheet = sheet.sheet1
        
        # 多次批量写入
        for batch in range(5):
            data = [[f'批次{batch}行{row}列{col}' for col in range(10)] for row in range(100)]
            worksheet.update(f'A{batch*100+1}', data)
            time.sleep(0.5)
        
        return sheet
    
    def display_enhanced_summary(self):
        """显示增强版总结"""
        st.subheader("📋 深度诊断总结")
        
        if not self.test_results:
            st.warning("没有测试结果")
            return
        
        # 显示服务账户信息
        if self.service_account_info:
            st.markdown("### 🔐 当前服务账户")
            st.json(self.service_account_info)
        
        # 按类型分组显示结果
        categories = {
            '认证相关': [r for r in self.test_results if '认证' in r['test_name']],
            '配额相关': [r for r in self.test_results if '配额' in r['test_name']],
            '403错误': [r for r in self.test_results if '403' in r['test_name']],
            '其他测试': [r for r in self.test_results if not any(keyword in r['test_name'] for keyword in ['认证', '配额', '403'])]
        }
        
        for category, results in categories.items():
            if results:
                st.markdown(f"### {category}")
                df = pd.DataFrame(results)
                st.dataframe(df, use_container_width=True)
        
        # 总体统计
        success_count = len([r for r in self.test_results if r['success']])
        total_count = len(self.test_results)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("总测试数", total_count)
        with col2:
            st.metric("成功数", success_count)
        with col3:
            st.metric("失败数", total_count - success_count)
        
        # 关键建议
        failed_tests = [r for r in self.test_results if not r['success']]
        if failed_tests:
            st.subheader("🎯 关键建议")
            
            error_types = {}
            for test in failed_tests:
                if test['error_info']:
                    if "storage quota" in test['error_info'].lower():
                        error_types['存储配额'] = error_types.get('存储配额', 0) + 1
                    elif "rate limit" in test['error_info'].lower():
                        error_types['请求限制'] = error_types.get('请求限制', 0) + 1
                    elif "billing" in test['error_info'].lower():
                        error_types['计费问题'] = error_types.get('计费问题', 0) + 1
                    else:
                        error_types['其他'] = error_types.get('其他', 0) + 1
            
            for error_type, count in error_types.items():
                if error_type == '存储配额':
                    st.error(f"🚨 检测到 {count} 个存储配额问题")
                    st.markdown("""
                    **即使是新服务账户，仍然出现存储配额问题，可能的原因：**
                    1. 服务账户配置没有更新
                    2. 项目级别的存储限制
                    3. Google Cloud项目本身的问题
                    
                    **建议：**
                    - 确认Streamlit Cloud中的Secrets已更新
                    - 重启应用确保使用新配置
                    - 考虑升级到Google Workspace
                    """)
                elif error_type == '请求限制':
                    st.warning(f"⚠️ 检测到 {count} 个请求频率问题")
                elif error_type == '计费问题':
                    st.error(f"💳 检测到 {count} 个计费相关问题")
        else:
            st.success("🎉 所有测试都通过了！")
    
    def run_enhanced_diagnostic(self):
        """运行增强版诊断"""
        st.title("🔬 增强版403错误深度诊断")
        
        st.markdown("""
        这个增强版诊断工具会深度分析各种403错误的可能原因，包括：
        - 服务账户身份验证
        - API配额限制
        - 项目计费状态
        - 存储空间问题
        - 权限配置问题
        """)
        
        # 清空之前的结果
        self.test_results = []
        self.service_account_info = {}
        
        # 运行诊断
        step1 = self.verify_service_account_identity()
        
        if step1:
            step2 = self.test_authentication_detailed()
            
            if step2:
                step3 = self.test_api_quotas()
                step4 = self.test_project_billing_status()
                step5 = self.comprehensive_403_test()
        
        # 显示增强版总结
        self.display_enhanced_summary()

# 运行增强版诊断工具
def run_enhanced_diagnostic():
    """运行增强版诊断工具"""
    tracker = Enhanced403DiagnosticTool()
    tracker.run_enhanced_diagnostic()

if __name__ == "__main__":
    run_enhanced_diagnostic()
