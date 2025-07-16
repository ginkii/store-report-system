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

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GoogleSheetsErrorTracker:
    """Google Sheets API 错误追踪工具"""
    
    def __init__(self):
        self.client = None
        self.test_results = []
        self.error_details = []
        self.permissions_tested = []
    
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
        
        if not success and error_info:
            self.error_details.append({
                'test_name': test_name,
                'error_info': error_info,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
    
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
    
    def test_1_basic_authentication(self):
        """测试1: 基础认证"""
        st.subheader("🔐 测试1: 基础认证")
        
        try:
            # 检查配置
            if "google_sheets" not in st.secrets:
                self.log_test_result("基础认证", False, "未找到 google_sheets 配置", "Missing secrets configuration")
                self.display_progress("未找到 google_sheets 配置", "error")
                return False
            
            config = st.secrets["google_sheets"]
            
            # 验证必要字段
            required_fields = ["type", "project_id", "private_key", "client_email", "client_id"]
            missing_fields = [field for field in required_fields if field not in config]
            
            if missing_fields:
                error_msg = f"缺少必要字段: {', '.join(missing_fields)}"
                self.log_test_result("基础认证", False, error_msg, error_msg)
                self.display_progress(error_msg, "error")
                return False
            
            # 显示配置信息
            self.display_progress(f"项目ID: {config.get('project_id')}", "info")
            self.display_progress(f"服务账户: {config.get('client_email')}", "info")
            
            # 创建认证
            scopes = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
                "https://www.googleapis.com/auth/drive.file"
            ]
            
            credentials = Credentials.from_service_account_info(config, scopes=scopes)
            self.client = gspread.authorize(credentials)
            
            self.log_test_result("基础认证", True, "认证成功", None)
            self.display_progress("基础认证成功", "success")
            return True
            
        except Exception as e:
            error_msg = f"认证失败: {str(e)}"
            self.log_test_result("基础认证", False, error_msg, traceback.format_exc())
            self.display_progress(error_msg, "error")
            return False
    
    def test_2_drive_permissions(self):
        """测试2: Google Drive 权限"""
        st.subheader("🗂️ 测试2: Google Drive 权限")
        
        if not self.client:
            self.display_progress("需要先通过基础认证", "error")
            return False
        
        # 测试2.1: 创建文件权限
        try:
            self.display_progress("测试创建文件权限...", "info")
            
            # 尝试创建一个简单的文件
            test_sheet = self.client.create("ErrorTracker_测试文件_请删除")
            sheet_id = test_sheet.id
            
            self.log_test_result("创建文件权限", True, f"成功创建文件: {sheet_id}", None)
            self.display_progress(f"文件创建成功: {sheet_id}", "success")
            
            # 测试2.2: 文件访问权限
            try:
                self.display_progress("测试文件访问权限...", "info")
                
                # 尝试访问文件
                worksheet = test_sheet.sheet1
                worksheet.update('A1', [['测试', '数据', datetime.now().strftime('%Y-%m-%d %H:%M:%S')]])
                
                self.log_test_result("文件访问权限", True, "成功写入数据", None)
                self.display_progress("文件访问成功", "success")
                
            except Exception as e:
                error_msg = f"文件访问失败: {str(e)}"
                self.log_test_result("文件访问权限", False, error_msg, traceback.format_exc())
                self.display_progress(error_msg, "error")
            
            # 测试2.3: 文件共享权限
            try:
                self.display_progress("测试文件共享权限...", "info")
                
                # 尝试设置文件权限（这是常见的403错误来源）
                test_sheet.share('', perm_type='anyone', role='reader')
                
                self.log_test_result("文件共享权限", True, "成功设置共享权限", None)
                self.display_progress("文件共享成功", "success")
                
            except Exception as e:
                error_msg = f"文件共享失败: {str(e)}"
                self.log_test_result("文件共享权限", False, error_msg, traceback.format_exc())
                self.display_progress(error_msg, "warning")
                # 共享权限失败不一定是致命错误
            
            # 测试2.4: 文件删除权限
            try:
                self.display_progress("测试文件删除权限...", "info")
                
                # 尝试删除文件
                self.client.del_spreadsheet(sheet_id)
                
                self.log_test_result("文件删除权限", True, "成功删除文件", None)
                self.display_progress("文件删除成功", "success")
                
            except Exception as e:
                error_msg = f"文件删除失败: {str(e)}"
                self.log_test_result("文件删除权限", False, error_msg, traceback.format_exc())
                self.display_progress(error_msg, "error")
            
            return True
            
        except Exception as e:
            error_msg = f"创建文件失败: {str(e)}"
            self.log_test_result("创建文件权限", False, error_msg, traceback.format_exc())
            self.display_progress(error_msg, "error")
            
            # 分析具体的403错误
            if "403" in str(e):
                self.analyze_403_error(str(e), "Drive API - 创建文件")
            
            return False
    
    def test_3_sheets_permissions(self):
        """测试3: Google Sheets 权限"""
        st.subheader("📊 测试3: Google Sheets 权限")
        
        if not self.client:
            self.display_progress("需要先通过基础认证", "error")
            return False
        
        try:
            # 创建测试表格
            self.display_progress("创建测试表格...", "info")
            test_sheet = self.client.create("ErrorTracker_Sheets测试_请删除")
            
            # 测试3.1: 基本读写权限
            try:
                self.display_progress("测试基本读写权限...", "info")
                
                worksheet = test_sheet.sheet1
                
                # 写入数据
                test_data = [
                    ['测试列1', '测试列2', '测试列3'],
                    ['数据1', '数据2', '数据3'],
                    ['时间', datetime.now().strftime('%Y-%m-%d %H:%M:%S'), '测试完成']
                ]
                worksheet.update('A1', test_data)
                
                # 读取数据
                read_data = worksheet.get_all_values()
                
                self.log_test_result("Sheets读写权限", True, f"成功读写数据: {len(read_data)} 行", None)
                self.display_progress(f"读写测试成功: {len(read_data)} 行数据", "success")
                
            except Exception as e:
                error_msg = f"Sheets读写失败: {str(e)}"
                self.log_test_result("Sheets读写权限", False, error_msg, traceback.format_exc())
                self.display_progress(error_msg, "error")
            
            # 测试3.2: 批量操作权限
            try:
                self.display_progress("测试批量操作权限...", "info")
                
                # 创建大量数据
                batch_data = []
                for i in range(100):
                    batch_data.append([f'行{i+1}', f'数据{i+1}', f'时间{i+1}'])
                
                worksheet.update('A5', batch_data)
                
                self.log_test_result("Sheets批量操作", True, f"成功批量写入 {len(batch_data)} 行", None)
                self.display_progress(f"批量操作成功: {len(batch_data)} 行", "success")
                
            except Exception as e:
                error_msg = f"批量操作失败: {str(e)}"
                self.log_test_result("Sheets批量操作", False, error_msg, traceback.format_exc())
                self.display_progress(error_msg, "error")
            
            # 测试3.3: 工作表管理权限
            try:
                self.display_progress("测试工作表管理权限...", "info")
                
                # 添加新工作表
                new_worksheet = test_sheet.add_worksheet(title="测试工作表2", rows=100, cols=10)
                
                # 删除工作表
                test_sheet.del_worksheet(new_worksheet)
                
                self.log_test_result("工作表管理权限", True, "成功创建和删除工作表", None)
                self.display_progress("工作表管理成功", "success")
                
            except Exception as e:
                error_msg = f"工作表管理失败: {str(e)}"
                self.log_test_result("工作表管理权限", False, error_msg, traceback.format_exc())
                self.display_progress(error_msg, "error")
            
            # 清理测试文件
            try:
                self.client.del_spreadsheet(test_sheet.id)
                self.display_progress("测试文件已清理", "success")
            except Exception as e:
                self.display_progress(f"清理测试文件失败: {str(e)}", "warning")
            
            return True
            
        except Exception as e:
            error_msg = f"Sheets权限测试失败: {str(e)}"
            self.log_test_result("Sheets权限测试", False, error_msg, traceback.format_exc())
            self.display_progress(error_msg, "error")
            
            # 分析具体的403错误
            if "403" in str(e):
                self.analyze_403_error(str(e), "Sheets API")
            
            return False
    
    def test_4_simulate_app_operations(self):
        """测试4: 模拟应用实际操作"""
        st.subheader("🔄 测试4: 模拟应用实际操作")
        
        if not self.client:
            self.display_progress("需要先通过基础认证", "error")
            return False
        
        try:
            # 模拟创建主数据表格
            self.display_progress("模拟创建主数据表格...", "info")
            main_sheet = self.client.create("门店报表系统数据_测试")
            
            # 模拟创建权限表
            try:
                self.display_progress("模拟创建权限表...", "info")
                
                permissions_ws = main_sheet.add_worksheet(title="store_permissions", rows=1000, cols=20)
                
                # 模拟权限数据
                permissions_data = [
                    ['门店名称', '人员编号', '更新时间'],
                    ['测试门店1', '001', datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
                    ['测试门店2', '002', datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
                ]
                permissions_ws.update('A1', permissions_data)
                
                self.log_test_result("权限表创建", True, "成功创建权限表", None)
                self.display_progress("权限表创建成功", "success")
                
            except Exception as e:
                error_msg = f"权限表创建失败: {str(e)}"
                self.log_test_result("权限表创建", False, error_msg, traceback.format_exc())
                self.display_progress(error_msg, "error")
            
            # 模拟创建报表数据表
            try:
                self.display_progress("模拟创建报表数据表...", "info")
                
                reports_ws = main_sheet.add_worksheet(title="store_reports", rows=2000, cols=10)
                
                # 模拟报表数据
                reports_data = [
                    ['门店名称', '报表数据JSON', '行数', '列数', '更新时间'],
                    ['测试门店1', '{"test": "data"}', '10', '5', datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
                ]
                reports_ws.update('A1', reports_data)
                
                self.log_test_result("报表数据表创建", True, "成功创建报表数据表", None)
                self.display_progress("报表数据表创建成功", "success")
                
            except Exception as e:
                error_msg = f"报表数据表创建失败: {str(e)}"
                self.log_test_result("报表数据表创建", False, error_msg, traceback.format_exc())
                self.display_progress(error_msg, "error")
            
            # 模拟大数据写入
            try:
                self.display_progress("模拟大数据写入...", "info")
                
                # 创建大量数据
                large_data = [['门店名称', '报表数据JSON', '行数', '列数', '更新时间']]
                for i in range(50):
                    large_data.append([
                        f'门店{i+1}',
                        f'{{"data": "large_test_data_{i}", "size": {i*100}}}',
                        str(i*10),
                        str(i*2),
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    ])
                
                # 分批写入
                batch_size = 15
                for i in range(0, len(large_data), batch_size):
                    batch = large_data[i:i+batch_size]
                    if i == 0:
                        reports_ws.update('A1', batch)
                    else:
                        reports_ws.update(f'A{i+1}', batch)
                    time.sleep(0.1)  # 避免API限制
                
                self.log_test_result("大数据写入", True, f"成功写入 {len(large_data)} 行数据", None)
                self.display_progress(f"大数据写入成功: {len(large_data)} 行", "success")
                
            except Exception as e:
                error_msg = f"大数据写入失败: {str(e)}"
                self.log_test_result("大数据写入", False, error_msg, traceback.format_exc())
                self.display_progress(error_msg, "error")
            
            # 清理测试文件
            try:
                self.client.del_spreadsheet(main_sheet.id)
                self.display_progress("测试文件已清理", "success")
            except Exception as e:
                self.display_progress(f"清理测试文件失败: {str(e)}", "warning")
            
            return True
            
        except Exception as e:
            error_msg = f"应用操作模拟失败: {str(e)}"
            self.log_test_result("应用操作模拟", False, error_msg, traceback.format_exc())
            self.display_progress(error_msg, "error")
            
            # 分析具体的403错误
            if "403" in str(e):
                self.analyze_403_error(str(e), "应用操作模拟")
            
            return False
    
    def analyze_403_error(self, error_str: str, context: str):
        """分析403错误的具体原因"""
        st.subheader(f"🔍 403错误分析 - {context}")
        
        error_analysis = {
            'context': context,
            'error_string': error_str,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'possible_causes': [],
            'solutions': []
        }
        
        # 分析具体的403错误类型
        if "insufficient permissions" in error_str.lower():
            error_analysis['possible_causes'].append("权限不足")
            error_analysis['solutions'].append("检查服务账户是否有足够的权限")
        
        if "quota exceeded" in error_str.lower():
            error_analysis['possible_causes'].append("配额超限")
            error_analysis['solutions'].append("等待配额重置或升级账户")
        
        if "api not enabled" in error_str.lower():
            error_analysis['possible_causes'].append("API未启用")
            error_analysis['solutions'].append("在Google Cloud Console中启用相关API")
        
        if "invalid credentials" in error_str.lower():
            error_analysis['possible_causes'].append("认证凭据无效")
            error_analysis['solutions'].append("检查服务账户密钥是否正确")
        
        if "access denied" in error_str.lower():
            error_analysis['possible_causes'].append("访问被拒绝")
            error_analysis['solutions'].append("检查IAM权限设置")
        
        # 显示分析结果
        st.error(f"**错误上下文**: {context}")
        st.error(f"**错误信息**: {error_str}")
        
        if error_analysis['possible_causes']:
            st.warning("**可能的原因**:")
            for cause in error_analysis['possible_causes']:
                st.write(f"- {cause}")
        
        if error_analysis['solutions']:
            st.info("**建议的解决方案**:")
            for solution in error_analysis['solutions']:
                st.write(f"- {solution}")
        
        self.error_details.append(error_analysis)
    
    def display_summary(self):
        """显示测试总结"""
        st.subheader("📋 测试总结")
        
        if not self.test_results:
            st.warning("没有测试结果")
            return
        
        # 创建结果表格
        df = pd.DataFrame(self.test_results)
        
        # 显示成功/失败统计
        success_count = len([r for r in self.test_results if r['success']])
        total_count = len(self.test_results)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("总测试数", total_count)
        with col2:
            st.metric("成功数", success_count)
        with col3:
            st.metric("失败数", total_count - success_count)
        
        # 显示详细结果
        st.dataframe(df, use_container_width=True)
        
        # 显示错误详情
        if self.error_details:
            st.subheader("❌ 错误详情")
            for error in self.error_details:
                with st.expander(f"错误: {error['context']} - {error['timestamp']}"):
                    st.code(error['error_string'])
                    if error['possible_causes']:
                        st.write("**可能原因**:")
                        for cause in error['possible_causes']:
                            st.write(f"- {cause}")
                    if error['solutions']:
                        st.write("**解决方案**:")
                        for solution in error['solutions']:
                            st.write(f"- {solution}")
    
    def run_full_diagnostic(self):
        """运行完整诊断"""
        st.title("🔍 Google Sheets API 错误追踪诊断")
        
        st.markdown("""
        这个工具会逐步测试各种Google Sheets API操作，帮助精确定位403错误的原因。
        """)
        
        # 清空之前的结果
        self.test_results = []
        self.error_details = []
        
        # 运行测试
        test1_success = self.test_1_basic_authentication()
        
        if test1_success:
            test2_success = self.test_2_drive_permissions()
            test3_success = self.test_3_sheets_permissions()
            test4_success = self.test_4_simulate_app_operations()
        
        # 显示总结
        self.display_summary()
        
        # 提供建议
        st.subheader("🎯 下一步建议")
        
        failed_tests = [r for r in self.test_results if not r['success']]
        
        if not failed_tests:
            st.success("🎉 所有测试都通过了！你的Google Sheets配置是正确的。")
            st.info("如果你的应用仍然出现403错误，可能是代码逻辑问题或特定操作的权限问题。")
        else:
            st.error(f"发现 {len(failed_tests)} 个问题:")
            for test in failed_tests:
                st.write(f"- **{test['test_name']}**: {test['details']}")
            
            st.info("请根据上面的错误分析和解决方案来修复这些问题。")

# 在你的Streamlit应用中使用这个工具
def run_error_diagnostic():
    """运行错误诊断工具"""
    tracker = GoogleSheetsErrorTracker()
    tracker.run_full_diagnostic()

# 在管理员界面中添加这个诊断功能
if __name__ == "__main__":
    run_error_diagnostic()

st.write("当前服务账户:", st.secrets["google_sheets"]["client_email"])
