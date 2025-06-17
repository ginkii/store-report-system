import streamlit as st
import pandas as pd
import io
import json
from datetime import datetime
import time
import gspread
from google.oauth2.service_account import Credentials
import openpyxl
from openpyxl import load_workbook
import tempfile
import os

# 页面配置
st.set_page_config(
    page_title="门店报表查询系统", 
    page_icon="📊",
    layout="wide"
)

# 系统配置
ADMIN_PASSWORD = "admin123"
PERMISSIONS_SHEET_NAME = "store_permissions"
REPORTS_SHEET_NAME = "store_reports"
COMMENTS_SHEET_NAME = "store_comments"
SYSTEM_INFO_SHEET_NAME = "system_info"

# CSS样式
st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        padding: 1rem 0;
        margin-bottom: 2rem;
    }
    .store-info {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 1.5rem;
        border-radius: 10px;
        margin: 1rem 0;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    .admin-panel {
        background: linear-gradient(135deg, #ffeaa7 0%, #fab1a0 100%);
        padding: 1.5rem;
        border-radius: 10px;
        border: 2px solid #fdcb6e;
        margin: 1rem 0;
    }
    .receivable-positive {
        background: linear-gradient(135deg, #ff9a9e 0%, #fecfef 100%);
        color: #721c24;
        padding: 2rem;
        border-radius: 15px;
        border: 3px solid #f093fb;
        margin: 1rem 0;
        text-align: center;
    }
    .receivable-negative {
        background: linear-gradient(135deg, #a8edea 0%, #d299c2 100%);
        color: #0c4128;
        padding: 2rem;
        border-radius: 15px;
        border: 3px solid #48cab2;
        margin: 1rem 0;
        text-align: center;
    }
    .dashboard-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 1.5rem;
        border-radius: 12px;
        margin: 0.5rem 0;
        text-align: center;
    }
    .comment-cell {
        background-color: #fff3cd;
        border: 1px solid #ffeaa7;
        border-radius: 4px;
        position: relative;
    }
    .comment-indicator {
        position: absolute;
        top: 2px;
        right: 2px;
        width: 8px;
        height: 8px;
        background-color: #ff6b35;
        border-radius: 50%;
    }
    .comment-tooltip {
        background-color: #f8f9fa;
        border: 1px solid #dee2e6;
        border-radius: 6px;
        padding: 8px;
        margin: 4px 0;
        font-size: 0.85em;
        color: #495057;
    }
    </style>
""", unsafe_allow_html=True)

def read_excel_with_comments(file_path_or_buffer):
    """读取Excel文件，包括单元格备注"""
    try:
        import tempfile
        import os
        
        # 如果是文件对象，保存为临时文件
        if hasattr(file_path_or_buffer, 'read'):
            file_path_or_buffer.seek(0)  # 重置文件指针
            
            # 创建临时文件
            with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_file:
                tmp_file.write(file_path_or_buffer.read())
                temp_path = tmp_file.name
            
            try:
                workbook = load_workbook(temp_path, data_only=False)
            finally:
                # 清理临时文件
                try:
                    os.unlink(temp_path)
                except:
                    pass
        else:
            workbook = load_workbook(file_path_or_buffer, data_only=False)
        
        sheets_data = {}
        
        for sheet_name in workbook.sheetnames:
            worksheet = workbook[sheet_name]
            
            # 获取数据范围
            max_row = worksheet.max_row
            max_col = worksheet.max_column
            
            if max_row == 0 or max_col == 0:
                continue
            
            # 读取数据
            data = []
            comments_data = {}
            
            # 先扫描所有单元格查找备注
            print(f"正在扫描工作表 {sheet_name}，最大行: {max_row}，最大列: {max_col}")
            
            for row in range(1, max_row + 1):
                row_data = []
                for col in range(1, max_col + 1):
                    cell = worksheet.cell(row=row, column=col)
                    cell_value = cell.value
                    
                    # 处理空值
                    if cell_value is None:
                        cell_value = ""
                    
                    row_data.append(cell_value)
                    
                    # 检查是否有备注
                    if cell.comment is not None:
                        cell_address = f"{row-1}_{col-1}"  # 转换为0基索引
                        comment_text = cell.comment.text
                        
                        print(f"发现备注在 {cell_address}: {comment_text}")
                        
                        if comment_text and comment_text.strip():
                            comments_data[cell_address] = {
                                'text': comment_text.strip(),
                                'row': row - 1,
                                'col': col - 1,
                                'cell_value': str(cell_value),
                                'author': getattr(cell.comment, 'author', 'Unknown')
                            }
                
                data.append(row_data)
            
            if data:
                # 创建DataFrame
                df = pd.DataFrame(data)
                print(f"工作表 {sheet_name}: {len(data)} 行, {len(data[0]) if data else 0} 列, {len(comments_data)} 个备注")
                
                sheets_data[sheet_name] = {
                    'dataframe': df,
                    'comments': comments_data
                }
        
        print(f"总共读取到 {len(sheets_data)} 个工作表")
        return sheets_data
    
    except Exception as e:
        print(f"读取Excel文件时出错: {str(e)}")
        import traceback
        print(f"详细错误: {traceback.format_exc()}")
        st.error(f"读取Excel文件时出错: {str(e)}")
        return None

def analyze_receivable_data(df, comments_data=None):
    """分析应收未收额数据 - 专门查找第69行，并包含备注信息"""
    result = {}
    
    if len(df.columns) == 0 or len(df) == 0:
        return result
    
    # 检查第一行是否是门店名称（通常第一行只有第一个单元格有值）
    first_row = df.iloc[0] if len(df) > 0 else None
    if first_row is not None:
        non_empty_count = sum(1 for val in first_row if pd.notna(val) and str(val).strip() != '')
        # 如果第一行只有1-2个非空值，可能是门店名称行
        if non_empty_count <= 2:
            # 跳过第一行，使用第二行作为新的第一行
            df = df.iloc[1:].reset_index(drop=True)
            result['skipped_store_name_row'] = True
            # 调整备注数据的行索引
            if comments_data:
                adjusted_comments = {}
                for key, comment in comments_data.items():
                    row, col = map(int, key.split('_'))
                    if row > 0:  # 跳过第一行的备注
                        new_key = f"{row-1}_{col}"
                        comment['row'] = row - 1
                        adjusted_comments[new_key] = comment
                comments_data = adjusted_comments
    
    # 查找第69行（如果跳过了第一行，实际是原始数据的第70行）
    target_row_index = 68  # 第69行
    
    if len(df) > target_row_index:
        row = df.iloc[target_row_index]
        first_col_value = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ""
        
        # 检查第一列是否包含"应收-未收额"相关关键词
        keywords = ['应收-未收额', '应收未收额', '应收-未收', '应收未收']
        
        for keyword in keywords:
            if keyword in first_col_value:
                # 查找该行中的数值（从后向前查找，通常合计在后面的列）
                for col_idx in range(len(row)-1, 0, -1):
                    val = row.iloc[col_idx]
                    if pd.notna(val) and str(val).strip() not in ['', 'None', 'nan']:
                        # 清理数值
                        cleaned = str(val).replace(',', '').replace('¥', '').replace('￥', '').strip()
                        
                        # 处理括号表示的负数
                        if cleaned.startswith('(') and cleaned.endswith(')'):
                            cleaned = '-' + cleaned[1:-1]
                        
                        try:
                            amount = float(cleaned)
                            if amount != 0:
                                result['应收-未收额'] = {
                                    'amount': amount,
                                    'column_name': str(df.columns[col_idx]),
                                    'row_name': first_col_value,
                                    'row_index': target_row_index,
                                    'actual_row_number': target_row_index + 1  # 实际行号
                                }
                                
                                # 查找该行的备注信息
                                row_comments = []
                                if comments_data:
                                    for col_i in range(len(row)):
                                        comment_key = f"{target_row_index}_{col_i}"
                                        if comment_key in comments_data:
                                            comment_info = comments_data[comment_key]
                                            row_comments.append({
                                                'column': str(df.columns[col_i]) if col_i < len(df.columns) else f'列{col_i+1}',
                                                'text': comment_info['text'],
                                                'author': comment_info.get('author', 'Unknown'),
                                                'cell_value': comment_info.get('cell_value', '')
                                            })
                                
                                if row_comments:
                                    result['应收-未收额']['comments'] = row_comments
                                
                                # 查找备注信息（通常在最后几列或特定的备注列）
                                remarks = []
                                for col in df.columns:
                                    col_lower = str(col).lower()
                                    if any(keyword in col_lower for keyword in ['备注', '说明', '注释', 'remark', 'note', '参考资', '单位']):
                                        # 找到备注列，提取第69行的备注
                                        remark_val = str(row[col]) if col in row.index else ""
                                        if remark_val and remark_val.strip() not in ['', 'nan', 'None', '0']:
                                            remarks.append(f"{col}: {remark_val}")
                                
                                # 也检查最后几列是否有备注信息
                                for col_idx in range(max(0, len(row)-3), len(row)):
                                    if col_idx < len(row) and col_idx != len(row)-1:  # 排除已经作为金额的列
                                        val = str(row.iloc[col_idx]) if pd.notna(row.iloc[col_idx]) else ""
                                        if val and val.strip() not in ['', 'nan', 'None', '0'] and not val.replace('.', '').replace('-', '').isdigit():
                                            col_name = df.columns[col_idx]
                                            if col_name not in [r.split(':')[0] for r in remarks]:  # 避免重复
                                                remarks.append(f"{col_name}: {val}")
                                
                                if remarks:
                                    result['应收-未收额']['remarks'] = remarks
                                
                                return result
                        except ValueError:
                            continue
                break
    
    # 如果第69行没找到，提供备用查找方案
    if '应收-未收额' not in result:
        # 在所有行中查找
        keywords = ['应收-未收额', '应收未收额', '应收-未收', '应收未收']
        
        for idx, row in df.iterrows():
            try:
                row_name = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ""
                
                if not row_name.strip():
                    continue
                
                # 检查是否匹配关键词
                for keyword in keywords:
                    if keyword in row_name:
                        # 查找该行中的数值
                        for col_idx in range(len(row)-1, 0, -1):
                            val = row.iloc[col_idx]
                            if pd.notna(val) and str(val).strip() not in ['', 'None', 'nan']:
                                cleaned = str(val).replace(',', '').replace('¥', '').replace('￥', '').strip()
                                
                                if cleaned.startswith('(') and cleaned.endswith(')'):
                                    cleaned = '-' + cleaned[1:-1]
                                
                                try:
                                    amount = float(cleaned)
                                    if amount != 0:
                                        result['应收-未收额'] = {
                                            'amount': amount,
                                            'column_name': str(df.columns[col_idx]),
                                            'row_name': row_name,
                                            'row_index': idx,
                                            'actual_row_number': idx + 1,
                                            'note': f'在第{idx+1}行找到（非第69行）'
                                        }
                                        
                                        # 查找该行的备注信息
                                        row_comments = []
                                        if comments_data:
                                            for col_i in range(len(row)):
                                                comment_key = f"{idx}_{col_i}"
                                                if comment_key in comments_data:
                                                    comment_info = comments_data[comment_key]
                                                    row_comments.append({
                                                        'column': str(df.columns[col_i]) if col_i < len(df.columns) else f'列{col_i+1}',
                                                        'text': comment_info['text'],
                                                        'author': comment_info.get('author', 'Unknown'),
                                                        'cell_value': comment_info.get('cell_value', '')
                                                    })
                                        
                                        if row_comments:
                                            result['应收-未收额']['comments'] = row_comments
                                        
                                        # 查找备注信息
                                        remarks = []
                                        for col in df.columns:
                                            col_lower = str(col).lower()
                                            if any(keyword in col_lower for keyword in ['备注', '说明', '注释', 'remark', 'note', '参考资', '单位']):
                                                remark_val = str(row[col]) if col in row.index else ""
                                                if remark_val and remark_val.strip() not in ['', 'nan', 'None', '0']:
                                                    remarks.append(f"{col}: {remark_val}")
                                        
                                        if remarks:
                                            result['应收-未收额']['remarks'] = remarks
                                        
                                        return result
                                except ValueError:
                                    continue
                        break
            except Exception:
                continue
    
    # 返回调试信息
    result['debug_info'] = {
        'total_rows': len(df),
        'checked_row_69': len(df) > target_row_index,
        'row_69_content': str(df.iloc[target_row_index].iloc[0]) if len(df) > target_row_index else 'N/A',
        'comments_count': len(comments_data) if comments_data else 0
    }
    
    return result

@st.cache_resource
def get_google_sheets_client():
    """获取Google Sheets客户端"""
    try:
        credentials_info = st.secrets["google_sheets"]
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        credentials = Credentials.from_service_account_info(credentials_info, scopes=scopes)
        return gspread.authorize(credentials)
    except Exception as e:
        st.error(f"连接失败: {str(e)}")
        return None

def get_or_create_spreadsheet(gc, name="门店报表系统数据"):
    """获取或创建表格"""
    try:
        return gc.open(name)
    except:
        return gc.create(name)

def get_or_create_worksheet(spreadsheet, name):
    """获取或创建工作表"""
    try:
        return spreadsheet.worksheet(name)
    except:
        return spreadsheet.add_worksheet(title=name, rows=1000, cols=20)

def save_permissions_to_sheets(df, gc):
    """保存权限数据"""
    try:
        spreadsheet = get_or_create_spreadsheet(gc)
        worksheet = get_or_create_worksheet(spreadsheet, PERMISSIONS_SHEET_NAME)
        
        worksheet.clear()
        time.sleep(1)
        
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        all_data = [['门店名称', '人员编号', '更新时间']]
        
        for _, row in df.iterrows():
            all_data.append([str(row.iloc[0]), str(row.iloc[1]), current_time])
        
        worksheet.update('A1', all_data)
        return True
    except Exception as e:
        st.error(f"保存失败: {str(e)}")
        return False

def load_permissions_from_sheets(gc):
    """加载权限数据"""
    try:
        spreadsheet = get_or_create_spreadsheet(gc)
        worksheet = spreadsheet.worksheet(PERMISSIONS_SHEET_NAME)
        data = worksheet.get_all_values()
        
        if len(data) <= 1:
            return None
        
        df = pd.DataFrame(data[1:], columns=['门店名称', '人员编号', '更新时间'])
        return df[['门店名称', '人员编号']]
    except:
        return None

def save_reports_to_sheets(reports_dict, gc):
    """保存报表数据 - 支持大文件完整保存，包括备注信息"""
    try:
        spreadsheet = get_or_create_spreadsheet(gc)
        
        # 保存报表数据
        reports_worksheet = get_or_create_worksheet(spreadsheet, REPORTS_SHEET_NAME)
        reports_worksheet.clear()
        time.sleep(1)
        
        # 保存备注数据
        comments_worksheet = get_or_create_worksheet(spreadsheet, COMMENTS_SHEET_NAME)
        comments_worksheet.clear()
        time.sleep(1)
        
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        reports_data = [['门店名称', '报表数据JSON', '行数', '列数', '更新时间', '分片序号', '总分片数']]
        comments_data = [['门店名称', '备注数据JSON', '更新时间']]
        
        for store_name, sheet_info in reports_dict.items():
            df = sheet_info['dataframe']
            comments = sheet_info.get('comments', {})
            
            try:
                # 清理数据
                df_cleaned = df.copy()
                for col in df_cleaned.columns:
                    df_cleaned[col] = df_cleaned[col].astype(str).replace('nan', '').replace('None', '')
                
                # 转换为JSON
                json_data = df_cleaned.to_json(orient='records', force_ascii=False)
                
                # 检查是否需要分片（每片最大40000字符）
                max_chunk_size = 40000
                if len(json_data) <= max_chunk_size:
                    # 不需要分片
                    reports_data.append([store_name, json_data, len(df), len(df.columns), current_time, "1", "1"])
                else:
                    # 需要分片存储
                    chunks = []
                    chunk_size = max_chunk_size
                    
                    # 将JSON数据分割成多个片段
                    for i in range(0, len(json_data), chunk_size):
                        chunks.append(json_data[i:i + chunk_size])
                    
                    total_chunks = len(chunks)
                    
                    # 保存每个分片
                    for idx, chunk in enumerate(chunks):
                        chunk_name = f"{store_name}_分片{idx+1}/{total_chunks}"
                        reports_data.append([chunk_name, chunk, len(df), len(df.columns), current_time, str(idx+1), str(total_chunks)])
                
                # 保存备注数据
                if comments:
                    comments_json = json.dumps(comments, ensure_ascii=False)
                    comments_data.append([store_name, comments_json, current_time])
                
            except Exception as e:
                st.warning(f"处理 {store_name} 时出错: {str(e)}")
                # 保存错误信息
                error_data = {
                    "error": str(e),
                    "rows": len(df),
                    "columns": len(df.columns)
                }
                reports_data.append([f"{store_name}_错误", json.dumps(error_data, ensure_ascii=False), len(df), len(df.columns), current_time, "1", "1"])
                continue
        
        # 保存报表数据
        if len(reports_data) > 1:
            batch_size = 20
            for i in range(1, len(reports_data), batch_size):
                batch_data = reports_data[i:i+batch_size]
                if i == 1:
                    reports_worksheet.update(f'A1', [reports_data[0]] + batch_data)
                else:
                    row_num = i + 1
                    reports_worksheet.update(f'A{row_num}', batch_data)
                time.sleep(0.5)
        
        # 保存备注数据
        if len(comments_data) > 1:
            comments_worksheet.update('A1', comments_data)
        
        return True
    except Exception as e:
        st.error(f"保存报表失败: {str(e)}")
        return False

def load_reports_from_sheets(gc):
    """加载报表数据，包括备注信息"""
    try:
        spreadsheet = get_or_create_spreadsheet(gc)
        
        # 加载报表数据
        reports_worksheet = spreadsheet.worksheet(REPORTS_SHEET_NAME)
        reports_data = reports_worksheet.get_all_values()
        
        # 加载备注数据
        try:
            comments_worksheet = spreadsheet.worksheet(COMMENTS_SHEET_NAME)
            comments_data = comments_worksheet.get_all_values()
            print(f"加载到 {len(comments_data)} 行备注数据")
        except:
            comments_data = []
            print("未找到备注数据工作表")
        
        # 处理备注数据
        comments_dict = {}
        if len(comments_data) > 1:
            for row in comments_data[1:]:
                if len(row) >= 2:
                    store_name = row[0]
                    comments_json = row[1]
                    try:
                        parsed_comments = json.loads(comments_json)
                        comments_dict[store_name] = parsed_comments
                        print(f"加载门店 {store_name} 的 {len(parsed_comments)} 个备注")
                    except Exception as e:
                        print(f"解析备注数据失败 {store_name}: {e}")
                        continue
        
        print(f"总共加载 {len(comments_dict)} 个门店的备注数据")
        
        if len(reports_data) <= 1:
            return {}
        
        reports_dict = {}
        
        # 处理分片数据的合并
        store_chunks = {}
        
        for row in reports_data[1:]:
            if len(row) >= 7:  # 确保有足够的列
                store_name = row[0]
                json_data = row[1]
                chunk_num = row[5] if len(row) > 5 else "1"
                total_chunks = row[6] if len(row) > 6 else "1"
                
                # 处理分片数据
                if "_分片" in store_name:
                    base_store_name = store_name.split("_分片")[0]
                    if base_store_name not in store_chunks:
                        store_chunks[base_store_name] = {}
                    store_chunks[base_store_name][int(chunk_num)] = json_data
                else:
                    # 非分片数据，直接处理
                    try:
                        df = pd.read_json(json_data, orient='records')
                        
                        # 处理DataFrame
                        df = process_dataframe(df)
                        
                        # 获取备注信息
                        store_comments = comments_dict.get(store_name, {})
                        
                        reports_dict[store_name] = {
                            'dataframe': df,
                            'comments': store_comments
                        }
                    except Exception as e:
                        print(f"解析 {store_name} 数据失败: {str(e)}")
                        continue
        
        # 处理分片数据
        for base_store_name, chunks in store_chunks.items():
            try:
                # 按顺序合并分片
                combined_json = ""
                for i in range(1, max(chunks.keys()) + 1):
                    if i in chunks:
                        combined_json += chunks[i]
                
                df = pd.read_json(combined_json, orient='records')
                df = process_dataframe(df)
                
                # 获取备注信息
                store_comments = comments_dict.get(base_store_name, {})
                
                reports_dict[base_store_name] = {
                    'dataframe': df,
                    'comments': store_comments
                }
            except Exception as e:
                print(f"合并分片数据失败 {base_store_name}: {str(e)}")
                continue
        
        print(f"最终加载 {len(reports_dict)} 个门店数据")
        return reports_dict
    
    except Exception as e:
        print(f"加载报表数据失败: {str(e)}")
        st.error(f"加载报表数据失败: {str(e)}")
        return {}

def process_dataframe(df):
    """处理DataFrame的通用方法"""
    # 检查第一行是否是门店名称
    if len(df) > 0:
        first_row = df.iloc[0]
        non_empty_count = sum(1 for val in first_row if pd.notna(val) and str(val).strip() != '')
        
        # 如果第一行只有少数非空值，可能是门店名称，跳过它
        if non_empty_count <= 2 and len(df) > 1:
            df = df.iloc[1:]
    
    # 如果有足够的行，使用第二行作为表头
    if len(df) > 1:
        header_row = df.iloc[0].fillna('').astype(str).tolist()
        data_rows = df.iloc[1:].copy()
        
        # 清理列名并处理重复
        cols = []
        for i, col in enumerate(header_row):
            col = str(col).strip()
            if col == '' or col == 'nan' or col == '0':
                col = f'列{i+1}' if i > 0 else '项目名称'
            
            # 处理重复列名
            original_col = col
            counter = 1
            while col in cols:
                col = f"{original_col}_{counter}"
                counter += 1
            cols.append(col)
        
        # 确保列数匹配
        min_cols = min(len(data_rows.columns), len(cols))
        cols = cols[:min_cols]
        data_rows = data_rows.iloc[:, :min_cols]
        
        data_rows.columns = cols
        data_rows = data_rows.reset_index(drop=True).fillna('')
        return data_rows
    else:
        # 处理少于3行的数据
        df_clean = df.fillna('')
        # 设置默认列名避免重复
        default_cols = []
        for i in range(len(df_clean.columns)):
            col_name = f'列{i+1}' if i > 0 else '项目名称'
            default_cols.append(col_name)
        df_clean.columns = default_cols
        return df_clean

def verify_user_permission(store_name, user_id, permissions_data):
    """验证用户权限"""
    if permissions_data is None or len(permissions_data.columns) < 2:
        return False
    
    store_col = permissions_data.columns[0]
    id_col = permissions_data.columns[1]
    
    for _, row in permissions_data.iterrows():
        stored_store = str(row[store_col]).strip()
        stored_id = str(row[id_col]).strip()
        
        if (store_name in stored_store or stored_store in store_name) and stored_id == str(user_id):
            return True
    
    return False

def find_matching_reports(store_name, reports_data):
    """查找匹配的报表"""
    matching = []
    for sheet_name in reports_data.keys():
        if store_name in sheet_name or sheet_name in store_name:
            matching.append(sheet_name)
    return matching

def display_comments_for_cell(comments_data, row_idx, col_idx):
    """显示特定单元格的备注"""
    if not comments_data:
        return None
    
    comment_key = f"{row_idx}_{col_idx}"
    if comment_key in comments_data:
        comment_info = comments_data[comment_key]
        return comment_info
    return None

def create_dataframe_with_comments(df, comments_data):
    """创建带备注标识的DataFrame显示"""
    if not comments_data:
        return df
    
    # 创建一个HTML表格来显示带备注的数据
    html_parts = ['<div style="overflow-x: auto;"><table style="width: 100%; border-collapse: collapse;">']
    
    # 表头
    html_parts.append('<thead><tr>')
    for col_idx, col_name in enumerate(df.columns):
        html_parts.append(f'<th style="border: 1px solid #ddd; padding: 8px; background-color: #f2f2f2;">{col_name}</th>')
    html_parts.append('</tr></thead>')
    
    # 表体
    html_parts.append('<tbody>')
    for row_idx, (_, row) in enumerate(df.iterrows()):
        html_parts.append('<tr>')
        for col_idx, (col_name, value) in enumerate(row.items()):
            comment_key = f"{row_idx}_{col_idx}"
            has_comment = comment_key in comments_data
            
            cell_style = "border: 1px solid #ddd; padding: 8px;"
            if has_comment:
                cell_style += " background-color: #fff3cd; position: relative;"
                comment_info = comments_data[comment_key]
                title_text = f"备注: {comment_info['text']}"
                if comment_info.get('author'):
                    title_text += f" (作者: {comment_info['author']})"
                
                html_parts.append(f'<td style="{cell_style}" title="{title_text}">')
                html_parts.append(f'{value}')
                if has_comment:
                    html_parts.append('<span style="position: absolute; top: 2px; right: 2px; width: 8px; height: 8px; background-color: #ff6b35; border-radius: 50%; font-size: 8px;">💬</span>')
                html_parts.append('</td>')
            else:
                html_parts.append(f'<td style="{cell_style}">{value}</td>')
        html_parts.append('</tr>')
    
    html_parts.append('</tbody></table></div>')
    
    return ''.join(html_parts)

# 初始化会话状态
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
if 'store_name' not in st.session_state:
    st.session_state.store_name = ""
if 'user_id' not in st.session_state:
    st.session_state.user_id = ""
if 'is_admin' not in st.session_state:
    st.session_state.is_admin = False
if 'google_sheets_client' not in st.session_state:
    st.session_state.google_sheets_client = None

# 主标题
st.markdown('<h1 class="main-header">📊 门店报表查询系统</h1>', unsafe_allow_html=True)

# 初始化Google Sheets客户端
if not st.session_state.google_sheets_client:
    with st.spinner("连接云数据库..."):
        gc = get_google_sheets_client()
        if gc:
            st.session_state.google_sheets_client = gc
            st.success("✅ 连接成功！")
        else:
            st.error("❌ 连接失败，请检查配置")
            st.stop()

gc = st.session_state.google_sheets_client

# 侧边栏
with st.sidebar:
    st.title("⚙️ 系统功能")
    
    user_type = st.radio("选择用户类型", ["普通用户", "管理员"])
    
    if user_type == "管理员":
        st.subheader("🔐 管理员登录")
        admin_password = st.text_input("管理员密码", type="password")
        
        if st.button("验证管理员身份"):
            if admin_password == ADMIN_PASSWORD:
                st.session_state.is_admin = True
                st.success("✅ 验证成功！")
                st.rerun()
            else:
                st.error("❌ 密码错误！")
        
        if st.session_state.is_admin:
            st.subheader("📁 文件管理")
            
            # 上传权限表
            permissions_file = st.file_uploader("上传门店权限表", type=['xlsx', 'xls'])
            if permissions_file:
                try:
                    df = pd.read_excel(permissions_file)
                    if len(df.columns) >= 2:
                        if save_permissions_to_sheets(df, gc):
                            st.success(f"✅ 权限表已上传：{len(df)} 个用户")
                            st.balloons()
                        else:
                            st.error("❌ 保存失败")
                    else:
                        st.error("❌ 格式错误：需要至少两列")
                except Exception as e:
                    st.error(f"❌ 读取失败：{str(e)}")
            
            # 上传财务报表（支持备注）
            reports_file = st.file_uploader("上传财务报表（支持单元格备注）", type=['xlsx', 'xls'])
            if reports_file:
                try:
                    with st.spinner("正在读取Excel文件和备注信息..."):
                        # 重置文件指针到开始位置
                        reports_file.seek(0)
                        
                        # 使用新的函数读取Excel文件，包括备注
                        sheets_data = read_excel_with_comments(reports_file)
                        
                        # 调试信息
                        st.write("🔍 调试信息：")
                        if sheets_data:
                            for sheet_name, sheet_info in sheets_data.items():
                                comments_count = len(sheet_info.get('comments', {}))
                                st.write(f"- {sheet_name}: {comments_count} 个备注")
                                if comments_count > 0:
                                    st.write(f"  备注示例: {list(sheet_info['comments'].keys())[:3]}")
                        else:
                            st.write("- 未读取到任何数据")
                    
                    if sheets_data:
                        if save_reports_to_sheets(sheets_data, gc):
                            total_comments = sum(len(sheet_info.get('comments', {})) for sheet_info in sheets_data.values())
                            st.success(f"✅ 报表已上传：{len(sheets_data)} 个门店，{total_comments} 个备注")
                            if total_comments == 0:
                                st.info("💡 提示：如果您的Excel文件包含备注，请确保使用.xlsx格式并且备注不为空")
                            st.balloons()
                        else:
                            st.error("❌ 保存失败")
                    else:
                        st.error("❌ 无法读取Excel文件")
                except Exception as e:
                    st.error(f"❌ 读取失败：{str(e)}")
                    import traceback
                    st.error(f"详细错误：{traceback.format_exc()}")
    
    else:
        if st.session_state.logged_in:
            st.subheader("👤 当前登录")
            st.info(f"门店：{st.session_state.store_name}")
            st.info(f"编号：{st.session_state.user_id}")
            
            if st.button("🚪 退出登录"):
                st.session_state.logged_in = False
                st.session_state.store_name = ""
                st.session_state.user_id = ""
                st.rerun()

# 主界面
if user_type == "管理员" and st.session_state.is_admin:
    st.markdown('<div class="admin-panel"><h3>👨‍💼 管理员控制面板</h3><p>数据永久保存在云端（支持单元格备注）</p></div>', unsafe_allow_html=True)
    
    permissions_data = load_permissions_from_sheets(gc)
    reports_data = load_reports_from_sheets(gc)
    
    col1, col2, col3 = st.columns(3)
    with col1:
        perms_count = len(permissions_data) if permissions_data is not None else 0
        st.metric("权限表用户数", perms_count)
    with col2:
        reports_count = len(reports_data)
        st.metric("报表门店数", reports_count)
    with col3:
        total_comments = sum(len(sheet_info.get('comments', {})) for sheet_info in reports_data.values())
        st.metric("单元格备注数", total_comments)

elif user_type == "管理员" and not st.session_state.is_admin:
    st.info("👈 请在左侧边栏输入管理员密码")

else:
    if not st.session_state.logged_in:
        st.subheader("🔐 用户登录")
        
        permissions_data = load_permissions_from_sheets(gc)
        
        if permissions_data is None:
            st.warning("⚠️ 系统维护中，请联系管理员")
        else:
            stores = sorted(permissions_data[permissions_data.columns[0]].unique().tolist())
            
            with st.form("login_form"):
                selected_store = st.selectbox("选择门店", stores)
                user_id = st.text_input("人员编号")
                submit = st.form_submit_button("🚀 登录")
                
                if submit and selected_store and user_id:
                    if verify_user_permission(selected_store, user_id, permissions_data):
                        st.session_state.logged_in = True
                        st.session_state.store_name = selected_store
                        st.session_state.user_id = user_id
                        st.success("✅ 登录成功！")
                        st.balloons()
                        st.rerun()
                    else:
                        st.error("❌ 门店或编号错误！")
    
    else:
        # 已登录 - 显示报表
        st.markdown(f'<div class="store-info"><h3>🏪 {st.session_state.store_name}</h3><p>操作员：{st.session_state.user_id}</p></div>', unsafe_allow_html=True)
        
        reports_data = load_reports_from_sheets(gc)
        matching_sheets = find_matching_reports(st.session_state.store_name, reports_data)
        
        if matching_sheets:
            if len(matching_sheets) > 1:
                selected_sheet = st.selectbox("选择报表", matching_sheets)
            else:
                selected_sheet = matching_sheets[0]
            
            sheet_info = reports_data[selected_sheet]
            df = sheet_info['dataframe']
            comments_data = sheet_info.get('comments', {})
            
            # 检查并处理第一行是否为门店名称
            original_df = df.copy()
            if len(df) > 0:
                first_row = df.iloc[0]
                non_empty_count = sum(1 for val in first_row if pd.notna(val) and str(val).strip() != '')
                
                # 如果第一行只有少数非空值，可能是门店名称，跳过它
                if non_empty_count <= 2 and len(df) > 1:
                    df = df.iloc[1:].reset_index(drop=True)
            
            # 应收-未收额看板
            st.subheader("💰 应收-未收额")
            
            try:
                analysis_results = analyze_receivable_data(df, comments_data)
                
                if '应收-未收额' in analysis_results:
                    data = analysis_results['应收-未收额']
                    amount = data['amount']
                    
                    col1, col2, col3 = st.columns([1, 2, 1])
                    with col2:
                        if amount > 0:
                            st.markdown(f'''
                                <div class="receivable-positive">
                                    <h1 style="margin: 0; font-size: 3rem;">💳 ¥{amount:,.2f}</h1>
                                    <h3 style="margin: 0.5rem 0;">门店应付款</h3>
                                </div>
                            ''', unsafe_allow_html=True)
                        
                        elif amount < 0:
                            st.markdown(f'''
                                <div class="receivable-negative">
                                    <h1 style="margin: 0; font-size: 3rem;">💚 ¥{abs(amount):,.2f}</h1>
                                    <h3 style="margin: 0.5rem 0;">总部应退款</h3>
                                </div>
                            ''', unsafe_allow_html=True)
                        
                        else:
                            st.markdown('''
                                <div style="background: #e8f5e8; color: #2e7d32; padding: 2rem; border-radius: 15px; text-align: center;">
                                    <h1 style="margin: 0; font-size: 3rem;">⚖️ ¥0.00</h1>
                                    <h3 style="margin: 0.5rem 0;">收支平衡</h3>
                                    <p style="margin: 0;">应收未收额为零，账目平衡</p>
                                </div>
                            ''', unsafe_allow_html=True)
                    
                    # 显示备注信息
                    if 'comments' in data:
                        st.subheader("📝 相关备注")
                        for comment in data['comments']:
                            with st.expander(f"💬 {comment['column']} - 备注", expanded=False):
                                st.markdown(f"""
                                <div class="comment-tooltip">
                                    <strong>列名：</strong> {comment['column']}<br>
                                    <strong>单元格值：</strong> {comment['cell_value']}<br>
                                    <strong>备注内容：</strong> {comment['text']}<br>
                                    <strong>作者：</strong> {comment['author']}
                                </div>
                                """, unsafe_allow_html=True)
                
                else:
                    st.warning("⚠️ 未找到应收-未收额数据")
                    
                    # 显示调试信息
                    with st.expander("🔍 查看详情", expanded=True):
                        debug_info = analysis_results.get('debug_info', {})
                        
                        st.markdown("### 📋 数据查找说明")
                        st.write(f"- **报表总行数：** {debug_info.get('total_rows', 0)} 行")
                        st.write(f"- **备注总数：** {debug_info.get('comments_count', 0)} 个")
                        
                        if debug_info.get('checked_row_69'):
                            st.write(f"- **第69行内容：** {debug_info.get('row_69_content', 'N/A')}")
                        else:
                            st.write("- **第69行：** 报表行数不足69行")
                        
                        st.markdown("""
                        ### 💡 可能的原因
                        1. 第69行不包含"应收-未收额"相关关键词
                        2. 第69行的数值为空或格式不正确
                        3. 报表格式与预期不符
                        
                        ### 🛠️ 建议
                        - 请检查Excel报表第69行是否包含"应收-未收额"
                        - 确认该行有对应的金额数据
                        - 如需调整查找位置，请联系技术支持
                        """)
            
            except Exception as e:
                st.error(f"❌ 分析数据时出错：{str(e)}")
            
            st.divider()
            
            # 完整报表数据
            st.subheader("📋 完整报表数据")
            
            # 显示备注统计
            if comments_data:
                st.info(f"💬 此报表包含 {len(comments_data)} 个单元格备注，鼠标悬停在黄色背景单元格上可查看备注内容")
            
            search_term = st.text_input("🔍 搜索报表内容")
            
            # 数据过滤
            try:
                if search_term:
                    # 安全的搜索实现
                    search_df = df.copy()
                    # 确保所有数据都是字符串
                    for col in search_df.columns:
                        search_df[col] = search_df[col].astype(str).fillna('')
                    
                    mask = search_df.apply(
                        lambda x: x.str.contains(search_term, case=False, na=False, regex=False)
                    ).any(axis=1)
                    filtered_df = df[mask]
                    st.info(f"找到 {len(filtered_df)} 条包含 '{search_term}' 的记录")
                else:
                    filtered_df = df
                
                # 数据统计
                st.info(f"📊 数据统计：共 {len(filtered_df)} 条记录，{len(df.columns)} 列，{len(comments_data)} 个备注")
                
                # 显示数据表格（带备注）
                if len(filtered_df) > 0:
                    # 清理数据以确保显示正常
                    display_df = filtered_df.copy()
                    
                    # 确保列名唯一
                    unique_columns = []
                    for i, col in enumerate(display_df.columns):
                        col_name = str(col)
                        if col_name in unique_columns:
                            col_name = f"{col_name}_{i}"
                        unique_columns.append(col_name)
                    display_df.columns = unique_columns
                    
                    # 清理数据内容
                    for col in display_df.columns:
                        display_df[col] = display_df[col].astype(str).fillna('')
                    
                    # 创建带备注的HTML表格
                    if comments_data:
                        html_table = create_dataframe_with_comments(display_df, comments_data)
                        st.markdown(html_table, unsafe_allow_html=True)
                        
                        # 显示备注列表
                        if st.expander("📝 查看所有备注", expanded=False):
                            for comment_key, comment_info in comments_data.items():
                                row_idx, col_idx = map(int, comment_key.split('_'))
                                if row_idx < len(display_df):
                                    col_name = display_df.columns[col_idx] if col_idx < len(display_df.columns) else f'列{col_idx+1}'
                                    st.markdown(f"""
                                    <div class="comment-tooltip">
                                        <strong>位置：</strong> 第{row_idx+1}行，{col_name}<br>
                                        <strong>单元格值：</strong> {comment_info['cell_value']}<br>
                                        <strong>备注内容：</strong> {comment_info['text']}<br>
                                        <strong>作者：</strong> {comment_info.get('author', 'Unknown')}
                                    </div>
                                    """, unsafe_allow_html=True)
                    else:
                        # 普通表格显示
                        st.dataframe(display_df, use_container_width=True, height=400)
                
                else:
                    st.warning("没有找到符合条件的数据")
                    
            except Exception as e:
                st.error(f"❌ 数据处理时出错：{str(e)}")
            
            # 下载功能
            st.subheader("📥 数据下载")
            
            col1, col2 = st.columns(2)
            with col1:
                try:
                    buffer = io.BytesIO()
                    # 准备下载数据
                    download_df = df.copy()
                    # 确保列名唯一
                    unique_cols = []
                    for i, col in enumerate(download_df.columns):
                        col_name = str(col)
                        if col_name in unique_cols:
                            col_name = f"{col_name}_{i}"
                        unique_cols.append(col_name)
                    download_df.columns = unique_cols
                    
                    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                        download_df.to_excel(writer, index=False)
                        
                        # 如果有备注，添加备注信息到另一个工作表
                        if comments_data:
                            comments_df = pd.DataFrame([
                                {
                                    '行号': int(k.split('_')[0]) + 1,
                                    '列号': int(k.split('_')[1]) + 1,
                                    '列名': download_df.columns[int(k.split('_')[1])] if int(k.split('_')[1]) < len(download_df.columns) else f'列{int(k.split("_")[1])+1}',
                                    '单元格值': v['cell_value'],
                                    '备注内容': v['text'],
                                    '作者': v.get('author', 'Unknown')
                                } for k, v in comments_data.items()
                            ])
                            comments_df.to_excel(writer, sheet_name='备注信息', index=False)
                    
                    st.download_button(
                        "📥 下载完整报表 (Excel)",
                        buffer.getvalue(),
                        f"{st.session_state.store_name}_报表_{datetime.now().strftime('%Y%m%d')}.xlsx",
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                except Exception as e:
                    st.error(f"Excel下载准备失败：{str(e)}")
            
            with col2:
                try:
                    # CSV下载
                    csv_df = df.copy()
                    # 处理列名
                    unique_cols = []
                    for i, col in enumerate(csv_df.columns):
                        col_name = str(col)
                        if col_name in unique_cols:
                            col_name = f"{col_name}_{i}"
                        unique_cols.append(col_name)
                    csv_df.columns = unique_cols
                    
                    csv = csv_df.to_csv(index=False, encoding='utf-8-sig')
                    st.download_button(
                        "📥 下载CSV格式",
                        csv,
                        f"{st.session_state.store_name}_报表_{datetime.now().strftime('%Y%m%d')}.csv",
                        "text/csv"
                    )
                except Exception as e:
                    st.error(f"CSV下载准备失败：{str(e)}")
        
        else:
            st.error(f"❌ 未找到门店 '{st.session_state.store_name}' 的报表")
