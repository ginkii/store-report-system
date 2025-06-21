def save_reports_to_sheets(reports_dict, gc):
    """保存报表数据 - 高速版"""
    try:
        spreadsheet = get_or_create_spreadsheet(gc)
        
        success_count = 0
        total_stores = len(reports_dict)
        failed_stores = []
        saved_stores = []  # 记录成功保存的门店
        
        progress_text = st.empty()
        progress_bar = st.progress(0)
        
        # 批量处理所有门店
        for idx, (store_name, df) in enumerate(reports_dict.items()):
            try:
                progress_text.text(f"正在保存门店 {idx+1}/{total_stores}: {store_name}")
                progress_bar.progress((idx + 1) / total_stores)
                
                # 创建安全的工作表名称
                safe_sheet_name = store_name.replace('/', '_').replace('\\', '_')[:31]
                
                # 获取或创建工作表
                worksheet = get_or_create_worksheet(spreadsheet, safe_sheet_name)
                
                # 清理数据
                df_cleaned = df.copy()
                for col in df_cleaned.columns:
                    df_cleaned[col] = df_cleaned[col].astype(str).replace('nan', '').replace('None', '')
                
                # 转换为列表格式
                data_list = [df_cleaned.columns.tolist()] + df_cleaned.values.tolist()
                
                # 清空并批量写入
                worksheet.clear()
                
                # 使用batch_update优化
                if safe_batch_update(worksheet, data_list, 1, BATCH_SIZE, show_progress=False):
                    success_count += 1
                    saved_stores.append(safe_sheet_name)  # 记录成功保存的工作表名
                else:
                    failed_stores.append(store_name)
                
            except Exception as e:
                logger.error(f"Failed to save report for {store_name}: {str(e)}")
                failed_stores.append(store_name)
                
                # 速率限制时短暂等待
                if "429" in str(e):
                    time.sleep(5)
        
        progress_text.empty()
        progress_bar.empty()
        
        # 更新系统信息
        try:
            info_worksheet = get_or_create_worksheet(spreadsheet, SYSTEM_INFO_SHEET_NAME)
            info_data = [
                ['Last Update', datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
                ['Total Stores', str(success_count)],  # 使用实际成功数量
                ['Success Count', str(success_count)],
                ['Failed Count', str(len(failed_stores))],
                ['Status', 'Active' if success_count > 0 else 'Error'],
                ['Store List', ', '.join(saved_stores[:10]) + ('...' if len(saved_stores) > 10 else '')]  # 保存部分门店列表
            ]
            
            info_worksheet.clear()
            info_worksheet.update('A1', info_data, value_input_option='RAW')
        except:
            pass  # 系统信息更新失败不影响主功能
        
        # 显示结果
        if failed_stores:
            st.warning(f"以下门店保存失败: {', '.join(failed_stores)}")
        
        return success_count > 0
        
    except Exception as e:
        logger.error(f"Failed to save reports: {str(e)}")
        st.error(f"保存报表失败: {str(e)}")
        return False
