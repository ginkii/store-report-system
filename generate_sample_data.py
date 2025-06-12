import pandas as pd
import random
from datetime import datetime
import numpy as np

def generate_sample_data():
    """生成示例权限表和财务报表数据（匹配实际报表格式）"""
    
    # 1. 生成权限表
    stores = ['北京店', '上海店', '广州店', '深圳店', '杭州店']
    permissions_data = []
    
    for store in stores:
        # 每个门店生成3-5个员工
        num_employees = random.randint(3, 5)
        for i in range(num_employees):
            permissions_data.append({
                '门店名称': store,
                '人员编号': f"{stores.index(store)+1:02d}{i+1:01d}"
            })
    
    permissions_df = pd.DataFrame(permissions_data)
    
    # 2. 生成财务报表（模拟实际格式）
    months = ['1月', '2月', '3月', '4月', '5月', '6月']
    business_units = ['类团', '锌了么']  # 两个业务板块
    
    # 创建Excel writer对象
    with pd.ExcelWriter('示例财务报表.xlsx', engine='openpyxl') as writer:
        
        for store in stores:
            # 财务项目列表（与实际报表一致）
            financial_items = [
                '月份',  # 标题行
                '平台',  # 副标题
                '订单量',
                '运营费用',
                '平台推广费用',
                '一. 毛利-线上',
                '线下销售收入',
                '线下销售成本',
                '线下销售成本',
                '仓内商品损耗',
                '其他费',
                '推广费用',
                '二. 毛利-线及下',
                '1. 人工费用',
                '工资（含绩效）',
                '福利费',
                '差旅费',
                '2. 房租招待',
                '房租费',
                '3. 房租物业水电',
                '租赁费',
                '物业费',
                '水电费',
                '装修费',
                '4. 办公电话快递',
                '办公费',
                '通讯费',
                '快递费',
                '5. 其他管理费用',
                '折旧摊销',
                '其他',
                '6. 财务费用',
                '利息收入',
                '利息支出',
                '其他支出',
                '营业外收入',
                '营业外支出',
                '所得税费用',
                '三. 毛利-线上',  # 重要指标
                '五. 净利润'      # 重要指标
            ]
            
            # 添加更多项目使表格更完整
            additional_items = [
                '总营业额',
                '应收-收款完成数',
                '收到-分润款',
                '应收-未收额'  # 重要指标
            ]
            financial_items.extend(additional_items)
            
            # 创建数据框架
            data = {'月份': financial_items}
            
            # 为每个月份生成两个业务板块的数据
            base_revenue = random.randint(80000, 200000)
            
            for month in months:
                for unit in business_units:
                    col_name = f"{month}\n{unit}"
                    values = []
                    
                    for item in financial_items:
                        if item in ['月份', '平台']:
                            values.append(unit if item == '平台' else '')
                        elif item == '订单量':
                            values.append(random.randint(1000, 2000))
                        elif item == '三. 毛利-线上':
                            # 毛利数据
                            revenue = base_revenue * random.uniform(0.4, 0.6) * (0.6 if unit == '类团' else 0.4)
                            values.append(round(revenue, 2))
                        elif item == '五. 净利润':
                            # 净利润数据
                            profit = base_revenue * random.uniform(0.2, 0.3) * (0.6 if unit == '类团' else 0.4)
                            values.append(round(profit, 2))
                        elif item == '应收-未收额':
                            # 应收未收额
                            if month == months[-1]:  # 只在最后一个月有未收额
                                values.append(round(base_revenue * 0.1, 2))
                            else:
                                values.append(0)
                        elif '费' in item or '成本' in item:
                            # 费用项目
                            cost = base_revenue * random.uniform(0.05, 0.15)
                            values.append(round(cost, 2))
                        else:
                            # 其他项目随机生成或为0
                            if random.random() > 0.7:
                                values.append(round(random.uniform(1000, 5000), 2))
                            else:
                                values.append(0)
                    
                    data[col_name] = values
            
            # 添加合计列
            data['合计'] = []
            for i, item in enumerate(financial_items):
                if item in ['月份', '平台']:
                    data['合计'].append('')
                else:
                    # 计算所有月份的合计
                    total = 0
                    for col in data:
                        if col not in ['月份', '合计']:
                            try:
                                total += float(data[col][i])
                            except:
                                pass
                    data['合计'].append(round(total, 2) if total > 0 else '')
            
            # 创建DataFrame
            df = pd.DataFrame(data)
            
            # 写入Excel
            sheet_name = f"{store}"
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            # 调整格式
            worksheet = writer.sheets[sheet_name]
            # 设置列宽
            for idx, col in enumerate(df.columns):
                worksheet.column_dimensions[chr(65 + idx)].width = 15
    
    # 保存权限表
    permissions_df.to_excel('示例门店权限表.xlsx', index=False)
    
    print("✅ 已生成示例文件：")
    print("   - 示例门店权限表.xlsx")
    print("   - 示例财务报表.xlsx")
    print("\n文件说明：")
    print(f"- 权限表包含 {len(stores)} 个门店，共 {len(permissions_df)} 个员工")
    print(f"- 财务报表包含 {len(stores)} 个门店的 {len(months)} 个月数据")
    print(f"- 每个月份包含 {len(business_units)} 个业务板块（类团、锌了么）")
    print("\n重要财务指标：")
    print("- 三. 毛利-线上")
    print("- 五. 净利润")
    print("- 应收-未收额（在合计列中）")
    print("\n您可以使用这些文件测试系统功能！")

if __name__ == "__main__":
    generate_sample_data()
