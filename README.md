# 门店财务核算系统 - 完整版

一个基于Streamlit和MongoDB的门店财务管理系统，支持财务填报、报表查询、批量上传和权限管理。

## 📋 系统要求

### Python 环境
- Python 3.8+
- pip (Python包管理器)

### 数据库
- MongoDB 4.4+
- 可选：MongoDB Atlas (云数据库)

## 🚀 快速部署

### 1. 安装依赖
```bash
pip install -r requirements.txt
```

### 2. 配置数据库

#### 本地MongoDB
```bash
# 安装MongoDB (Ubuntu/Debian)
sudo apt install mongodb

# 安装MongoDB (macOS)
brew install mongodb-community

# 启动MongoDB服务
sudo systemctl start mongodb
# 或者 macOS
brew services start mongodb-community
```

#### 云端MongoDB Atlas
1. 访问 https://www.mongodb.com/cloud/atlas
2. 创建免费集群
3. 获取连接字符串
4. 更新 `.streamlit/secrets.toml` 中的URI

### 3. 配置系统

编辑 `.streamlit/secrets.toml` 文件：
```toml
[mongodb]
uri = "你的MongoDB连接字符串"
database_name = "store_reports"

[security]
admin_password = "你的管理员密码"
```

### 4. 启动应用
```bash
streamlit run streamlit_app.py
```

访问：http://localhost:8501

## 📁 文件结构

```
项目根目录/
├── streamlit_app.py           # 主应用文件
├── requirements.txt           # 依赖包列表
├── .streamlit/
│   ├── secrets.toml          # 敏感配置 (不要提交到Git)
│   └── config.toml           # 应用配置
└── README.md                 # 本文件
```

## 🔧 功能模块

### 💼 财务填报系统
- ✅ **用户财务数据填报**：工资、房租、水电费等线下费用录入
- ✅ **管理员预设数据显示**：回款、线上支出、线上净利润等只读显示
- ✅ **实时财务计算**：自动计算线上余额、线下费用合计、最终余额、最终净利润
- ✅ **可视化运算看板**：
  - 🟢 现金表运算 (浅绿背景)：(1)回款 - (11)线上支出 = (15)线上余额 - (17)线下费用合计 = **(26)最终余额**
  - 🔵 利润表运算 (浅蓝背景)：(16)线上净利润 - (17)线下费用合计 = **(27)最终净利润**
- ✅ **勾稽关系提醒**：表一(9) ≡ 表二(11) | 表一(14) ≡ 表二(12)
- ✅ **实时计算更新**：用户输入18-26项明细时，第17项和第27项实时自动更新
- ✅ **关键指标加粗显示**：代号26、1、16、27对应的单元格加粗显示
- ✅ **保存与提交**：支持草稿保存和正式提交两种模式
- ✅ **Excel导出**：单店财务报表一键下载，包含完整格式和计算公式

### 👨‍💼 财务管理系统（管理员）
- ✅ **管理员身份验证**：密码登录保护
- ✅ **提交情况概览**：总门店数、已提交、待提交、完成率统计
- ✅ **详细报表列表**：所有门店财务数据汇总展示
- ✅ **批量导出功能**：所有报表/仅已提交/仅草稿的ZIP打包下载
- ✅ **数据分析图表**：各门店余额对比、净利润对比的可视化分析
- ✅ **汇总统计**：总余额、总净利润、平均值等关键指标
- ✅ **报表创建工具**：为门店批量创建期间报表

### 🔍 门店查询系统
- ✅ **查询代码验证**：基于权限的门店数据访问
- ✅ **门店信息展示**：门店名称、代码、区域等基本信息
- ✅ **历史报表查询**：按期间查看历史财务数据
- ✅ **财务运算看板**：与填报系统相同的可视化展示
- ✅ **Excel下载**：单期间报表数据导出
- ✅ **数据统计展示**：总收入、总支出、净利润等关键指标

### 📤 批量上传系统（管理员）
- ✅ **多文件上传**：同时上传多个Excel文件
- ✅ **自动门店识别**：根据文件名智能匹配门店
- ✅ **手动门店选择**：无法自动识别时的手动指定功能
- ✅ **数据预览**：上传前查看文件内容和基本信息
- ✅ **批量处理**：一键批量上传所有文件，带进度显示
- ✅ **期间统一设置**：批量操作时统一指定报告期间

### 👥 权限管理系统（管理员）
- ✅ **查询权限管理**：添加、查看、导出查询代码
- ✅ **门店管理**：添加新门店、查看门店列表、导出门店信息
- ✅ **数据统计分析**：
  - 门店区域分布饼图
  - 报表提交趋势折线图
  - 基础数据统计（总门店数、总权限数、总报表数）

## 📊 财务运算逻辑

### 🟢 现金表运算 (代号映射)
```
(1) 回款 - (11) 线上支出 = (15) 线上余额
(15) 线上余额 - (17) 线下费用合计 = (26) 最终余额
```

### 🔵 利润表运算 (代号映射)  
```
(17) 线下费用合计 = SUM(18至26项明细)
(16) 线上净利润 - (17) 线下费用合计 = (27) 最终净利润
```

### ⚠️ 勾稽关系检查
- 表一(9) ≡ 表二(11) 
- 表一(14) ≡ 表二(12)

## 📊 数据库结构

系统会自动创建以下集合：
- `stores`: 门店信息
- `permissions`: 查询权限  
- `reports`: 历史报表数据
- `store_financial_reports`: 财务填报数据

### store_financial_reports 集合结构
```json
{
  "header": {
    "store_id": "store_xxx",
    "store_name": "门店名称", 
    "period": "2024-12",
    "status": "pending|submitted",
    "created_at": "2024-12-01T00:00:00",
    "updated_at": "2024-12-01T00:00:00"
  },
  "admin_data": {
    "1": 50000,   // 回款
    "2": 0,       // 其他现金收入
    "11": 30000,  // 线上支出
    "16": 20000   // 线上净利润
  },
  "user_inputs": {
    "18": 8000,   // 工资
    "19": 5000,   // 房租
    "20": 1000,   // 水电费
    "21": 500,    // 物业费
    "22": 0,      // 其他费用1-5
    "23": 0,
    "24": 0, 
    "25": 0,
    "26": 0
  },
  "calculated_metrics": {
    "15": 20000,  // 线上余额 = 1 - 11
    "17": 14500,  // 线下费用合计 = SUM(18-26)
    "26": 5500,   // 最终余额 = 15 - 17
    "27": 5500    // 最终净利润 = 16 - 17
  },
  "metadata": {
    "created_at": "2024-12-01T00:00:00",
    "updated_at": "2024-12-01T00:00:00",
    "submitted_by": null,
    "submission_time": null
  }
}
```

## ⚙️ 环境变量 (可选)

如果不使用secrets.toml，可以设置环境变量：

```bash
export MONGODB_URI="mongodb://localhost:27017/"
export DATABASE_NAME="store_reports"
export ADMIN_PASSWORD="admin123"
```

## 🛡️ 安全注意事项

1. **修改默认密码**：首次部署后立即修改管理员密码
2. **数据库安全**：为MongoDB设置认证和防火墙
3. **HTTPS部署**：生产环境建议使用HTTPS
4. **备份策略**：定期备份MongoDB数据

## 🚨 故障排除

### 连接问题
```python
# 检查MongoDB连接
from pymongo import MongoClient
client = MongoClient("你的连接字符串")
client.admin.command('ping')
```

### 依赖问题
```bash
# 升级pip
pip install --upgrade pip

# 清理缓存重新安装
pip cache purge
pip install -r requirements.txt --force-reinstall
```

### 端口冲突
```bash
# 指定端口启动
streamlit run streamlit_app.py --server.port 8502
```

## 🔄 更新应用

1. 停止当前应用 (Ctrl+C)
2. 更新代码文件
3. 重新启动应用

```bash
git pull origin main  # 如果使用Git
streamlit run streamlit_app.py
```

## 📞 支持

如有问题，请检查：
1. MongoDB是否正常运行
2. 所有依赖是否正确安装
3. 配置文件是否正确设置
4. 防火墙和网络设置

## 🎯 性能优化

### 数据库优化
- 使用索引加速查询
- 定期清理旧数据
- 监控数据库性能

### 应用优化
- 使用@st.cache_data缓存数据
- 避免频繁的数据库查询
- 优化Excel处理性能

---
© 2024 门店财务核算系统 - 完整版
