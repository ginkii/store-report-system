# 门店报表查询系统

一个基于Streamlit的门店报表查询系统，支持汇总Excel文件上传到腾讯云COS，用户可通过门店选择和编码查询获取对应的报表数据。

## 🚀 快速开始

如果您想快速启动系统，请参考：[快速开始指南](QUICKSTART.md)

## 功能特点

- **管理员功能**：
  - 密码登录验证
  - 汇总Excel文件上传到腾讯云COS
  - 自动解析Excel文件中的门店sheet
  - 报表管理和统计
  - 系统监控和设置

- **用户功能**：
  - 两步查询：先选择门店，再输入查询编码
  - 支持精确和模糊匹配
  - 数据预览和导出
  - 查询历史记录

- **系统特性**：
  - 基于JSON的数据存储（支持并发访问）
  - 腾讯云COS文件存储
  - 多用户并发支持
  - 响应式Web界面

## 项目结构

```
├── .streamlit/
│   ├── secrets.toml        # Streamlit配置文件（需要创建）
│   └── config.toml         # Streamlit应用配置
├── app.py                  # 主应用文件
├── config.py               # 配置管理（支持Streamlit secrets）
├── json_handler.py         # JSON数据操作
├── cos_handler.py          # 腾讯云COS操作
├── excel_parser.py         # Excel文件解析
├── query_handler.py        # 查询处理逻辑
├── setup_config.py         # 配置设置脚本
├── requirements.txt        # 项目依赖
├── secrets.toml.example    # 配置模板
├── .gitignore             # Git忽略文件
├── data.json              # 数据文件（自动生成）
├── README.md              # 项目说明
├── DEPLOY.md              # 部署指南
└── QUICKSTART.md          # 快速开始指南
```

## 环境要求

- Python 3.8+
- Streamlit
- 腾讯云COS账号和配置

## 安装部署

### 1. 克隆项目

```bash
git clone <project-repo>
cd store-report-query
```

### 2. 安装依赖

```bash
pip install -r requirements.txt
```

### 3. 配置系统

**推荐使用Streamlit Secrets管理配置（更安全）**

#### 方法1：使用Streamlit Secrets（推荐）

1. 创建 `.streamlit/secrets.toml` 文件：
```toml
# 管理员配置
ADMIN_PASSWORD = "your_admin_password"

# 腾讯云COS配置
[tencent_cos]
secret_id = "AKIDARaYN4YpuqcDdqrfJkFnCQSYbVDi06zf"
secret_key = "XszvmRt9C3iWHC6ymU2OXVIsGRPBk8LN"
region = "ap-shanghai"
bucket_name = "store-reports-data-1369683907"
domain = ""

# 应用配置
[app]
max_file_size = 52428800  # 50MB
session_timeout = 3600    # 1小时
```

### 快速配置

可以使用提供的配置脚本快速设置：

```bash
# 运行配置设置脚本
python setup_config.py

# 或者手动设置
mkdir -p .streamlit
cp secrets.toml.example .streamlit/secrets.toml
# 然后编辑 .streamlit/secrets.toml 文件
```

2. 将 `secrets.toml.example` 复制为 `.streamlit/secrets.toml` 并填入您的配置：
```bash
mkdir -p .streamlit
cp secrets.toml.example .streamlit/secrets.toml
# 然后编辑 .streamlit/secrets.toml 文件
```

#### 方法2：使用环境变量（备选）

```bash
# 管理员密码
export ADMIN_PASSWORD="your_admin_password"

# 腾讯云COS配置
export COS_REGION="ap-shanghai"
export COS_SECRET_ID="AKIDARaYN4YpuqcDdqrfJkFnCQSYbVDi06zf"
export COS_SECRET_KEY="XszvmRt9C3iWHC6ymU2OXVIsGRPBk8LN"
export COS_BUCKET="store-reports-data-1369683907"
```

#### Streamlit Cloud部署

在Streamlit Cloud部署时，可以直接在部署面板的"Secrets"选项中配置，无需创建本地文件。

或者查看 `secrets.toml.example` 文件中的完整配置模板。

**⚠️ 安全提醒**：
- `.streamlit/secrets.toml` 文件已在 `.gitignore` 中，不会被提交到版本控制
- 请妥善保管您的COS密钥信息
- 建议定期更换密钥

### 4. 运行应用

```bash
streamlit run app.py
```

系统将在默认端口8501启动，访问 `http://localhost:8501`

## 使用说明

### 管理员操作

1. **登录**：在"管理员登录"标签页输入密码
2. **上传报表**：
   - 选择Excel汇总文件
   - 系统自动解析所有sheet（门店）
   - 填写报表描述并确认上传
3. **管理报表**：查看当前报表、门店列表和历史记录
4. **系统监控**：查看查询统计和系统状态

### 用户查询

1. **选择门店**：从下拉列表选择要查询的门店
2. **输入编码**：输入查询编码（支持数字、字母混合）
3. **查询设置**：选择精确匹配或模糊匹配
4. **查看结果**：查看匹配结果和详细数据
5. **导出数据**：将查询结果导出为Excel文件

## Excel文件要求

- 支持格式：`.xlsx`, `.xls`
- 文件结构：按sheet分门店，每个sheet名称即为门店名称
- 数据格式：每个sheet包含该门店的完整数据
- 文件大小：建议不超过50MB

## 性能优化

- 使用JSON文件存储，支持并发读写
- 文件直接从COS流式下载，无本地缓存
- 分页显示大量数据
- 异步处理文件上传和解析

## 故障排除

### 1. COS连接失败
- 检查 `.streamlit/secrets.toml` 文件是否存在且配置正确
- 验证网络连接到腾讯云上海地区
- 确认COS存储桶 `store-reports-data-1369683907` 的权限设置
- 检查SECRET_ID和SECRET_KEY是否有效
- 在管理员面板→系统设置中测试COS连接

### 2. 配置文件问题
- 确认 `.streamlit/secrets.toml` 文件格式正确
- 检查TOML语法是否有误
- 验证所有必需的配置项是否存在
- 尝试重新从 `secrets.toml.example` 复制配置

### 2. 文件上传失败
- 检查文件格式和大小
- 验证COS存储空间
- 确认文件权限

### 3. 查询无结果
- 确认门店是否存在
- 检查编码格式
- 尝试模糊匹配

### 4. 系统错误
- 查看控制台错误日志
- 检查data.json文件权限
- 重启应用服务

## 技术支持

如需技术支持，请联系系统管理员或查看以下资源：

- [Streamlit官方文档](https://docs.streamlit.io/)
- [腾讯云COS文档](https://cloud.tencent.com/document/product/436)
- [Pandas文档](https://pandas.pydata.org/docs/)

## 更新日志

### v1.1.0
- 🔐 重构配置管理，使用Streamlit Secrets
- 🛡️ 增强配置安全性，敏感信息不再出现在代码中
- 📁 添加配置模板和.gitignore文件
- 🔄 保持向后兼容性，支持环境变量降级
- 📖 更新文档和部署指南

### v1.0.1
- 更新腾讯云COS配置为上海地区
- 配置信息内置到代码中，简化部署流程
- 更新文档和示例配置

### v1.0.0
- 基础功能实现
- 管理员报表上传
- 用户两步查询
- 数据导出功能

## 许可证

本项目采用MIT许可证。
