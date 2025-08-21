import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# MongoDB配置
MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb+srv://your_username:your_password@cluster.mongodb.net/')
DATABASE_NAME = os.getenv('DATABASE_NAME', 'store_reports')

# 应用配置
APP_TITLE = "门店报表查询系统"
APP_ICON = "🏪"

# 安全配置
SECRET_KEY = os.getenv('SECRET_KEY', 'your_secret_key_here')

# 报表配置
DEFAULT_MONTHS_TO_SHOW = 3
REPORTS_PER_PAGE = 10