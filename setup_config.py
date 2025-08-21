#!/usr/bin/env python3
"""
快速配置脚本 - 帮助用户设置MongoDB连接
"""

import os
import secrets
import string
from pathlib import Path

def generate_secret_key(length=50):
    """生成安全的密钥"""
    alphabet = string.ascii_letters + string.digits + "!@#$%^&*"
    return ''.join(secrets.choice(alphabet) for _ in range(length))

def setup_streamlit_secrets():
    """设置Streamlit secrets配置"""
    print("🔧 门店报表系统 - 配置向导")
    print("=" * 50)
    
    # 确保.streamlit目录存在
    streamlit_dir = Path(".streamlit")
    streamlit_dir.mkdir(exist_ok=True)
    
    secrets_file = streamlit_dir / "secrets.toml"
    
    print("\n📋 请提供以下配置信息：")
    
    # MongoDB配置
    print("\n1. MongoDB配置")
    print("如果你使用MongoDB Atlas，连接URI格式如下：")
    print("mongodb+srv://username:password@cluster.mongodb.net/")
    
    mongo_uri = input("MongoDB URI: ").strip()
    if not mongo_uri:
        mongo_uri = "mongodb://localhost:27017/"
        print(f"使用默认本地连接: {mongo_uri}")
    
    db_name = input("数据库名称 [store_reports]: ").strip()
    if not db_name:
        db_name = "store_reports"
    
    # 安全配置
    print("\n2. 安全配置")
    secret_key = generate_secret_key()
    print(f"自动生成密钥: {secret_key[:20]}...")
    
    admin_password = input("管理员密码 [admin123456]: ").strip()
    if not admin_password:
        admin_password = "admin123456"
    
    # 生成配置文件
    config_content = f"""# Streamlit Secrets配置文件
# 由配置向导自动生成

[mongodb]
uri = "{mongo_uri}"
database_name = "{db_name}"

[app]
secret_key = "{secret_key}"
debug = false

[security]
admin_password = "{admin_password}"
session_timeout = 14400  # 4小时

# 注意：此文件包含敏感信息，请妥善保管
# 不要将此文件提交到版本控制系统
"""
    
    # 写入文件
    with open(secrets_file, 'w', encoding='utf-8') as f:
        f.write(config_content)
    
    print(f"\n✅ 配置文件已创建: {secrets_file}")
    print("🔒 文件权限已设置为仅当前用户可读")
    
    # 设置文件权限（仅当前用户可读）
    os.chmod(secrets_file, 0o600)
    
    print("\n🚀 配置完成！现在可以启动应用：")
    print("python start_app.py")

def test_connection():
    """测试数据库连接"""
    try:
        from config_manager import ConfigManager
        from pymongo import MongoClient
        
        print("\n🔍 测试数据库连接...")
        config = ConfigManager.get_mongodb_config()
        
        client = MongoClient(config['uri'], serverSelectionTimeoutMS=5000)
        db = client[config['database_name']]
        
        # 测试连接
        db.command('ping')
        print("✅ 数据库连接成功！")
        
        # 显示数据库信息
        server_info = client.server_info()
        print(f"MongoDB版本: {server_info['version']}")
        
        client.close()
        return True
        
    except Exception as e:
        print(f"❌ 数据库连接失败: {e}")
        print("\n💡 请检查：")
        print("1. MongoDB URI是否正确")
        print("2. 网络连接是否正常")
        print("3. 数据库用户权限是否足够")
        return False

def main():
    """主函数"""
    current_dir = Path(__file__).parent
    os.chdir(current_dir)
    
    print("选择操作：")
    print("1. 配置MongoDB连接")
    print("2. 测试数据库连接")
    print("3. 查看当前配置")
    print("4. 退出")
    
    while True:
        choice = input("\n请选择 (1-4): ").strip()
        
        if choice == "1":
            setup_streamlit_secrets()
        elif choice == "2":
            if not Path(".streamlit/secrets.toml").exists():
                print("❌ 配置文件不存在，请先运行配置")
            else:
                test_connection()
        elif choice == "3":
            secrets_file = Path(".streamlit/secrets.toml")
            if secrets_file.exists():
                print(f"\n📄 配置文件位置: {secrets_file}")
                print("📝 配置内容:")
                with open(secrets_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                    # 隐藏敏感信息
                    lines = content.split('\n')
                    for line in lines:
                        if 'password' in line.lower() or 'secret' in line.lower():
                            if '=' in line:
                                key = line.split('=')[0]
                                print(f"{key}= [隐藏]")
                            else:
                                print(line)
                        else:
                            print(line)
            else:
                print("❌ 配置文件不存在")
        elif choice == "4":
            print("👋 再见！")
            break
        else:
            print("❌ 无效选择")

if __name__ == "__main__":
    main()