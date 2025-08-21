#!/usr/bin/env python3
"""
门店报表查询系统启动脚本
"""

import subprocess
import sys
import os
from pathlib import Path

def install_requirements():
    """安装依赖包"""
    try:
        print("正在安装依赖包...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        print("依赖包安装完成！")
        return True
    except subprocess.CalledProcessError as e:
        print(f"安装依赖包失败: {e}")
        return False

def start_query_app():
    """启动门店查询应用"""
    try:
        print("启动门店报表查询系统...")
        cmd = [
            sys.executable, "-m", "streamlit", "run", 
            "enhanced_app.py",
            "--server.port=8501",
            "--server.address=0.0.0.0",
            "--theme.primaryColor=#FF6B6B",
            "--theme.backgroundColor=#FFFFFF",
            "--theme.secondaryBackgroundColor=#F0F2F6"
        ]
        subprocess.run(cmd)
    except KeyboardInterrupt:
        print("\n应用已停止")
    except Exception as e:
        print(f"启动应用失败: {e}")

def start_upload_app():
    """启动批量上传应用"""
    try:
        print("启动批量上传系统...")
        cmd = [
            sys.executable, "-m", "streamlit", "run", 
            "bulk_uploader.py",
            "--server.port=8502",
            "--server.address=0.0.0.0",
            "--theme.primaryColor="#1f77b4",
            "--theme.backgroundColor=#FFFFFF"
        ]
        subprocess.run(cmd)
    except KeyboardInterrupt:
        print("\n上传应用已停止")
    except Exception as e:
        print(f"启动上传应用失败: {e}")

def main():
    """主函数"""
    current_dir = Path(__file__).parent
    os.chdir(current_dir)
    
    print("=== 门店报表查询系统 ===")
    print("1. 门店查询系统 (端口: 8501)")
    print("2. 批量上传系统 (端口: 8502)")
    print("3. 安装依赖包")
    print("4. 退出")
    
    while True:
        choice = input("\n请选择操作 (1-4): ").strip()
        
        if choice == "1":
            start_query_app()
        elif choice == "2":
            start_upload_app()
        elif choice == "3":
            if install_requirements():
                print("可以现在启动应用了！")
        elif choice == "4":
            print("再见！")
            break
        else:
            print("无效选择，请重试")

if __name__ == "__main__":
    main()