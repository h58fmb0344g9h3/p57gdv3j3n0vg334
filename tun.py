import schedule
import time
from datetime import datetime

def reformat_file():
    # 打开原始文件
    with open('f74bjd2h2ko99f3j5', 'r') as file:
        lines = file.readlines()

    # 重新编排内容
    reformatted_lines = []
    for line in lines:
        ip, port, country, _ = line.strip().split(',')
        reformatted_lines.append(f"{ip}:{port}#{country}\n")

    # 将重新编排的内容写入新文件
    with open('proxy.txt', 'w') as file:
        file.writelines(reformatted_lines)

    print(f"文件已更新于 {datetime.now()}")

# 设置定时任务，每天上午9点执行
schedule.every().day.at("14:05").do(reformat_file)

# 保持脚本运行
while True:
    schedule.run_pending()
    time.sleep(1)
