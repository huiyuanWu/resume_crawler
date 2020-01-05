# resume_crawler

## 运行环境：
macOS 10.15.
## Dependencies:
Python 3.7.4, PostgreSQL 11.5, selenium, BeautifulSoup4, psycopg2.
## 使用方法: 
python crawler.py
## 输出：
crawl.xls, 一个excel表格，存储全部信息。 bl_video: 从postgresql中export的csv文件
## 已知bug/不足：
有时request库会返回NoneType,怀疑是访问过多ip被限制访问。selenium访问性能受限，过快也会导致连接超时。视频外层分区信息没有加入，只获取了内层分区信息。
