import time, requests, re, datetime, random
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
import xlwt
from multiprocessing import Pool
import psycopg2

rank = 1 #global variable 存取当前搜索排名

def connect_db():
    try:
        conn = psycopg2.connect(database="postgres", user="postgres", password="1270", host="localhost", port="5432")
    except Exception as e:
        print(e)
    else:
        return conn
    return None

def close_db(conn):
    conn.commit()
    conn.close()

def create_db(conn):
    if not conn:
        print('connection failed')
        return
    cur = conn.cursor()
    try:
        cur.execute("CREATE TABLE bl_video(search_terms varchar,search_rank integer,up_id integer, up_username varchar,up_follow_num integer,video_url varchar(100),video_name varchar, video_published_at date,video_playback_num integer,video_barrage_num integer,video_like_num integer,video_coin_num integer,video_favorite_num integer,video_forward_num integer,category_1 varchar,created_at timestamp);")
        print('created table')
    except Exception as e:
        print(e)


def LoadUserAgent(uafile):
    uas = []
    with open(uafile,'rb') as uaf:
        for ua in uaf.readlines():
            if ua:
                uas.append(ua.strip()[1:-1])
    random.shuffle(uas)
    return uas

uas = LoadUserAgent("user_agents.txt")
ua = random.choice(uas)

headers = {
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,ja;q=0.7',
    'Connection': 'keep-alive',
    'Host': 'api.bilibili.com',
    'Origin': 'https://www.bilibili.com',
    'User-Agent': ua
}

#点赞数、硬币数、收藏数、转发数这四个数据值在各个视频网页里面，这个helper用来在搜索结果页面读取它们
def get_video_info(href):
    try:
        html = requests.get("http://api.bilibili.com/archive_stat/stat?aid="+href, headers=headers,timeout=6).json()
        data = html['data']
        like_num = data['like']
        coin_num = data['coin']
        favourite_num = data['favorite']
        forward_num = data['share']
        return [like_num, coin_num, favourite_num, forward_num]
    except Exception as e:
        print(e)
        return get_video_info(href)

#用beautiful soup分析网页元素
def get_source(browser, sheet, key, conn):
    time.sleep(3)
    html = browser.page_source
    soup = BeautifulSoup(html, 'lxml')
    save_to_excel(soup, sheet, key, conn)

#进入api网页获取up主信息
def get_up_info(href):
    try:
        html = requests.get("https://api.bilibili.com/x/relation/stat?vmid="+href, headers=headers, timeout=6).json()
        data = html['data']
        follower = data['follower']
        return follower
    except Exception as e:
        print(e)
        return get_up_info(href)

#格式：xx.x万，转换为int值存入数据库，少于一万还是int
def num2int(num):
    if num[len(num)-1] == '万':
        num = num[:-1]
        num = int(float(num)*10000)
    return num
    

#翻到搜索结果的下一页
def next_page(browser, page_num, sheet, key, conn):
    global rank
    WAIT = WebDriverWait(browser, 25)
    try:
        time.sleep(5)
        next_btn = WAIT.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'div.page-wrap > div > ul > li.page-item.next > button')))
        next_btn.click()
        WAIT.until(EC.text_to_be_present_in_element((By.CSS_SELECTOR, 'div.page-wrap > div > ul > li.page-item.active > button'), str(page_num)))
        get_source(browser, sheet, key, conn)
    except TimeoutException:
        print('connection time out at', rank)
        browser.refresh()
        return next_page(browser, page_num, sheet, key, conn)


def save_to_db(total_info, conn):
    cur = conn.cursor()
    try:
        cur.execute("insert into bl_video(search_terms,search_rank,up_id, up_username,up_follow_num,video_url,video_name, video_published_at,video_playback_num,video_barrage_num,video_like_num,video_coin_num,video_favorite_num,video_forward_num,category_1,created_at)"
        "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)" , total_info)
        print("add success")
    except Exception as e:
        print(e)
        conn.rollback()


#从搜索页获取视频信息
def save_to_excel(soup, sheet, key, conn):
    infos = soup.find_all(class_= 'video-item matrix')
    global rank
    
    for info in infos:
        title = info.find('a').get('title') #视频标题
        href = info.find('a').get('href') #视频url
        aid = re.findall(r'www.bilibili.com/video/av(.+?)?from=search', href) #av号
        aid = aid[0].strip('?')
        uspace = info.find('a', class_='up-name').get('href')
        uid = re.findall(r'space.bilibili.com/(.+?)?from=search', uspace)
        uid = uid[0].strip('?') #up主uid
        video_info = get_video_info(aid) 
        follower = get_up_info(uid)
        views = info.find(class_='so-icon watch-num').text.strip()
        views = num2int(views) #视频播放数
        barrages = info.find(class_='so-icon hide').text.strip()
        barrages = num2int(barrages) #视频弹幕量
        date = info.find(class_='so-icon time').text.strip() #上传时间
        up = info.find(class_='up-name').string.strip() #up主用户名
        category_1 = info.find(class_='type hide').string
        if not category_1:
            category_1 = 'null'
        else:
            category_1 = category_1.strip() #视频分区里层 （外层没找到）
        dt = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') #爬取时间

        #request访问api接口有时会返回NoneType
        if not video_info:
            video_info = (0,0,0,0)
        if not follower:
            follower = 0

        total_info = (key, rank, uid, up, follower, href, title, date,
                        views, barrages, video_info[0], video_info[1], video_info[2], video_info[3], category_1,
                        dt)
        save_to_db(total_info, conn) #写入postgresql数据库

        print(up)
        #写入excel表格
        sheet.write(rank, 0, key)
        sheet.write(rank, 1, rank)
        sheet.write(rank, 2, uid)
        sheet.write(rank, 3, up)
        sheet.write(rank, 4, follower)
        sheet.write(rank, 5, href)
        sheet.write(rank, 6, title)
        sheet.write(rank, 7, date)
        sheet.write(rank, 8, views)
        sheet.write(rank, 9, barrages)
        sheet.write(rank, 10, video_info[0])
        sheet.write(rank, 11, video_info[1])
        sheet.write(rank, 12, video_info[2])
        sheet.write(rank, 13, video_info[3])
        sheet.write(rank, 14, category_1)
        sheet.write(rank, 15, dt)
        
        rank+=1



def main():
    browser = webdriver.Chrome()
    browser.get('https://www.bilibili.com/')
    browser.refresh()
    input = browser.find_element(By.CLASS_NAME, 'nav-search-keyword')
    button = browser.find_element(By.CLASS_NAME, 'nav-search-btn')
    keys = ['职场','简历', '简历模板', '面试', '实习', '找工作', '笔试' ]
    workbook = xlwt.Workbook(encoding = 'utf-8')
    conn = connect_db()
    create_db(conn)
    for key in keys:
        input.clear()
        input.send_keys(key)
        button.click()
        all_h = browser.window_handles
        browser.switch_to.window(all_h[1])
        total_btn = browser.find_elements(By.CSS_SELECTOR, "div.page-wrap > div > ul > li.page-item.last > button")
        total = int(total_btn[0].text)
        print(total)

        sheet = workbook.add_sheet(key, cell_overwrite_ok=True)
        
        sheet.write(0,0,"search_terms")
        sheet.write(0,1,"search_rank")
        sheet.write(0,2,"up_id")
        sheet.write(0,3,"up_username")
        sheet.write(0,4,"up_follow_num")
        sheet.write(0,5,"video_url")
        sheet.write(0,6,"video_name")
        sheet.write(0,7,"video_published_at")
        sheet.write(0,8,"video_playback_num")
        sheet.write(0,9,"video_barrage_num")
        sheet.write(0,10,"video_like_num")
        sheet.write(0,11,"video_coin_num")
        sheet.write(0,12,"video_favorite_num")
        sheet.write(0,13,"video_forward_num")
        sheet.write(0,14,"category_1")
        sheet.write(0,15,"created_at")

        get_source(browser, sheet, key, conn)

        for i in range(2, total+1):
            next_page(browser, i, sheet, key, conn)
        
        browser.close()
        browser.switch_to.window(all_h[0])
        global rank
        rank = 1
        time.sleep(10)
        
    
    workbook.save('crawl.xls')
    close_db(conn)
    browser.quit()
    

if __name__ == "__main__":
    main()