import httplib
import urlparse
from pprint import pprint
from lxml import etree
from lxml.etree import tostring
import lxml.html
from lxml.html.clean import clean_html, Cleaner
import random
from Queue import Queue
from threading import Thread, Lock, stack_size
import threading
import time

host = 'www.amazon.com'
url = '/review/top-reviewers/'
port = 80
strict = 1
timeout = 10
source_address = None # not used at moment
#conn = httplib.HTTPConnection(host=host, port=port, strict=strict, timeout=timeout, source_address=source_address)     # how to sort the proxy? http://www.velocityreviews.com/forums/t325113-httplib-and-proxy.html 
headers = {
    #"Host":"www.amazon.com", 
    "Connection":"keep-alive",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.52 Safari/536.5",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    #"Accept-Encoding": "gzip,deflate,sdch",
    "Accept-Language": "en-US,en;q=0.8",
    "Accept-Charset": "ISO-8859-1,utf-8;q=0.7,*;q=0.3",
    }

def get_conn_http():
    conn = httplib.HTTPConnection(host=host, port=port, strict=strict, timeout=timeout, source_address=source_address)
    return conn

conn = get_conn_http()


###############
def use_httplib(url, headers):
    global conn
    try:
        if conn == None:
            print conn, type(conn)
            conn = get_conn_http()
        conn.request(method='GET', url=url, headers=headers)
        #print '**ready'
        status, body = use_httplib_resp(conn)
        return status, body
    except Exception as e:
        print '####exception', e, type(conn), conn
        conn.close()
        conn = None
        return -1, e

def use_httplib_redirect(host, url, headers):
    print host, url, "======= redirect ========"
    try:
        conn_c = httplib.HTTPConnection(host=host, port=80)
        conn_c.request(method="GET", url=url, headers=headers)
        status, body = use_httplib_resp(conn)
        conn_c.close()
        return status, body
    except Exception as e:
        print 'exception', e
        return -1, e

def use_httplib_resp(conn):
    resp = conn.getresponse()
    #print "*//togo"
    status_resp = resp.status
    reason_resp = resp.reason
    headers_resp = resp.getheaders() 
    #print headers_resp
    if 300 <= status_resp < 400 : # redirect
        location = resp.getheader('Location')
        parsed = urlparse.urlparse(location)
        host_r = parsed.netloc
        url_r = parsed.path
        if location != None: # return None if it is not exist
            return use_httplib_redirect(host_r, url_r, headers)
        else:
            return status_resp, 'Location is None:'
    msg_resp = resp.msg
    body_resp = resp.read()
    v_resp = resp.version
    return status_resp, body_resp

#################
def body_clean(body):
    cleaner = Cleaner(style=True, javascript=True, page_structure=True)
    body = cleaner.clean_html(body)
    parser = etree.HTMLParser(remove_blank_text=True, remove_comments=True)
    html = lxml.html.fromstring(body, parser=parser)
    return html

#body = body.decode('ISO-8859-1') # can get from http header to determine which code for decoding
def use_lxml_reviewer_rank(body):
    html = body_clean(body)
    paths = '//tr[contains(@id, "reviewer")]' # lxml only support xpath 1.0, so can not use matches()
    results = html.xpath(paths)
    for r in results:
        name = r.findall('.//span')[0].text
        link = r.findall('.//span/../..')[0].get('href').strip()
        aid = link.split('/')[6]
        crnum = r.findall('.//td[@class="crNum"]')
        rank = crnum[0].text.replace('#', '').strip()
        total_reviews = crnum[1].text.strip()
        helpful_votes = crnum[2].text.strip()
        percent_helpful = r.findall('.//td[@class="crNumPercentHelpful"]')[0].text.strip()
        fan_voters = r.findall('.//td[@class="crNumFanVoters"]')[0].text.strip()
        db_general_execute(sql_reviewer_insert, (name, aid, link, rank, total_reviews, helpful_votes, percent_helpful, fan_voters))
        

pds = {}
def use_lxml_reviewer_profile(body, aid):
    html = body_clean(body)
    #badges
    paths = '//div[@class="badges"]'
    rs = html.xpath(paths)
    if len(rs):
        rs = rs[0]
        for r in rs:
            badget = r[0].get('alt').strip()
            db_general_execute(sql_reviewer_badget_insert, (aid, badget))
    # hall of fame years
    paths = '//div[@class="hallofFameYears"]'
    rs = html.xpath(paths)
    if len(rs):
        rs = rs[0]
        years = rs.text.replace('Hall of Fame Reviewer -', '').strip()
        years = years.split(' ')
        for year in years:
            db_general_execute(sql_reviewer_badget_insert, (aid, year))
    # vote values:
    paths = '//span[@class="value"]'
    rs = html.xpath(paths)
    if len(rs):
        rs = rs[0]
        values = tostring(rs[0]).strip() # the format is strange here
        values = values.split('(')[1]
        values = values.split(')')[0]
        values = values.split('of')
        values_x = values[0]
        values_y = values[1]
        db_general_execute(sql_reviewer_helpful_update, (values_x, values_y, aid))
    # personal details
    paths = '//div[@class="personalDetails xxsmall"]'
    rs = html.xpath(paths)
    if len(rs):
        rs = rs[0]
        for r in rs:
            b = r.find('.//b')
            bt = b.text.strip()
            if bt == "E-mail:":
                email = b.getnext()[0].text
                db_general_execute(sql_reviewer_email_update, (email, aid))
            elif bt == 'Anniversary:':
                anniversary = b.getnext().text
                db_general_execute(sql_reviewer_anniversary_update, (anniversary, aid))
            elif bt == 'Birthday:':
                birthday = b.getnext().text
                db_general_execute(sql_reviewer_birthday_update, (birthday, aid))
            elif bt == 'Web Page:':
                webpage = b.getnext()[0].text
                db_general_execute(sql_reviewer_webpage_update, (webpage, aid))
            elif bt == 'Location:':
                location = b.getparent()[0]
                location = tostring(location)
                location = location.replace('<b>Location:</b>', '').strip()
                db_general_execute(sql_reviewer_location_update, (location, aid))
            elif bt == 'In My Own Words:':
                own_words = b.getparent().getnext().text
                db_general_execute(sql_reviewer_my_own_words_update, (own_words, aid))
            else:
                print "wrong bt:", bt
                break
        # Interests & Tags
        paths = '//div[@id="interestsTags"]'
        rs = html.xpath(paths)
        if len(rs):
            rs = rs[0]
            paths = '//b[@class="orange"]'
            rs = rs.xpath(paths)
            for r in rs:
                bt = r.text
                if bt == "Interests":
                    interests = r.getnext().text
                    for interest in interests:
                        db_general_execute(sql_reviewer_interest_insert, (aid, interest))
                if bt == "Frequently Used Tags":
                    tags = r.getnext()
                    for t in tags:
                        tag = t.text
                        tag_c = t.get('title').replace('tagged items', '').strip()
                        db_general_execute(sql_reviewer_tag_insert, (aid, tag, tag_c))
        #db_general_execute(sql_reviewer_profile_finish_update, (aid, )) 
            
    
def use_lxml_review_list(body, aid):
    c = conn_db.cursor()
    html = body_clean(body)
    #paths = '//span[text()="Price:"]'
    paths = '//b[text()="Availability:"]'
    rs = html.xpath(paths)
    for r in rs:
        print "=="
        products = r.xpath('../../../tr')
        product = products[0][0][0][0]
        product_name = product.text
        if product_name != None:
            product_link = product.get('href')
            product_link = product_link.split('/ref')[0]
        else:
            product_link = ''
            product_name = ''
        if len(products) == 4:
            offered = products[1][0][0]
            offered_by = tostring(offered)
        else:
            offered_by = ''
        # review 
        review = products[0].xpath('../../..')[0].getnext()
        review_id = review.xpath('./td/a')[0].get('name')
        review_helpful = review.xpath('./td/div/div[contains(text(), "people found the following review helpful")]')
        if len(review_helpful) > 0:
            review_help = review_helpful[0].text.strip().split('people')[0].split('of')
            #print review_help
            review_help_x = review_help[0]
            review_help_y = review_help[1]
        else:
            review_help_x = 0
            review_help_y = 0
        # review meta
        first = review.xpath('./td/div/div/span/img[contains(@src, "customer-reviews/stars-")]/../..')[0]
        #print tostring(first)
        review_stars = first[0][0].get('alt').split('out')[0].strip()
        review_title = first[1].text
        review_time = first.xpath('text()')[2].strip(', ')
        comments = review.xpath('./td/div/div[last()]/a')[1].text.replace('Comment', '').strip()
        if comments == '' or comments == None:
            comments = 0
        else:
            comments = comments.strip('( ) ')
        premalink = review.xpath('./td/div/div[last()]/a')[2]
        permalink = review.xpath('./td/div/div[last()]/a')[2].text.replace('Permalink', '').strip()
        if permalink == '' or permalink == None:
            permalink = 0
        else:
            permalink = permalink.strip('( ) ')
        # review text
        review_content = review.xpath('./td/div/text()')
        review_content = ''.join(review_content)
        review_content = review_content.strip('\n ')
        #print aid, review_id, review_help_x, review_help_y, review_stars, review_time, review_title, comments, permalink, product_name, product_link, offered_by
        c.execute(sql_review_insert, (aid, review_id, review_help_x, review_help_y, review_stars, review_time, review_title, comments, permalink, review_content, product_name, product_link, offered_by))
        conn_db.commit()
    c.close()
    #print results
    #return results
        

############### for spped, i may use multiple thread
def reviewers_rank_read(page_id):
    print '** rank', page_id, "**"
    url = '/review/top-reviewers/ref=cm_cr_tr_link_%s?ie=UTF8&page=%s'%(page_id, page_id)
    status, body = use_httplib(url, headers)
    if status == 200:
        use_lxml_reviewer_rank(body)
        db_general_execute(sql_rank_read_status_update, (page_id, ))

def reviewer_profile_read(link, rank):
    url = link.replace('http://www.amazon.com', '').strip()
    print "** profile", url, rank, "**"
    aid = link.split('/')[6]
    status, body = use_httplib(url, headers)
    if status == 200:
        use_lxml_reviewer_profile(body, aid)
        db_general_execute(sql_profile_read_status_update, (aid, ))
    print status


def review_lists_read(aid, page_id, rank):
    print "** review", aid, page_id, rank, "**"
    url = '/gp/cdp/member-reviews/%s?ie=UTF8&display=public&page=%s&sort_by=MostRecentReview'%(aid, page_id)
    status, body = use_httplib(url, headers) 
    if status == 200:
        use_lxml_review_list(body, aid)
        db_general_execute(sql_review_read_status_update, (aid, page_id))
    


###############
sql_init = '''
CREATE TABLE IF NOT EXISTS reviewer (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  -- from rank list
  name TEXT,
  aid TEXT NOT NULL UNIQUE, 
  link TEXT, 
  rank TEXT, 
  total_reviews TEXT, 
  helpful_votes TEXT, 
  percent_helpful TEXT, 
  fan_voters TEXT, 
  -- from reviewer profile 
  helpful_x TEXT DEFAULT 0,
  helpful_y TEXT DEFAULT 0,
  email TEXT,
  anniversary TEXT,
  birthday TEXT, 
  web_page TEXT, 
  location TEXT, 
  in_my_own_words TEXT
);
CREATE TABLE IF NOT EXISTS reviewer_badget (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  aid TEXT NOT NULL,
  badget TEXT NOT NULL,
  UNIQUE (aid, badget)
);
CREATE TABLE IF NOT EXISTS reviewer_hall_of_fame_year (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  aid TEXT NOT NULL,
  year TEXT NOT NULL, 
  UNIQUE (aid, year)
);
CREATE TABLE IF NOT EXISTS reviewer_interest (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  aid TEXT NOT NULL,
  interest TEXT NOT NULL,
  UNIQUE (aid, interest)
);
CREATE TABLE IF NOT EXISTS reviewer_tag (
  id INTEGER PRIMARY KEY AUTOINCREMENT, 
  aid TEXT NOT NULL,
  tag TEXT, 
  tag_count TEXT DEFAULT 0,
  UNIQUE (aid, tag)
);
CREATE TABLE IF NOT EXISTS review (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  aid TEXT NOT NULL,
  review_id TEXT UNIQUE NOT NULL,
  review_help_x TEXT DEFAULT 0,
  review_help_y TEXT DEFAULT 0,
  review_stars TEXT DEFAULT 0,
  review_time TEXT, 
  review_title TEXT,
  review_comment TEXT DEFAULT 0,
  review_premalink TEXT DEFAULT 0,
  review_text TEXT,
  product_name TEXT, 
  product_link TEXT,
  offered_by TEXT
);
-- read status
CREATE TABLE IF NOT EXISTS rank_read_status (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  page_id TEXT NOT NULL UNIQUE, 
  read_status TEXT DEFAULT 0
);
CREATE TABLE IF NOT EXISTS profile_read_status (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  aid TEXT NOT NULL UNIQUE, 
  link TEXT NOT NULL,
  rank TEXT NOT NULL, 
  read_status TEXT DEFAULT 0
);
CREATE TABLE IF NOT EXISTS review_read_status (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  aid TEXT NOT NULL,
  rank TEXT, 
  page_id TEXT NOT NULL,
  read_status TEXT DEFAULT 0, -- 0 means not read yet
  UNIQUE (aid, page_id)
);
'''

import sqlite3
db_path = './nps.db'
conn_db = sqlite3.connect(db_path)
def db_init():
    c = conn_db.cursor()
    c.executescript(sql_init)
    conn_db.commit()
    c.execute('SELECT * FROM SQLITE_MASTER')
    tables = c.fetchall()
    print '** tables total number: '+str(len(tables))
    c.close()

def db_general_execute (sql, params):
    c = conn_db.cursor()
    c.execute(sql, params)
    conn_db.commit()
    c.close()


sql_reviewer_insert = '''
INSERT OR IGNORE INTO reviewer (name, aid, link, rank, total_reviews, helpful_votes, percent_helpful, fan_voters) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
'''

sql_reviewer_badget_insert = '''
INSERT OR IGNORE INTO reviewer_badget (aid, badget) VALUES (?, ?)
'''

sql_reviewer_hall_of_fame_year_insert = '''
INSERT OR IGNORE INTO reviewer_hall_of_fame_year (aid, year) VALUES (?, ?)
'''

sql_reviewer_helpful_update = '''
UPDATE reviewer SET helpful_x = ?, helpful_y = ? WHERE aid = ?
'''
sql_reviewer_email_update = '''
UPDATE reviewer SET email = ? WHERE aid = ?
'''
sql_reviewer_anniversary_update = '''
UPDATE reviewer SET anniversary = ? WHERE aid = ?
'''
sql_reviewer_webpage_update = '''
UPDATE reviewer SET web_page = ? WHERE aid = ?
'''
sql_reviewer_location_update = '''
UPDATE reviewer SET location = ? WHERE aid = ?
'''
sql_reviewer_birthday_update = '''
UPDATE reviewer SET birthday = ? WHERE aid = ?
'''
sql_reviewer_my_own_words_update = '''
UPDATE reviewer SET in_my_own_words = ? WHERE aid = ?
'''

sql_reviewer_interest_insert = '''
INSERT OR IGNORE INTO reviewer_interest (aid, interest) VALUES (?, ?)
'''
sql_reviewer_tag_insert = '''
INSERT OR IGNORE INTO reviewer_tag (aid, tag, tag_count) VALUES (?, ?, ?)
'''

sql_reviewer_profile_finish_update = '''
UPDATE reviewer SET profile_page_status = 1 WHERE aid = ?
'''

sql_review_insert = '''
INSERT OR IGNORE INTO review (aid, review_id, review_help_x, review_help_y, review_stars, review_time, review_title, review_comment, review_premalink, review_text, product_name, product_link, offered_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
'''


sql_rank_read_status_insert = '''
INSERT OR IGNORE INTO rank_read_status (page_id) VALUES (?)
'''
sql_profile_read_status_insert = '''
INSERT OR IGNORE INTO profile_read_status (aid, link, rank) VALUES (?, ?, ?)
'''
sql_review_read_status_insert = '''
INSERT OR IGNORE INTO review_read_status (aid, rank, page_id) VALUES (?, ?, ?)
'''
sql_review_read_status_update = '''
UPDATE review_read_status SET read_status = 1 WHERE aid = ? AND page_id = ?
'''
sql_rank_read_status_update = '''
UPDATE rank_read_status SET read_status = 1 WHERE page_id = ?
'''
sql_profile_read_status_update = '''
UPDATE profile_read_status SET read_status = 1 WHERE aid = ?
'''


################
#print tree
db_init()


def read_rank():
    c = conn_db.cursor()
    c.execute('SELECT page_id FROM rank_read_status WHERE read_status = 0', ())
    flag = 0
    r = c.fetchone()
    while r != None:
    #for r in c.fetchall():
        page_id = r[0]
        reviewers_rank_read(str(page_id))
        flag = flag + 1
        r = c.fetchone()
    c.close()
    if flag > 0:
        read_rank()

def read_profile():
    c = conn_db.cursor()
    c.execute('SELECT aid, link, rank FROM profile_read_status WHERE read_status = 0', ()) 
    flag = 0
    r = c.fetchone()
    while r != None:
    #for r in c.fetchall():
        aid = r[0]
        link = r[1]
        rank = r[2]
        reviewer_profile_read(str(link), str(rank))
        flag = flag + 1
        r = c.fetchone()
    c.close()
    print "&&&&&&&&&&&& flag:", flag
    if flag > 0:
        read_profile()

def read_review(): # consider to use multiple thread
    c = conn_db.cursor()
    c.execute('SELECT aid, page_id, rank FROM review_read_status WHERE read_status = 0', ())
    flag = 0
    r = c.fetchone()
    while r != None:
    #for r in c.fetchall():
        aid = r[0]
        page_id = r[1]
        rank = r[2]
        review_lists_read(str(aid), str(page_id), str(rank))
        flag = flag + 1
        r = c.fetchone()
    c.close()
    if flag > 0:
        read_review()

qi = Queue()
qo = Queue(5)
def read_review_thread(): # consider to use multiple thread
    c = conn_db.cursor()
    c.execute('SELECT aid, page_id, rank FROM review_read_status WHERE read_status = 0', ())
    for i in range(5):
        t = ThreadReview(qi, qo)
        t.setDaemon(True)
        t.start()
    flag = 0
    for r in c.fetchall():
        aid = r[0]
        page_id = r[1]
        rank = r[2]
        #review_lists_read(str(aid), str(page_id))
        qi.put((str(aid), str(page_id)))
        flag = flag + 1
    c.close()
    #qi.join()
    while not qo.empty():
        results, aid, page_id = qo.get()
        review_thread_db(results, aid, page_id)
    if flag == 0:
        return 
    else:
        read_review()

def review_thread_db(results, aid, page_id):
    #results, aid, page_id = rs
    c = conn_db.cursor()
    #print aid, page_id
    for result in results:
        aid, review_id, review_help_x, review_help_y, review_stars, review_time, review_title, comments, permalink, review_content, price, product_name, product_link, offered_by = result
        c.execute(sql_review_insert, (aid, review_id, review_help_x, review_help_y, review_stars, review_time, review_title, comments, permalink, review_content, price, product_name, product_link, offered_by))
        conn_db.commit()
        print result
    #c.execute(sql_review_read_status_update, (aid, page_id))
    #conn_db.commit()
    c.close()

class ThreadReview(threading.Thread):
    def __init__(self, qi, qo):
        threading.Thread.__init__(self)
        self.conn = httplib.HTTPConnection(host=host, port=port, strict=strict, timeout=timeout, source_address=source_address)
        self.qi = qi
        self.qo = qo
    def run(self):
        while True:
            if self.qi.empty(): break
            aid, page_id = self.qi.get()
            out = review_lists_read(self.conn, aid, page_id)
            self.qo.put((out, aid, page_id))
            self.qi.task_done()
            time.sleep(0.1)


class FetchIO():
    def __init__(self, threads):
        self.lock = Lock()
        self.threads = threads
        self.qi = Queue()
        self.qo = Queue(threads)
        for i in range(threads):
            t = ThreadReview(target=self.threadget)
            t.setDaemon(True)
            t.start()
        self.running = 0
    
    def __del__(self):
        #time.sleep(0)
        self.qi.join()
        self.qo.join()
    
    def taskleft(self):
        return self.qi.qsize()+self.qo.qsize()+self.running
    
    def push(self, req):
        self.qi.put(req)
    
    def pop(self):
        return self.qo.get()
        
    def threadget(self):
        while True:
            aid, page_id = self.qi.get()
            with self.lock:
                self.running += 1
            print "[[[[[[[[[[", aid, page_id, "]]]]]]]]"
            out = review_lists_read(self.conn, aid, page_id)
            #print out
            print "(((((((((((", len(out), "))))))))))))"
            self.qo.put((out, aid, page_id), True, None)
            with self.lock:
                self.running -= 1
            self.qi.task_done()
            #time.sleep(0.1)


def read_main():
    c = conn_db.cursor()
    cc = conn_db.cursor()
    # rank 
    '''
    ls = range(1, 1001)
    for l in ls:
        cc.execute(sql_rank_read_status_insert, (l, ))
        conn_db.commit()
        print "rank: ", l
    read_rank()
    '''
    # profile prepare
    '''
    c.execute('SELECT aid, link, rank FROM reviewer', ()) # create reading list table
    for r in c.fetchall():
        aid = r[0]
        link = r[1]
        rank = r[2]
        cc.execute(sql_profile_read_status_insert, (aid, link, rank))
        conn_db.commit()
        print "profile: ", rank
    '''
    read_profile()
    #review prepare:
    '''
    c.execute('SELECT aid, link, total_reviews, rank FROM reviewer', ()) # create reading list table
    r = c.fetchone()
    while r != None:
    #for r in c.fetchall():
        aid = r[0]
        total_reviews = r[2]
        rank = r[3]
        total_reviews = total_reviews.replace(',', '').strip()
        reviews = int(total_reviews)
        last = (reviews - 1)/10+1
        ls = range(1, last+1)
        for l in ls:
            page_id = l
            cc.execute(sql_review_read_status_insert, (aid, rank, page_id))
            conn_db.commit()
        print "review: ", rank, aid
        r = c.fetchone()
    '''
    read_review()
    c.close()
    cc.close()

def table_clean():
    c = conn_db.cursor()
    c.execute('DROP TABLE IF EXISTS rank_read_status', ())
    c.execute('DROP TABLE IF EXISTS profile_read_status', ())
    c.execute('DROP TABLE IF EXISTS review_read_status', ())
    conn_db.commit()
    c.close()

#table_clean()
read_main()
#read_review()

#### need to check the db table, some column are not used in reviewer

#print body_resp
conn.close()
conn_db.close()
