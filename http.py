import httplib
import urlparse
from pprint import pprint
from lxml import etree
from lxml.etree import tostring
import lxml.html
from lxml.html.clean import clean_html, Cleaner

host = 'www.amazon.com'
url = '/review/top-reviewers/'
port = 80
strict = 1
timeout = 10
source_address = None # not used at moment
conn = httplib.HTTPConnection(host=host, port=port, strict=strict, timeout=timeout, source_address=source_address)     # how to sort the proxy? http://www.velocityreviews.com/forums/t325113-httplib-and-proxy.html 
headers = {
    #"Host":"www.amazon.com", 
    "Connection":"keep-alive",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.52 Safari/536.5",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    #"Accept-Encoding": "gzip,deflate,sdch",
    "Accept-Language": "en-US,en;q=0.8",
    "Accept-Charset": "ISO-8859-1,utf-8;q=0.7,*;q=0.3",
    }


###############
def use_httplib(conn, url, headers):
    try:
        conn.request(method='GET', url=url, headers=headers)
        #print '**ready'
        status, body = use_httplib_resp(conn)
        return status, body
    except Exception as e:
        print 'exception', e
        return -1, e

def use_httplib_redirect(host, url, headers):
    print host, url, "======= redirect ========"
    try:
        conn = httplib.HTTPConnection(host=host, port=80)
        conn.request(method="GET", url=url, headers=headers)
        status, body = use_httplib_resp(conn)
        conn.close()
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
        #print etree.tostring(link)
        crnum = r.findall('.//td[@class="crNum"]')
        rank = crnum[0].text.replace('#', '').strip()
        total_reviews = crnum[1].text.strip()
        helpful_votes = crnum[2].text.strip()
        percent_helpful = r.findall('.//td[@class="crNumPercentHelpful"]')[0].text.strip()
        fan_voters = r.findall('.//td[@class="crNumFanVoters"]')[0].text.strip()
        #print name, aid, link, rank, total_reviews, helpful_votes, percent_helpful, fan_voters
        #reviewer_profile_read(link)
        review_lists_read(total_reviews, aid)
        break

pds = {}
def use_lxml_reviewer_profile(body):
    html = body_clean(body)
    #badges
    paths = '//div[@class="badges"]'
    rs = html.xpath(paths)
    if len(rs):
        rs = rs[0]
        for r in rs:
            badget = r[0].get('alt').strip()
            print badget
    # hall of fame years
    paths = '//div[@class="hallofFameYears"]'
    rs = html.xpath(paths)
    if len(rs):
        rs = rs[0]
        years = rs.text.replace('Hall of Fame Reviewer -', '').strip()
        years = years.split(' ')
        print years
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
        print values
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
                print bt, email
            elif bt == 'Anniversary:':
                anniversary = b.getnext().text
                print bt, anniversary
            elif bt == 'Birthday:':
                birthday = b.getnext().text
                print bt, birthday
            elif bt == 'Web Page:':
                webpage = b.getnext()[0].text
                print bt, webpage
            elif bt == 'Location:':
                location = b.getparent()[0]
                location = tostring(location)
                location = location.replace('<b>Location:</b>', '').strip()
                print bt, location
            elif bt == 'In My Own Words:':
                own_words = b.getparent().getnext().text
                #print bt, own_words
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
                    print bt, interests
                if bt == "Frequently Used Tags":
                    tags = r.getnext()
                    for t in tags:
                        tag = t.text
                        tag_c = t.get('title').replace('tagged items', '').strip()
                        print tag, tag_c
            
        # review_all link:
        paths = '//div[@class="seeAll xsmall"]'
        rs = html.xpath(paths)
        if len(rs):
            rs = rs[0]
            ra_link = rs[0].get('href')
            print ra_link # calculate how many pages of review based on total number divided by 10, then check each pages. 
            
    
def use_lxml_review_list(body):
    html = body_clean(body)
    paths = '//span[text()="Price:"]'
    rs = html.xpath(paths)
    for r in rs:
        print "================="
        price = r.getparent().xpath('text()')[0].strip()
        print price
        products = r.xpath('../../../../../tr')
        product = products[0][0][0][0]
        product_name = product.text
        product_link = product.get('href')
        print product_link.split('/ref')[0]
        #print tostring(product)
        #print product
        if len(products) == 4:
            offered = products[1][0][0]
            offered_by = tostring(offered)
            print offered_by
        review = products[0].xpath('../../..')[0].getnext()
        #print tostring(review)
        #print review
        review_name = review.xpath('./td/a')[0].get('name')
        print review_name
        review_helpful = review.xpath('./td/div/div[contains(text(), "people found the following review helpful")]')
        if len(review_helpful) > 0:
            review_help = review_helpful[0].text.strip().split('people')[0].split('of')
            #print review_help
            review_help_x = review_help[0]
            review_help_y = review_help[1]
        else:
            review_help_x = 0
            review_help_y = 0
        print review_help_x, review_help_y
        first = review.xpath('./td/div/div/span/img[contains(@src, "customer-reviews/stars-")]/../..')[0]
        #print tostring(first)
        stars = first[0][0].get('alt').split('out')[0].strip()
        print stars
        review_title = first[1].text
        print review_title
        review_time = first.xpath('text()')[2].strip(', ')
        print review_time
        comments = review.xpath('./td/div/div[last()]/a')[1].text.replace('Comment', '').strip()
        if comments == '' or comments == None:
            comments = 0
        else:
            comments = comments.strip('( ) ')
        print comments
        premalink = review.xpath('./td/div/div[last()]/a')[2]
        permalink = review.xpath('./td/div/div[last()]/a')[2].text.replace('Permalink', '').strip()
        if permalink == '' or permalink == None:
            permalink = 0
        else:
            permalink = permalink.strip('( ) ')
        print permalink
        review_content = review.xpath('./td/div/text()')
        review_content = ''.join(review_content)
        review_content = review_content.strip('\n ')
        #print review_content
        

############### for spped, i may use multiple thread
#for i in range(1, 1000):
def reviewers_rank_read(ls):
    for i in ls:
        print '********* reviewer rank', i, "***************"
        url = '/review/top-reviewers/ref=cm_cr_tr_link_%d?ie=UTF8&page=%d'%(i, i)
        status, body = use_httplib(conn, url, headers)
        use_lxml_reviewer_rank(body)

def reviewer_profile_read(link):
    url = link.replace('http://www.amazon.com', '').strip()
    print url
    status, body = use_httplib(conn, url, headers)
    #print body
    use_lxml_reviewer_profile(body)


def review_lists_read(reviews, aid):
    if type(reviews) == str:
        reviews = int(reviews)
    total = reviews / 10 + 1
    last = reviews % 10
    ls = range(1, total+1)
    for i in ls:
        print "############## review list", i, "#####################"
        url = '/gp/cdp/member-reviews/%s?ie=UTF8&display=public&page=%d&sort_by=MostRecentReview'%(aid, i)
        status, body = use_httplib(conn, url, headers)
        use_lxml_review_list(body)
        #break
    


###############
sql_init = '''
CREATE TABLE IF NOT EXISTS reviewer(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT,
  aid TEXT NOT NULL UNIQUE, 
  link TEXT, 
  total_reviews TEXT, 
  helpful_votes TEXT, 
  percent_helpful TEXT, 
  fan_voters TEXT
),
'''

################
#print tree
ls = range(1, 2)
reviewers_rank_read(ls)

#print body_resp
conn.close()
