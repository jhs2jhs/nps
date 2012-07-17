import httplib
import urlparse
from pprint import pprint

host = 'www.amazon.com'
#host = 'www.usmc.mil'
#host = 'bit.lyy'
url = '/review/top-reviewers/'
#url = '/units/hqmc/'
#url = '/jAugPN'
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

def use_httplib(conn, url, headers):
    try:
        conn.request(method='GET', url=url, headers=headers)
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
    status_resp = resp.status
    reason_resp = resp.reason
    headers_resp = resp.getheaders() 
    print headers_resp
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

status, body = use_httplib(conn, url, headers)

#body = body.strip()

#print body

#from xml.dom import minidom
#doc = minidom.parseString(body)
#print doc
#body = body.decode('ISO-8859-1')

from lxml import etree
import lxml.html
from lxml.html.clean import clean_html
#root = etree.XML(body)
#print root
#parser = etree.HTMLParser()
#tree = etree.parse(body, parser)
parser = etree.HTMLParser(remove_blank_text=True, remove_comments=True)
#tree = etree.parse(body, parser)
html = lxml.html.fromstring(body, parser=parser)
#print lxml.html.tostring(html)
#print lxml.html.tostring(clean_html(html))
for h in html:
    print h
    for hs in h:
        print hs
print type(html)
result = html.xpath('//body')
#print lxml.html.tostring(result[0], pretty_print=True)


#print tree

#print body_resp
conn.close()
