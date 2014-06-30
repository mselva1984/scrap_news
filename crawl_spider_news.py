"""
Author = Selvakumar Manoharan
Contact = mselva1984@gmail.com
COMMENT = This file was developed for the crawling usatoday.com/sports/
and the crawled information is updated in the database,
MYSQL DB username,IP,PASSWORD,database nameand table name has to be configured.
if author is not present then the article is updated as N/A
number of comment is updated from FaceBook.

this script is executed as "scrapy runspider crawl_spider.py"
"""


from scrapy.selector import HtmlXPathSelector
from scrapy.spider import BaseSpider
from scrapy.http import Request
from urllib2 import urlopen
import re
import time
import datetime
import MySQLdb
import sys


DATE = "2012/11/04"
DOMAIN = 'usatoday.com'
URL = 'http://usatoday.com/sports/'
TIMEFILTER = time.mktime(datetime.datetime.strptime(DATE, "%Y/%m/%d").timetuple())
SQL_IP = "1.1.1.1"
SQL_USER = "selva"
SQL_PWD = "selva"
SQL_DB = "selva_db"


class MySpider(BaseSpider):
    name = DOMAIN
    allowed_domains = [DOMAIN]
    start_urls = [URL]
    def __init__(self):
        try:
            self.db = MySQLdb.connect(SQL_IP, SQL_USER, SQL_PWD, SQL_DB)
        except Exception, e:
            print "[DB_CONN_ERROR]:%s"%e
            sys.exit()
    def parse(self, response):
        hxs = HtmlXPathSelector(response)
        var_date_pattern = re.compile('\d{4}[-/]\d{2}[-/]\d{2}')
        for url in hxs.select('//div/@data-href').extract():
            url = str(url)
            var_datematch = var_date_pattern.search(url)
            if var_datematch != None:
                url_date = var_datematch.group(0)
                url_time = time.mktime(datetime.datetime.strptime(url_date, "%Y/%m/%d").timetuple())
                if url_time > TIMEFILTER:
                    url = 'http://usatoday.com'+url
                    yield Request(url, callback=self.parse_category)
                else:
                    print "filtered " +var_datematch.group(0)

    def parse_category(self, response):
        hxs = HtmlXPathSelector(response)
        author = hxs.select("//span[@class='author']//text()").extract()
        length_author = len(author)
        if length_author == 0:
            author = ["N/A"]

        title = hxs.select("//h4[@class='story-title']//text()").extract()
        length_title = len(title)
        if length_title != 0:
            title1 = title[0].strip()
        else:
            title1 = "N/A"

        var_cowboys_comment = " ".join(hxs.select("//p//text()").extract())
        var_cowboys_count = var_cowboys_comment.count("Cowboys")
        fb_url = "https://graph.facebook.com/?ids="+response.url+"&method=get&pretty=0&sdk=joey&callback=FB._callbacks.__gcb1"
        response = urlopen(fb_url)
        content = response.read()
        content_used = content.split("comments")
        if len(content1) > 1:
            var_comment_count = content_used[-1].split("}")[0]
            var_comment_count = var_comment_count.lstrip('":')
        else:
            var_comment_count = 0

        insert_qry = """INSERT INTO author values("%s", "%s", "%s", "%s", "%s")"""\
                      %(title1, response.url, author[0],\
                      str(var_comment_count), str(var_cowboys_count))
        cursor = self.db.cursor()
        try:
            cursor.execute(insert_qry)
        except Exception, e:
            print "[DB_INSERT_ERROR]:%s"%e
