# -*- coding: utf-8 -*-
from html.parser import HTMLParser
from urllib import request
import re
import os
import logging
import subprocess
import traceback
import logging.handlers
import mysql.connector
from collections import defaultdict

# ログローテーション/フォーマットの設定
format_base = '%(asctime)s [%(levelname)s] [Thread-%(thread)d] %(filename)s:(%(lineno)d).%(funcName)s -%(message)s'
format = logging.Formatter(format_base)
output_log = './logs/'
filename = output_log + 'search-crawler-tbshd.log'
fh=logging.handlers.TimedRotatingFileHandler(filename=filename, when='MIDNIGHT', backupCount=8, encoding='utf-8')
fh.setLevel(logging.INFO)
fh.setFormatter(format)
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
ch.setFormatter(format)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(fh)
logger.addHandler(ch)

# tocrawl は検索クローラーをこれから走らせるURL
tocrawl = []
# crawled は検索クローラーを走らせ終わったURL
crawled = []
#DBに挿入するデータの雛形
html_header = { "type": "text/html" , "url":"", "description":"", "keywords":"", "title":""}
#HTMLParserを継承する。HTMLParserの機能をtbs_Htmlparserクラスで使うことができる。
class tbs_Htmlparser(HTMLParser):
	description_check = 0
	def handle_starttag(self, tag, attrs):
		# join_url に整形(合体)したURLを入れる
		join_url = []
		if tag == 'meta':
			if attrs[0][0] == 'charset':
				html_header['charset'] = attrs[0][1]
			if attrs[0][1] == 'description' and attrs[1][0] == 'content':
				html_header['description'] = 'exist'
			if attrs[0][1] == 'keywords' and attrs[1][0] == 'content':
				html_header['keywords'] = 'exist'
		if tag == 'title':
			html_header['title'] = 'exist'
		if html_header['description'] == 'exist':
			html_header['description'] = attrs[1][1]
		if html_header['keywords'] == 'exist':
			html_header['keywords'] = attrs[1][1]
		for attr in attrs:
			# <a href="xxxx"> を見つける
			if tag == 'a' and attr[0] == 'href':
				# <a href="xxxx"> のxxxx部分を抜き出す。
				attr_href = attr[1]
				# xxxxが「/~.html」「/~/」の形式のパスを見つけトップページドメインと結合させる。
				if re.match(r'^/.*\.html$', attr_href) != None or re.match(r'^/.*/$', attr_href) != None:
					join_url.append("https://www.xxxx.co.jp")
					join_url.append(attr_href)
					target_url = "".join(join_url)
					# リストの中にかぶっているURLがないかどうかチェックする。
					# checkが0の場合、クロール対象URLになるので tocrawlリストに挿入する。
					# checkが1の場合は何もしない。
					check = self.check_list(target_url)
					if check == 0:
						tocrawl.append(target_url)
	# <a href=”www.hoge.com”>xxx</a>の「xxx」部分を抽出する
	def handle_data(self, data):
		if html_header['title'] == 'exist':
			html_header['title'] = data
	
	# tocrawlリストと、crawledリストに、対象URLが入っていた場合は checkを1にして returnする
	def check_list(self,join_url):
		check = 0
		for url in tocrawl:
			if join_url == url:
				check = 1
		for url in crawled:
			if join_url == url:
				check = 1
		return check

if __name__ == '__main__':
	crawler = tbs_Htmlparser()
	tbshd = 'https://www.xxxx.co.jp'
	html_top = request.urlopen(tbshd)
	print(html_top.getheaders())
	html_str = html_top.read().decode('utf-8')
	html_str_del = re.sub('<.*?>','',html_str)
	html_str_del = re.sub('\r','',html_str_del)
	html_str_del = re.sub('\n','',html_str_del)
	# パーサーにテキストを入力(str型限定)。
	crawler.feed(html_str)
	html_header['url'] = tbshd
	# DB connect
	con = mysql.connector.connect(
		host='localhost',
		port='3307',
		user='root',
		password='hoge',
		database='hoge',
		charset="utf8")
	cur = con.cursor(buffered=True)
	sql = ('''INSERT INTO `hoge` (url, title, description, keywords, body)VALUES (%s, %s, %s, %s, %s)''')
	data = [(html_header['url'], html_header['title'],html_header['description'],html_header['keywords'],html_str_del)]
	cur.executemany(sql, data)
	crawled.append(tbshd)
	while len(tocrawl) > 0:
		html_header['url']=''
		html_header['title']=''
		html_header['description']=''
		html_header['keywords']=''
		try:
			target_craw = tocrawl[0]
			html = request.urlopen(target_craw)
			if html.getcode()==200:
			# ステータスが200の時は
				html_str = html.read().decode('utf-8')
				crawler.feed(html_str)
				html_header['url'] = target_craw
				crawled.append(target_craw)
				tocrawl.pop(0)
				#タグ、改行などを削除
				html_str_del = re.sub('<.*?>','',html_str)
				html_str_del = re.sub('\n','',html_str_del)
				html_str_del = re.sub('\r','',html_str_del)
				#空白を削除
				html_str_del = re.sub(" ", "", html_str_del)
				sql = ('''INSERT INTO `data` (url, title, description, keywords, body)VALUES (%s, %s, %s, %s, %s)''')
				data = [(html_header['url'], html_header['title'],html_header['description'],html_header['keywords'], html_str_del)]
				cur.executemany(sql, data)
			else:
				tocrawl.pop(0)
		except ConnectionError as e:
			for err in traceback.format_exc().split('\n'):
				logger.error(err)
			check_err = 0
			for err in err_occur:
				if err == target_craw:
					check_err = 1
			if check_err == 0:
				err_occur.append(target_craw)
		finally:
			pass
	cur.close()
	con.close()

