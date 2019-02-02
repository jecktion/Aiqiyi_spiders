# -*- coding: utf-8 -*-
# 此程序用来抓取爱奇艺的数据
import hashlib
import os

import requests
import time
import random
import re
from multiprocessing.dummy import Pool
import csv
import json
import sys
from fake_useragent import UserAgent, FakeUserAgentError
from save_data import database

class Spider(object):
	def __init__(self):
		try:
			self.ua = UserAgent(use_cache_server=False).random
		except FakeUserAgentError:
			pass
		# self.limits = 50000
		# self.date = '2000-01-01'
		self.db = database()

	def get_headers(self):
		# user_agent = self.ua.chrome
		user_agents = ['Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20130406 Firefox/23.0',
		               'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:18.0) Gecko/20100101 Firefox/18.0',
		               'IBM WebExplorer /v0.94', 'Galaxy/1.0 [en] (Mac OS X 10.5.6; U; en)',
		               'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)',
		               'Opera/9.80 (Windows NT 6.0) Presto/2.12.388 Version/12.14',
		               'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0; TheWorld)',
		               'Opera/9.52 (Windows NT 5.0; U; en)',
		               'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.2pre) Gecko/2008071405 GranParadiso/3.0.2pre',
		               'Mozilla/5.0 (Windows; U; Windows NT 5.2; en-US) AppleWebKit/534.3 (KHTML, like Gecko) Chrome/6.0.458.0 Safari/534.3',
		               'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/532.0 (KHTML, like Gecko) Chrome/4.0.211.4 Safari/532.0',
		               'Opera/9.80 (Windows NT 5.1; U; ru) Presto/2.7.39 Version/11.00']
		user_agent = random.choice(user_agents)
		headers = {'host': "sns-comment.iqiyi.com",
		           'connection': "keep-alive",
		           'user-agent': user_agent,
		           'accept': "*/*",
		           'referer': "https://www.iqiyi.com.html",
		           'accept-encoding': "gzip, deflate, br",
		           'accept-language': "zh-CN,zh;q=0.9"
		           }
		return headers
	
	def p_time(self, stmp):  # 将时间戳转化为时间
		stmp = float(str(stmp)[:10])
		timeArray = time.localtime(stmp)
		otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
		return otherStyleTime
	
	def replace(self, x):
		# 将其余标签剔除
		removeExtraTag = re.compile('<.*?>', re.S)
		x = re.sub(removeExtraTag, "", x)
		x = re.sub('/', ";", x)
		x = re.sub(re.compile('\s{2,}'), ' ', x)
		x = re.sub(re.compile('[\n\r]'), ' ', x)
		return x.strip()
	
	def GetProxies(self):
		# 代理服务器
		proxyHost = "http-dyn.abuyun.com"
		proxyPort = "9020"
		# 代理隧道验证信息
		proxyUser = "HK847SP62Z59N54D"
		proxyPass = "C0604DD40C0DD358"
		proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
			"host": proxyHost,
			"port": proxyPort,
			"user": proxyUser,
			"pass": proxyPass,
		}
		proxies = {
			"http": proxyMeta,
			"https": proxyMeta,
		}
		return proxies
	
	def get_all_film_ids(self, product_url):
		film_ids = []
		retry = 5
		while 1:
			try:
				headers = self.get_headers()
				headers['host'] = 'www.iqiyi.com'
				text = requests.get(product_url, headers=headers, proxies=self.GetProxies(), timeout=10).text
				if '<ul class="album-numlist clearfix">' not in text:
					p00 = re.compile('<p class="site-piclist_info_describe">.*?<a href="(.*?)"', re.S)
					items = re.findall(p00, text)
					if len(items) > 0:
						urls = []
						urls.extend(items)
						print u'共有 %d 集' % len(urls)
						pool = Pool(8)
						items = pool.map(self.get_film_id, urls)
						pool.close()
						pool.join()
						mm = filter(lambda x: x is not None, items)
						film_ids.extend(mm)
						return film_ids
					else:
						p0 = re.compile('albumId: (\d+)')
						p1 = re.compile('playPageInfo\.sourceId = "(\d+?)"')
						try:
							film_id = re.findall(p0, text)[0]
							film_ids.append(film_id)
							return film_ids
						except:
							try:
								film_id = re.findall(p1, text)[0]
								film_ids.append(film_id)
								return film_ids
							except:
								return film_ids
				else:
					urls = []
					p = re.compile('<a title="\d+?" href="(.*?)" target="_blank" data-pb="(.*?)</li>', re.S)
					tmp = re.findall(p, text)
					for i in tmp:
						if 'yugao' not in i[-1]:
							urls.append(i[0])
					print u'共有 %d 集' % len(urls)
					pool = Pool(5)
					items = pool.map(self.get_film_id, urls)
					pool.close()
					pool.join()
					mm = filter(lambda x: x is not None, items)
					film_ids.extend(mm)
					return film_ids
			except Exception as e:
				retry -= 1
				if retry == 0:
					print e
					return film_ids
				else:
					continue
	
	def get_pagenums(self, film_id):  # 获取单个视频所有评论总页数
		url = "https://sns-comment.iqiyi.com/v3/comment/get_comments.action"
		querystring = {"agent_type": "118",
		               "agent_version": "9.0.0",
		               "authcookie": "null",
		               "business_type": "17",
		               "content_id": film_id,
		               "hot_size": "0",
		               "last_id": "",
		               "page": "1",
		               "page_size": "20",
		               "types": "time"}
		retry = 5
		while 1:
			try:
				text = requests.get(url, headers=self.get_headers(), proxies=self.GetProxies(), timeout=10,
				                    params=querystring).json()
				total = int(text['data']['totalCount'])
				# if total > self.limits:
				# 	total = self.limits
				if total % 20 == 0:
					pagenums = total / 20
				else:
					pagenums = total / 20 + 1
				return pagenums
			except:
				retry -= 1
				if retry == 0:
					return None
				else:
					continue
	
	def get_comments_page(self, ss):  # 获取某个视频某一页的评论
		film_id, page, product_url, product_number, plat_number = ss
		print 'page:',page
		url = "https://sns-comment.iqiyi.com/v3/comment/get_comments.action"
		querystring = {"agent_type": "118",
		               "agent_version": "9.0.0",
		               "authcookie": "null",
		               "business_type": "17",
		               "content_id": film_id,
		               "hot_size": "0",
		               "last_id": "",
		               "page": str(page),
		               "page_size": "20",
		               "types": "time"}
		retry = 5
		while 1:
			try:
				results = []
				text = requests.get(url, headers=self.get_headers(), proxies=self.GetProxies(), timeout=10,
				                    params=querystring).json()
				items = text['data']['comments']
				last_modify_date = self.p_time(time.time())
				for item in items:
					try:
						nick_name = item['userInfo']['uname']
					except:
						nick_name = ''
					try:
						tmp1 = self.p_time(item['addTime'])
						cmt_date = tmp1.split()[0]
						# if cmt_date < self.date:
						# 	continue
						cmt_time = tmp1
					except:
						cmt_date = ''
						cmt_time = ''
					try:
						comments = self.replace(item['content'])
					except:
						comments = ''
					try:
						like_cnt = str(item['likes'])
					except:
						like_cnt = '0'
					try:
						cmt_reply_cnt = str(item['replyCount'])
					except:
						cmt_reply_cnt = '0'
					long_comment = '0'
					source_url = product_url
					tmp = [product_number, plat_number, nick_name, cmt_date, cmt_time, comments, like_cnt,
					       cmt_reply_cnt, long_comment, last_modify_date, source_url]
					print '|'.join(tmp)
					results.append([x.encode('gbk', 'ignore') for x in tmp])
				return results
			except Exception as e:
				retry -= 1
				if retry == 0:
					print e
					return None
				else:
					continue
	
	def save_sql(self, table_name,items):  # 保存到sql
		all = len(items)
		print all
		results = []
		for i in items:
			try:
				t = [x.decode('gbk', 'ignore') for x in i]
				dict_item = {'product_number': t[0],
				             'plat_number': t[1],
				             'nick_name': t[2],
				             'cmt_date': t[3],
				             'cmt_time': t[4],
				             'comments': t[5],
				             'like_cnt': t[6],
				             'cmt_reply_cnt': t[7],
				             'long_comment': t[8],
				             'last_modify_date': t[9],
				             'src_url': t[10]}
				results.append(dict_item)
			except:
				continue
		for item in results:
			try:
				self.db.add(table_name, item)
			except:
				continue

	def get_comments_all(self, film_id, prouuct_url, product_number, plat_number):  # 获取某个视频的所有评论
		print 'film_id:',film_id
		pagenums = self.get_pagenums(film_id)
		if pagenums is None:
			return None
		else:
			print u'pagenums:%d' % pagenums
			ss = []
			for page in range(1, pagenums + 1):
				ss.append([film_id, page, prouuct_url, product_number, plat_number])
			pool = Pool(5)
			items = pool.map(self.get_comments_page, ss)
			pool.close()
			pool.join()
			mm = []
			for item in items:
				if item is not None:
					mm.extend(item)

			with open('data_comment.csv', 'a') as f:
				writer = csv.writer(f, lineterminator='\n')
				writer.writerows(mm)

			# self.save_sql('t_comments_pub', mm)  # 手动修改需要录入的库的名称
			# print u'%s 开始录入数据库' % product_number
			# self.save_sql('T_COMMENTS_PUB_MOVIE', mm)  # 手动修改需要录入的库的名称
			# print u'%s 录入数据库完毕' % product_number

	
	def get_comments(self, product_url, product_num, plat_number):  # 获取某个产品的所有评论
		film_ids = self.get_all_film_ids(product_url)
		for film_id in film_ids:
			self.get_comments_all(film_id, product_url, product_num, plat_number)
	
	def get_film_id(self, film_url):  # 获取film_id
		retry = 5
		while 1:
			try:
				if 'http' not in film_url:
					film_url = 'https:' + film_url
				headers = self.get_headers()
				headers['host'] = 'www.iqiyi.com'
				text = requests.get(film_url, headers=headers, proxies=self.GetProxies(), timeout=10).text
				p0 = re.compile('param\[\'tvid\'\] = "(\d+?)"')
				film_id = re.findall(p0, text)[0]
				return film_id
			except:
				retry -= 1
				if retry == 0:
					return None
				else:
					continue


if __name__ == "__main__":
	spider = Spider()
	s = []
	with open('new_data.csv') as f:
		tmp = csv.reader(f)
		for i in tmp:
			if 'http' in i[2]:
				s.append([i[2], i[0], 'P01'])
	for j in s:
		print j[1],j[0]
		spider.get_comments(j[0], j[1], j[2])
	spider.db.db.close()
