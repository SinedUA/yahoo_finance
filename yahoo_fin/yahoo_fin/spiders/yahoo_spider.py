# -*- coding: utf-8 -*-
import scrapy
import time
import csv
import requests
import pandas as pd
from datetime import date, timedelta
import datetime



class YahooSpiderSpider(scrapy.Spider):
	name = 'yahoo_spider'
	start_urls = ['https://finance.yahoo.com']

	def parse(self, response):
		company_lst = ['PD', 'ZUO', 'PINS', 'ZM', 'PVTL', 'DOCU', 'CLDR', 'RUN']
		for company in company_lst:

			# We can do like in your test task (step by step), but it's not necessary in this case
			# Also, it seems that the download link is dynamically built in one of their hosts
			# However, the download link is static, so we used it, just changed few veriable 
			
			period1 = '-14810142313' # from 1500 year :)
			period2 = int(time.time()) # now
			file_url='https://query1.finance.yahoo.com/v7/finance/download/{}?period1={}&period2={}&interval=1d&events=history'.format(company, period1, period2)
			resp = requests.get(file_url)
			if resp.status_code == 200:
				df = pd.read_csv(file_url)
				df.to_csv('{}_original_file.csv'.format(company), index = False) # if we need original file too
				df['Date'] = pd.to_datetime(df.Date)
				df = df.sort_values(by='Date',ascending=False)

				row_list = []
				start = 0
				fin = 4
				while start != len(df.index):
					part_df = df[start:fin]
					full_date = part_df.iloc[0]['Date']
					date_row = datetime.date(full_date.year, full_date.month, full_date.day)
					three_days_before = date_row - timedelta(days=3)
					try:
						index_three_days_before = part_df.Date[part_df.Date == str(three_days_before)].index.tolist()[0]
						three_days_before_close = part_df['Close'][index_three_days_before]
						main_day_close = part_df.iloc[0]['Close']
						change = main_day_close/three_days_before_close
					except:
						change = '-'

					main_day_index = part_df.Date[part_df.Date == str(date_row)].index.tolist()[0]
					part_df.loc[main_day_index, '3day_before_change'] = change
					start +=1
					fin +=1
					updated_row = part_df.loc[[main_day_index]]
					row_list.append(updated_row.values.tolist()[0])

				# new(updated) csv file
				df2 = pd.DataFrame(row_list, columns = ['Date' , 'Open', 'High' , 'Low', 'Close', 'Adj Close', 'Volume', '3day_before_change'])
				df2.to_csv('{}.csv'.format(company), mode='a', header=True, index = False)

				# “Summary” tab
				yield scrapy.Request(
				url='https://finance.yahoo.com/quote/{}?p={}'.format(company, company),
				meta={
					'company': company
				},
				headers={
					'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36',
				},
				callback=self.parse_news)

		
	def parse_news(self, response):
		latest_news_lst = []
		company = response.meta.get('company')
		latest_news_urls_lst = response.xpath('//*[@class="Mb(5px)"]/a/@href').extract()
		for link in latest_news_urls_lst:
			latest_news = {}
			latest_news['link'] = response.urljoin(link)
			latest_news['title'] = response.xpath('//*[contains(@href, "{}")]/text()'.format(link)).extract_first()
			latest_news_lst.append(latest_news)

		columns = ['link','title']
		news_file = '{}_latest_news.csv'.format(company)

		# Also we can get all news from uploaded/source page json file 
		# to csv
		try:
			with open(news_file, 'w') as csvfile:
				writer = csv.DictWriter(csvfile, fieldnames=columns)
				writer.writeheader()
				for data in latest_news_lst:
					writer.writerow(data)
		except IOError:
			print("I/O error")