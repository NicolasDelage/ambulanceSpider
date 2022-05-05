import scrapy
import re
import csv
from bs4 import BeautifulSoup


class AmbulanceCompanyNamesSpider(scrapy.Spider):
    name = 'ambulance'

    def __init__(self):
        self.filename = 'ambulance.csv'
        header = ['company_name', 'website_url', 'email', 'phone']
        with open(self.filename, 'w', newline='') as f:
            writer = csv.writer(f, delimiter=';')
            writer.writerow(header)

    def start_requests(self):
        url = 'https://www.infogreffe.fr/entreprises-francaises-par-departement.html'
        yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        urls = response.xpath('//div[@class="listeEntreprise"]/div/h3/div/a/@href').getall()

        for url in urls:
            print('url', url)
            first_url_part = url.split('/')[1]

            second_url_part = url.split('/')[-1]
            department_name = second_url_part.split('-')
            department_code = department_name.pop(-1)
            identity_numbers = '8690A-' + re.sub("[^0-9]", "", department_code) + '-1'
            department_name = '-'.join(department_name)

            final_url = first_url_part + '/' + department_name + '-' + identity_numbers + '.html'

            yield scrapy.Request(url='https://www.infogreffe.fr/' + final_url, callback=self.parse_company_name)

    def parse_company_name(self, response):
        urls = response.xpath('//div[@class="listeEntreprise"]/div/h2/a/@href').getall()

        for url in urls:
            company_name = re.sub(r'[0-9]', '', url.split('/')[-1]).split('-')
            company_name.pop(-1)
            company_name = ' '.join(company_name).rstrip().lstrip()

            yield scrapy.Request(url='http://www.google.com/search?q=' + company_name.replace(' ', '+'),
                                 callback=self.parse_website_url,
                                 meta={'company_name': company_name})

    def parse_website_url(self, response):
        print('PARSE WEBSITE URL')
        soup = BeautifulSoup(response.body, 'html.parser')
        div = soup.find('div', text='Site Web')
        if div:
            url = div.parent['href']
            website_name = url.replace('/url?q=', '').split('/')[2].replace('www.', '')

            yield scrapy.Request(url='http://www.google.com/search?q=%40' + website_name + '+contact',
                                 callback=self.parse_email,
                                 meta={'company_name': response.meta['company_name'], 'website_url': 'https://www.' +
                                                                                                     website_name})

    def parse_email(self, response):
        print('PARSE EMAIL')
        soup = BeautifulSoup(response.body, 'html.parser')
        match_email = None
        match_phone = None
        for text in soup.findAll(text=True):
            if match_email is None:
                match_email = re.search(r'[\w.+-]+@[\w-]+\.[\w.-]+', text)
            if match_phone is None:
                match_phone = re.search(r'[\+\(]?[0-9][0-9 .\-\(\)]{8,}[0-9]', text)

        with open(self.filename, 'a+') as f:
            writer = csv.writer(f, delimiter=';')
            writer.writerow([response.meta['company_name'], response.meta['website_url'],
                             match_email.group(0) if match_email else '', match_phone.group(0) if match_phone else ''])
