from scrapy.selector import Selector
from scrapy import Spider, Request
from car_company.items import BikeCompany
from car_company.spiders import PostgresUtils
import logging
from datetime import datetime


class BikeCompanySpider(Spider):
    """
    Class to Extract the Bike's Company Name and Bike's Models
    """
    root_api = 'https://www.bikewale.com'
    name = 'ExtractBike'
    allow_domains = ['bikewale.com']
    start_urls = [root_api]

    def __init__(self):
        self.model_bike_wise = dict()
        self.bike_list = list()
        self.bike_model_list = dict()
        self.bike_mode_obj = None
        self.get_bike_mode()

    def bike_company_name(self, response):
        s1 = Selector(response)
        bike = s1.xpath('//div[@id="brand-type-container"]/ul/li/a')
        logging.info("Extracting Bike Company Names [GET] [HTML]: {}".format(response.url))
        for name, data in zip(bike.xpath('@href'), bike.xpath('./span[@class="brand-type-title"]/text()')):
            self.bike_list.append(data.extract())
            self.bike_model_list[data.extract().__str__()] = name.extract().__str__()
        logging.info("Done")
        self.insert_bike_company()
        return True

    def bike_model_name(self, response):
        logging.info("Extracting Bike Model Names [GET] [HTML]: {}".format(response.meta['company_name']))
        s2 = Selector(response)
        model = s2.xpath('//section[@id="bikeMakeList"]/div[contains(@class,'
                         ' "inner-content-card")]/ul/li/@data-bike').extract()
        self.model_bike_wise[response.meta['company_name']] = model
        logging.info("Done")
        return True

    def parse(self, response):
        self.bike_company_name(response)
        for key, value in self.bike_model_list.items():
            url = BikeCompanySpider.root_api + value
            yield Request(url, self.parse2, meta={"company_name": key})

    def parse2(self, response):
        item = BikeCompany()
        self.bike_model_name(response)
        self.insert_bike_model()
        return item

    def insert_bike_company(self):
        cursor, connection = PostgresUtils().conn()
        for i in self.bike_list:
            logging.info("Inserting {}".format(i.title()))
            postgres_insert_query = """select * from master_vehiclecompany where name=%s and mode_id=%s"""
            cursor.execute(postgres_insert_query, (i.title(), self.bike_mode_obj[0],))
            if not cursor.fetchone():
                postgres_insert_query = """insert into master_vehiclecompany (name, mode_id, created_timestamp,
                 updated_timestamp) values (%s, %s, %s, %s)"""
                cursor.execute(postgres_insert_query, (i.title(), self.bike_mode_obj[0], datetime.now(),
                                                       datetime.now(),))
                connection.commit()
            logging.info("Done")
        PostgresUtils().close_conn()
        return True

    def insert_bike_model(self):
        cursor, connection = PostgresUtils().conn()
        for k,v in self.model_bike_wise.items():
            logging.info("Inserting Models of {}".format(k.title()))
            postgres_insert_query = """select * from master_vehiclecompany where name=%s and mode_id=%s"""
            cursor.execute(postgres_insert_query, (k.title(), self.bike_mode_obj[0],))
            company_obj = cursor.fetchone()
            if company_obj:
                for i in v:
                    postgres_insert_query = """select * from master_vehiclemodel where name=%s and company_id=%s and
                     mode_id=%s"""
                    cursor.execute(postgres_insert_query, (i.title(), company_obj[0],  self.bike_mode_obj[0],))
                    if cursor.rowcount < 1:
                        postgres_insert_query = """insert into master_vehiclemodel (name, company_id, mode_id,
                         created_timestamp, updated_timestamp) values (%s, %s, %s, %s, %s)"""
                        cursor.execute(postgres_insert_query, (i.title(), company_obj[0],  self.bike_mode_obj[0],
                                                               datetime.now(), datetime.now(),))
                        connection.commit()
                logging.info("Done")
            else:
                logging.info("Not present {}".format(k.title()))
        PostgresUtils().close_conn()
        return True

    def get_bike_mode(self):
        cursor, connection = PostgresUtils().conn()
        postgres_insert_query = """select * from master_vehiclemode where name=%s"""
        cursor.execute(postgres_insert_query, ('bike'.title(),))
        self.bike_mode_obj = cursor.fetchone()
        return True
