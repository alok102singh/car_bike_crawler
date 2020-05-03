from scrapy import Spider
from car_company.items import CarCompany
from car_company.spiders import PostgresUtils
from datetime import datetime
import json
import logging


class CarCompanySpider(Spider):
    """
    Class to Extract the Car's Company Name and Car's Models
    """

    name = 'ExtractCar'
    allow_domains = ['autoportal.com']
    start_urls = ['https://autoportal.com']

    def __init__(self):
        self.model_car_wise = dict()
        self.car_list = list()
        self.car_model_list = dict()
        self.car_mode_obj = None
        self.get_car_mode()

    def car_company_name(self, response):
        logging.info("Extracting Car Company Names [GET] [HTML]: {}".format(response.url))
        car = response.selector.xpath('//select[@name="brand"]/@data-render-select').extract()
        for i in car:
            data = json.loads(i)
            for k in data['data']:
                if k['attrs']['value']:
                    car_name = k['text']
                    self.car_list.append(car_name)
                    self.car_model_list[(k['attrs']['value'])] = k['text']
        logging.info("Done")
        self.insert_car_company()
        return True

    def car_model_name(self, response):
        logging.info("Extracting Car Model Names [GET] [HTML]: {}".format(response.url))
        for key, value in self.car_model_list.items():
            self.model_car_wise[value] = list()
        model = response.selector.xpath('//select[@name="model"]/@data-render-select').extract()
        for i in model:
            data = json.loads(i)
            for k in data['data']:
                if 'type' in k:
                    data1 = k['children']
                    for q in data1:
                        if q['attrs']['value']:
                            print self.car_model_list[q['attrs']['data-brand']]
                            self.model_car_wise[self.car_model_list[q['attrs']['data-brand']]].append(q['text'])
                else:
                    if k['attrs']['value']:
                        print self.car_model_list[k['attrs']['data-brand']]
                        self.model_car_wise[self.car_model_list[k['attrs']['data-brand']]].append(k['text'])
        logging.info("Done")
        self.insert_car_model()
        return True

    def parse(self, response):
        item = CarCompany()
        self.car_company_name(response)
        self.car_model_name(response)
        return item

    def insert_car_company(self):
        cursor, connection = PostgresUtils().conn()
        for i in self.car_list:
            logging.info("Inserting {}".format(i.title()))
            postgres_insert_query = """select * from master_vehiclecompany where name=%s and mode_id=%s"""
            cursor.execute(postgres_insert_query, (i.title(), self.car_mode_obj[0],))
            if not cursor.fetchone():
                postgres_insert_query = """insert into master_vehiclecompany (name, mode_id, created_timestamp,
                 updated_timestamp) values (%s, %s, %s, %s)"""
                cursor.execute(postgres_insert_query, (i.title(), self.car_mode_obj[0], datetime.now(),
                                                       datetime.now(),))
                connection.commit()
            logging.info("Done")
        PostgresUtils().close_conn()
        return True

    def insert_car_model(self):
        cursor, connection = PostgresUtils().conn()
        for k, v in self.model_car_wise.items():
            logging.info("Inserting Models of {}".format(k.title()))
            postgres_insert_query = """select * from master_vehiclecompany where name=%s and mode_id=%s"""
            cursor.execute(postgres_insert_query, (k.title(), self.car_mode_obj[0],))
            company_obj = cursor.fetchone()
            if company_obj:
                for i in v:
                    postgres_insert_query = """select * from master_vehiclemodel where name=%s and company_id=%s and 
                    mode_id=%s"""
                    cursor.execute(postgres_insert_query, (i.title(), company_obj[0], self.car_mode_obj[0],))

                    if cursor.rowcount < 1:
                        postgres_insert_query = """insert into master_vehiclemodel (name, company_id, mode_id,
                         created_timestamp, updated_timestamp) values (%s, %s, %s, %s, %s)"""
                        cursor.execute(postgres_insert_query, (i.title(), company_obj[0], self.car_mode_obj[0],
                                                               datetime.now(), datetime.now(),))
                        connection.commit()
                logging.info("Done")
            else:
                logging.info("Not present {}".format(k.title()))
        PostgresUtils().close_conn()
        return True

    def get_car_mode(self):
        cursor, connection = PostgresUtils().conn()
        postgres_insert_query = """select * from master_vehiclemode where name=%s"""
        cursor.execute(postgres_insert_query, ('car'.title(),))
        self.car_mode_obj = cursor.fetchone()
        return True
