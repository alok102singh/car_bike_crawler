# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

from scrapy import Item, Field


class CarCompany(Item):
    # define the fields for your item here like:
    car = Field()


class BikeCompany(Item):
    # define the fields for your item here like:
    bike = Field()
    link = Field()
    model = Field()
