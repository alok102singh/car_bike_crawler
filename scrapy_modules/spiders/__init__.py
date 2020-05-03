# This package will contain the spiders of your Scrapy project
#
# Please refer to the documentation for information on how to create and manage
# your spiders.
import psycopg2
import logging


class PostgresUtils(object):

    def __init__(self):
        self.connection = None
        self.cursor = None

    def conn(self):
        try:
            self.connection = psycopg2.connect(user="DATABASE_USER",
                                               password="DATABASE_PASSWORD",
                                               host="DATABASE_HOST",
                                               port="5432",
                                               database="DATABASE_NAME")
            self.cursor = self.connection.cursor()
            logging.info('PostgreSQL: Connection Open')
            return self.cursor, self.connection
        except (Exception, psycopg2.Error) as error:
            if self.connection:
                logging.info("PostgreSQL: Failed to create connection.", error)
            return False

    def close_conn(self):
        if self.connection:
            self.cursor.close()
            self.connection.close()
            logging.info('PostgreSQL: Connection Closed')
        return True
