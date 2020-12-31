import requests
import bs4
from urllib.parse import urljoin
import pymongo
import datetime

url = "https://magnit.ru/promo/?geo=moskva"

response = requests.get(url)

soup = bs4.BeautifulSoup(response.text, "lxml")


class MagnitParse:
    def __init__(self, start_url, mongo_db):
        self.start_url = start_url
        self.db = mongo_db


    def run(self):
        for product in self.parse():
            self.save(product)

    def __get_soup(self, url) -> bs4.BeautifulSoup:
        response = requests.get(url)
        return bs4.BeautifulSoup(response.text, "lxml")

    def parse(self):
        soup = self.__get_soup(self.start_url)
        catalog_main = soup.find("div", attrs = {"class":"сatalogue__main"})
        for product_tag in catalog_main.find_all("a", recursive=False):
            try:
                yield self.product_parse(product_tag)
            except AttributeError:
                pass

    def product_parse(self, product: bs4.Tag)->dict:
        try:
            old_price = float(product.find("div", class_="label__price label__price_old").get_text().strip().replace("\n", "."))
        except AttributeError:
            old_price = 0
        try:
            new_price = float(product.find("div", class_="label__price label__price_new").get_text().strip().replace("\n", "."))
        except ValueError or AttributeError:
            new_price = 0

        date_from = product.find("div", class_ = "card-sale__date").get_text().strip().split("\n")[0]
        date_to = product.find("div", class_ = "card-sale__date").get_text().strip().split("\n")[1]
        month = {
            "января": 1,
            "февраля": 2,
            "марта": 3,
            "апреля": 4,
            "мая": 5,
            "июня": 6,
            "июля": 7,
            "августа": 8,
            "сентября": 9,
            "октября": 10,
            "ноября": 11,
            "декабря": 12
        }
        date1 = datetime.date(
            year = 2020 if int(month[date_from.split()[2]])==12 else 2021,
            month = int(month[date_from.split()[2]]),
            day = int(date_from.split()[1]))

        date2 = datetime.date(
            year=2020 if int(month[date_to.split()[2]])==12 else 2021,
            month=int(month[date_to.split()[2]]),
            day=int(date_to.split()[1]))


        product = {
            "url": urljoin(self.start_url, product.get("href")),
            "promo_name": product.find("div", attrs={"class":"card-sale__header"}).text,
            "product_name": product.find("div", attrs={"class":"card-sale__title"}).text,
            "old_price": old_price,
            "new_price": new_price,
            "image_url": urljoin(self.start_url, product.find("source")["data-srcset"]),
            "date_from": date1.strftime("%d/%m/%Y"),
            "date_to": date2.strftime("%d/%m/%Y"),
        }
        return product

    def save(self, data):
        collection = self.db["magnit"]
        collection.insert_one(data)
        print(1)


if __name__ == "__main__":
    database = pymongo.MongoClient("mongodb://localhost:27017")["db_magnitparse"]
    parser = MagnitParse("https://magnit.ru/promo/?geo=moskva", database)
    parser.run()