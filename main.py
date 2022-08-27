import sqlalchemy as sq
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
import os
import json

Base = declarative_base()


class Publisher(Base):
    __tablename__ = "publisher"

    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String(length=40), nullable=False)


class Book(Base):
    __tablename__ = "book"

    id = sq.Column(sq.Integer, primary_key=True)
    title = sq.Column(sq.String(length=60), nullable=False)
    id_publisher = sq.Column(sq.Integer, sq.ForeignKey("publisher.id"), nullable=False)
    publisher = relationship(Publisher, backref="book")


class Shop(Base):
    __tablename__ = "shop"

    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String(length=40), nullable=False)


class Stock(Base):
    __tablename__ = "stock"

    id = sq.Column(sq.Integer, primary_key=True)
    id_book = sq.Column(sq.Integer, sq.ForeignKey("book.id"), nullable=False)
    id_shop = sq.Column(sq.Integer, sq.ForeignKey("shop.id"), nullable=False)
    count = sq.Column(sq.Integer, nullable=False)
    book = relationship(Book, backref="stock")
    shop = relationship(Shop, backref="stock")


class Sale(Base):
    __tablename__ = "sale"

    id = sq.Column(sq.Integer, primary_key=True)
    price = sq.Column(sq.Float, nullable=False)
    data_sale = sq.Column(sq.DateTime, nullable=False)
    id_stock = sq.Column(sq.Integer, sq.ForeignKey("stock.id"), nullable=False)
    count = sq.Column(sq.Integer, nullable=False)
    stock = relationship(Stock, backref="sale")


def create_tables(engine):
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

if __name__ == '__main__':
    # Параметры подключения к DB
    type_db ='postgresql'
    login = 'postgres'
    password = 'NetoSQL2022tvp'
    type_host = 'localhost'
    port = '5432'
    name_db = 'alchemy_db'
    DSN = type_db + '://' + login + ':' + password +'@' + type_host + ':' + port + '/' + name_db

    #engine = sq.create_engine(DSN, echo=True)
    engine = sq.create_engine(DSN)
    create_tables(engine)
    print('База данных в PostgreSQL создана, таблицы созданы')

    # Сессия
    Session = sessionmaker(bind=engine)
    session = Session()

    # Считаем данные из json файла
    path_directory = os.getcwd()
    path_file = path_directory + '\\fixtures\\tests_data.json'
    with open(path_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Заполняем BD данными
    for row in data:
        for key, value in row.items():
            if key == 'model':
                model = value
            if key == 'pk':
                pk = value
            if key == 'fields':
                fields = value
        if model == 'publisher':
            #row_table = Publisher(id=pk, name=[*fields.values()][0])
            row_table = Publisher(id=pk, name=list(fields.values())[0])
            session.add(row_table)
        if model == 'book':
            row_table = Book(id=pk,
                             title=list(fields.values())[0],
                             id_publisher=list(fields.values())[1])
            session.add(row_table)
        if model == 'shop':
            row_table = Shop(id=pk,
                             name=list(fields.values())[0])
            session.add(row_table)
        if model == 'stock':
            row_table = Stock(id=pk,
                              id_shop=list(fields.values())[0],
                              id_book=list(fields.values())[1],
                              count=list(fields.values())[2] )
            session.add(row_table)
        if model == 'sale':
            row_table = Sale(id=pk,
                             price=list(fields.values())[0],
                             data_sale=list(fields.values())[1],
                             count=list(fields.values())[2],
                             id_stock=list(fields.values())[3])
            session.add(row_table)
    session.commit()
    print('База данных заполнена тестовыми данными из файла test_data.json')

    # Запрашиваем id издателя
    publisher_id = int(input('Введите id издателя: '))
    # Проверка на наличие издателя в таблице publisher
    pubs_ids = []
    for a in session.query(Publisher.id).all():
        pubs_ids.append(a[0])
    if publisher_id not in pubs_ids:
        print(f'Издателя с id={publisher_id} не существует')
    #  Объединяем таблицы, фильтруем по запрошеному издателю и наличию продаж издателя,
    #  выводим наименование издателя и магазин
    else:
        tab = session.query(Publisher.name, Shop.name).join(Book).join(Stock).join(Shop).join(Sale).filter(
            Publisher.id == publisher_id,
            Sale.count != 0)
        for publisher_name, shop_name in tab.all():
            print(f'Издатель "{publisher_name}" продается в магазине "{shop_name}"')