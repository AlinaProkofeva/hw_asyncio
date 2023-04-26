import asyncio

import aiohttp
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy import JSON, Column, Integer
import os
from dotenv import load_dotenv


load_dotenv()

USER = os.getenv('DB_USER')
PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')
DB_PORT = os.getenv('DB_PORT')
DSN = f'postgresql+asyncpg://{USER}:{PASSWORD}@localhost:{DB_PORT}/{DB_NAME}'


engine = create_async_engine(DSN)
Session = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

Base = declarative_base()


class SwapiPeople(Base):

    __tablename__ = 'swapi_people'

    id = Column(Integer, primary_key=True, autoincrement=True)
    json = Column(JSON)


async def get_download_links_to_str(links_list, client_session, attribute):
    '''Подгрузка данных из ссылок в основной словарь персонажа'''
    coros = [client_session.get(link) for link in links_list]
    responses = await asyncio.gather(*coros)

    json_coros = [response.json() for response in responses]
    inner_json = await asyncio.gather(*json_coros)

    needed_info = [item[attribute] for item in inner_json]
    return ', '.join(needed_info)


async def get_people_from_swapi(people_id, client_session):
    '''Формирование основного словаря персонажа с догруженными по ссылкам данными'''
    response = await client_session.get(f'https://swapi.dev/api/people/{people_id}')
    json_data = await response.json()

    if response.status == 200:

        json_data['id'] = people_id  # добавляем атрибут id
        del(json_data['created'])  # удаляем ненужные атрибуты
        del(json_data['edited'])
        del(json_data['url'])

        films_list = json_data.get('films', '')  # формируем списки нужных линков для подгрузки
        starships_list = json_data.get('starships', '')
        vehicles_list = json_data.get('vehicles', '')
        species_list = json_data.get('species', '')
        homeworld_list = list()
        homeworld_list.append(json_data.get('homeworld', ''))

        films_coro = get_download_links_to_str(films_list, client_session, 'title')  # формируем корутины подгрузок
        starships_coro = get_download_links_to_str(starships_list, client_session, 'name')
        vehicles_coro = get_download_links_to_str(vehicles_list, client_session, 'name')
        species_coro = get_download_links_to_str(species_list, client_session, 'name')
        homeworld_coro = get_download_links_to_str(homeworld_list, client_session, 'name')

        fields = await asyncio.gather(films_coro, starships_coro, vehicles_coro, species_coro, homeworld_coro)

        films, starships, vehicles, species, homeworld = fields

        json_data['films'] = films
        json_data['starships'] = starships
        json_data['vehicles'] = vehicles
        json_data['species'] = species
        json_data['homeworld'] = homeworld

        return json_data
    return False


async def paste_to_db(peoples_list):
    '''Запись списка объектов в БД'''
    async with Session() as session:
        orm_objects = [SwapiPeople(json=people) for people in peoples_list]
        session.add_all(orm_objects)
        print(f'*** записаны персонажи id {peoples_list[0]["id"]} - {peoples_list[-1]["id"]}')
        await session.commit()


async def main():

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with aiohttp.ClientSession() as client_session:
        start_id = 1
        step = 10

        while True:
            coros = [get_people_from_swapi(id, client_session) for id in range(start_id, start_id + step)]
            res = await asyncio.gather(*coros)

            if False not in res or (res.count(False) < step):  # проверяем, что персы не закончились
                peoples_list = [people for people in res if people]  # чистим список от удаленных и закончившихся
                asyncio.create_task(paste_to_db(peoples_list))
                start_id += step

            else:
                break

    all_tasks = asyncio.all_tasks()
    all_tasks = all_tasks - {asyncio.current_task()}
    await asyncio.gather(*all_tasks)


if __name__ == '__main__':
    asyncio.run(main())
