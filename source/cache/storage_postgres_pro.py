from pprint import pprint
import asyncio
import os
from time import time
from typing import ContextManager, Union
from pydantic import BaseModel
from psycopg2 import OperationalError
from psycopg2._psycopg import connection
from fastapi.encoders import jsonable_encoder
from psycopg2.pool import SimpleConnectionPool
from psycopg2.extras import RealDictCursor, execute_batch
from psycopg2.extensions import cursor
import json
from contextlib import contextmanager, asynccontextmanager
from aiopg import create_pool, Connection, Pool
from sshtunnel import SSHTunnelForwarder

try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal

# Локальный импорт:
import sys
from pathlib import Path

__root__ = Path(__file__).absolute().parent.parent
sys.path.append(__root__.__str__())
from cache import CacheStorage
from models import DBModel


# ~Локальный импорт


class CacheStoragePostgresPro(CacheStorage):
    """Обертка для L1-кеша на базе расширения «in memory» Postgres Pro"""

    CHUNK_SIZE = 128

    def __init__(self):

        super().__init__()

        self.__tasks = set()

        # Эта проверка нужна потому, что вызов «cache = CacheStorage()» (без get_instance) дернет __init__,
        # если наличие атрибута не проверять, то каждый вызов CacheStorage() — super().__init__() — обнулит кэш.
        if not hasattr(self, "_CacheStoragePostgresPro__connection_pool_created"):

            self.__connection_pool_created = True

            # key = "postgres_pro_cache"
            # db = self.config.postgres_pro_cache.database
            # host = self.config.postgres_pro_cache.host

            ssh = self.config.postgres_pro_cache.get('ssh', None)

            if ssh:
                self.__tunnel = SSHTunnelForwarder((ssh['host'], ssh['port']),
                                                   ssh_username=ssh['user'],
                                                   ssh_password=ssh['password'],
                                                   remote_bind_address=(
                                                   ssh['remote_bind_host'], ssh['remote_bind_port']),
                                                   local_bind_address=(ssh['local_bind_host'], ssh['local_bind_port'])
                                                   )
                self.__tunnel.start()

            # self.__postgres_pro_pool = SimpleConnectionPool(
            #     minconn=1,
            #     maxconn=32,
            #     dsn=DBModel(**self.config.postgres_pro_cache).dsn
            # )
            # self.logger.info(f'Sync CACHE connection pool to key "{key}" - {db}@{host} - created!')

            # Уносим все это в lifespan, слишком большой оверхед по времени до фактического запуска получается:
            # loop = asyncio.get_event_loop()
            # # На всякий случай держим таску:
            # async_pool_init_task = loop.create_task(self.__ainit__())
            # self.__tasks.add(async_pool_init_task)
            # async_pool_init_task.add_done_callback(self.__tasks.discard)

    async def __ainit__(self):
        key = "postgres_pro_cache"
        db = self.config.postgres_pro_cache.database
        host = self.config.postgres_pro_cache.host

        async def on_connect(conn: Connection):
            self.logger.info(f'New async CACHE connection to {db}@{host} established...')

        self.__async_postgres_pro_pool: Pool = await create_pool(
            DBModel(**self.config.postgres_pro_cache).dsn,
            minsize=4,
            maxsize=32,
            pool_recycle=600,
            on_connect=on_connect
        )
        self.logger.info(f'Async CACHE connection pool to key "{key}" - {db}@{host} - created!')

    # @contextmanager
    # def __get_cursor(self, autocommit: bool = False) -> ContextManager[cursor]:
    #     conn: connection = self.__postgres_pro_pool.getconn()
    #
    #     if autocommit:
    #         conn.set_session(autocommit=True)
    #     try:
    #         yield conn.cursor(cursor_factory=RealDictCursor)
    #         conn.commit()
    #     except Exception as e:
    #         # conn.rollback()
    #         raise e
    #     finally:
    #         self.__postgres_pro_pool.putconn(conn, close=True)

    async def __adel__(self):
        await self.__async_postgres_pro_pool.clear()
        self.__async_postgres_pro_pool.terminate()
        await self.__async_postgres_pro_pool.wait_closed()

        # self.__postgres_pro_pool.closeall()

        key = "postgres_pro_cache"
        db = self.config.postgres_pro_cache.database
        host = self.config.postgres_pro_cache.host
        self.logger.info(f'CACHE connection pool to key "{key}" - {db}@{host} - closed.')

    def __contains__(self, item):
        return super().__contains__(item)

    async def __get_from_level_1(self, key, update_hits=False):
        """
        Получаем данные из хранилища «in_memory» Postgres Pro для локального обновления данных.

        :param key:

        :param update_hits: пока не используется, задумывалось для обновления хитов в случае, есл L1-значение
        есть, а локального нет

        :return:
        """

        async with self.__async_postgres_pro_pool.acquire() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            # print(f'__get_from_level_1: key "{key}" not in L0 storage, search in Cache L1 (Postgres Pro) for PID {os.getpid()}...')
            _SEL = """-- HL API get key "%(key)s":
                select 
                    array_to_string(array_agg(value order by chunk), '') as value, 
                    max(hits) as hits, 
                    max(uptime) as uptime
                from cache where key_hash = hashtextextended(%(key)s, 1)
            """
            await cur.execute(_SEL, {'key': key})
            data = await cur.fetchone()

        if data and data['value']:
            value = json.loads(data['value'])
            hits = data['hits']
            uptime = data['uptime']
            self.__setitem__(key, value, outer_hits=hits, outer_uptime=uptime)
            # print(f'__get_from_level_1: key "{key}" FOUND IN L1 for PID {os.getpid()}')
            return value

        # # print(f'__get_from_level_1: key "{key}" NOT FOUND IN L1 for PID {os.getpid()}')

        return None

    def __getitem__(self, key):
        """ВНИМАНИЕ! Здесь мы НЕ БРОСАЕМ ИСКЛЮЧЕНИЕ ПРИ ОТСУТСТВИИ ЭЛЕМЕНТА, а отдаем None"""
        ret = super().__getitem__(key)

        # TODO:
        #  Технически, от этого защищает uptime, но тогда получается, что вызов __get_from_level_1 тут вообще не нужен?
        if ret is None:
            # На всякий случай выбросим исключение, если мы тут можем все же просквозить через uptime:
            raise Exception(f'Empty value for key "key" in {self.__class__.__name__}.__getitem__')
        #     # print(f'__getitem__ in L1 needs to load data...')
        #     Вот тут потенциально могли отдать исключение как JSON вместо проброса, поэтому такая сложная схема.
        #     # __get_from_level_1 положит значение в кеш, использовать return value нет необходимости.
        #     _ = self.__get_from_level_1(key, update_hits=True)
        #     # Вот сейчас значение точно есть, а если это исключение, то super().__getitem__ его выбросит.
        #     ret = super().__getitem__(key)

        return ret

    def __setitem__(self, key, value, outer_hits: int = 0, outer_uptime: Union[Literal[False], float] = False):
        """

        :param key:

        :param value:

        :param outer_uptime: Может НЕ быть False в случае, когда мы через __getitem__ подгружаем значение из кеша и
        хотим сразу его сохранить в L0.

        :param outer_hits:

        :return:
        """

        new_uptime = outer_uptime or int(time())

        super().__setitem__(key, value, outer_hits, new_uptime)

        # Если это значение пришло НЕ из L1-хранилища (outer_uptime пустой), то пихаем в туда:
        if outer_uptime is False:
            loop = asyncio.get_event_loop()
            # На всякий случай держим таску:
            asetitem_task = loop.create_task(self.__asetitem__(key, value, outer_hits))
            self.__tasks.add(asetitem_task)
            asetitem_task.add_done_callback(self.__tasks.discard)

            # НИЧЕГО не приводим к строкам «насильно» (default=str закомментирован), чтобы не пропустить расхождение
            # типов данных из L1-кеша и локального L0: лучше пусть пизданется сразу, чем неведомо когда потом.
            #
            # Чтобы предотвратить падение при сохранении в Tarantool, все запросы (к базе, например), потенциально
            # возвращающие сложные объекты (дата-время), должны оборачиваться либо в модели, либо в jsonable_encoder.

            # if isinstance(value, BaseModel):
            #     value = jsonable_encoder(value)
            #
            # if isinstance(value, str):
            #     value_str = value
            # else:
            #     value_str = json.dumps(value, ensure_ascii=False, sort_keys=True)  #, default=str)
            #
            # with self.__get_cursor() as cursor_:
            #     # print(f'__setitem__: SYNC SET item to L1 (Postgres Pro) on key "{key}"')
            #     _INS = """
            #         insert into cache(key, chunk, value, hits, uptime) VALUES (%s, %s, %s, %s, %s)
            #     """
            #     try:
            #         cursor_.execute('DELETE from cache where key_hash = hashtextextended(%(key)s, 1)', {'key': key})
            #         execute_batch(cursor_, _INS, self.__get_chunks(key, value_str, outer_hits), page_size=5_000)
            #     except Exception as err:
            #         self.logger.error(f'ERROR ON STORE TO L1: {err}')

    async def __asetitem__(self, key, value, outer_hits):
        """
        Асинхронная часть __setitem__.
        При использовании из-за того, что мы не можем официально дождаться завершения таски «положи в базу», возникают
        коллизии, когда несколько запросов просто висит.
        """

        if isinstance(value, BaseModel):
            value = jsonable_encoder(value)

        if isinstance(value, str):
            value_str = value
        else:
            value_str = json.dumps(value, ensure_ascii=False, sort_keys=True)  # , default=str)
        # print(f'__setitem__: TRY-1 ASYNC SET item to L1 (Postgres Pro) on key "{key}"')
        async with self.__async_postgres_pro_pool.acquire() as c, c.cursor(cursor_factory=RealDictCursor) as cur:
            # print(f'__setitem__: TRY-2 ASYNC SET item to L1 (Postgres Pro) on key "{key}"')
            try:
                await cur.execute('DELETE from cache where key_hash = hashtextextended(%(key)s, 1)', {'key': key})

                placeholders = ['(hashtextextended(%s, 1), %s, %s, %s, %s)']

                chunks = [c for c in self.__get_chunks(key, value_str, outer_hits)]

                placeholders = ','.join(placeholders * len(chunks))

                _INS = f"""insert into cache(key_hash, chunk, value, hits, uptime) VALUES {placeholders}"""

                await cur.execute(_INS, tuple(value for chunk in chunks for value in chunk))

                print(f'__setitem__: ASYNC SET item to L1 (Postgres Pro) on key "{key}"')
            except Exception as err:
                self.logger.error(f'ERROR ON STORE TO L1: {err}')

    def __get_chunks(self, key, value: str, hits: int = 0, uptime: float = False):

        if uptime is False:
            uptime = int(time())

        chunk = 1
        for i in range(0, len(value), self.CHUNK_SIZE):
            yield key, chunk, value[i:i + self.CHUNK_SIZE], hits, uptime
            chunk += 1

    async def uptime(self, key=None) -> Union[float, Literal[False]]:
        """При вызове без ключа отдает общий uptime текущего инстанса кеша; если ключа в кеше нет, отдаст False."""
        uptime = await super().uptime(key)

        if uptime is False:
            # print(f'LOCAL uptime is False for "{key}", search into the L1 for PID {os.getpid()}')
            # Здесь обновится uptime.
            if await self.__get_from_level_1(key):
                uptime = await super().uptime(key)
            else:
                # Не даем процессу обращаться к состоянию в L1-кеше чаще, чем 4 раза в секунду. Итого, при
                # конкурентности в 32 процесса мы получим максимум 128 запросов в секунду.
                await asyncio.sleep(1.0 / 4.0)

        return uptime

    @asynccontextmanager
    async def acquire(self, key) -> bool:
        """
        Распределенная блокировка на основе advisory locks Postgres:
        - https://habr.com/ru/companies/tensor/articles/488024/
        - https://postgrespro.ru/docs/postgresql/12/functions-admin#FUNCTIONS-ADVISORY-LOCKS
        """
        pid = os.getpid()
        async with super().acquire(key) as lock_attempt_local:
            # print(f'PID {pid} LOCAL LOCK is "{lock_attempt_local}" for key "{key}"')
            if lock_attempt_local:
                # Если нам удалось получить локальную блокировку на уровне текущего процесса (что не факт, т.к. нас
                # постоянно домогаются асинхронные конкуренты), то потратим время и обратимся за распределенной
                # блокировкой к Postgres:
                async with self.__async_postgres_pro_pool.acquire() as c, c.cursor(
                        cursor_factory=RealDictCursor) as cur:

                    # Про единицу во втором параметре hashtextextended:
                    # https://stackoverflow.com/questions/9809381/hashing-a-string-to-a-numeric-value-in-postgresql.
                    # «Just in case somebody wants a 64-bit hash value that is compatible with the existing 32-bit hash
                    # values, make the low 32-bits of the 64-bit hash value match the 32-bit hash value when the seed
                    # is 0».
                    # Нам это без необходимости, поэтому второй параметр 1.

                    _LOCK = """-- get lock for key "%(key)s":
                        SELECT pg_try_advisory_lock(hashtextextended(%(key)s, 1)) as pg_lock
                    """
                    await cur.execute(_LOCK, {'key': key})
                    lock_attempt_pg = (await cur.fetchone())['pg_lock']
                    # print(f'PID {pid} DISTRIBUTED LOCK is "{lock_attempt_pg}" for key "{key}"')
                    try:
                        yield lock_attempt_pg
                    finally:
                        if lock_attempt_pg:
                            # print(f'PID {pid} TRY TO RELEASE DISTRIBUTED LOCK FOR KEY "{key}"')
                            _UNLOCK = """-- unlock for key "%(key)s":
                                SELECT pg_advisory_unlock(hashtextextended(%(key)s, 1)) as pg_unlock
                            """
                            await cur.execute(_UNLOCK, {'key': key})
                            unlock = (await cur.fetchone())['pg_unlock']
                            print(f'PID {pid} RELEASE DISTRIBUTED LOCK FOR KEY "{key}": "{unlock}"')
            else:
                yield False

    def hits(self, key) -> int:
        """
        Попытка каждый раз обращаться в L1 за хитами сильно замедлит процесс...
        :param key:
        :return:
        """
        return super().hits(key)
