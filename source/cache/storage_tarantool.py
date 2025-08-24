from pprint import pprint
from time import perf_counter, time
from typing import Optional, Dict, Union
from pydantic import BaseModel
from tarantool import Connection as TarantoolConnection
from tarantool.error import NetworkError
from fastapi.encoders import jsonable_encoder
import logging
import json

try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal


# Локальный импорт:
import sys
from pathlib import Path
__root__ = Path(__file__).absolute().parent
sys.path.append(__root__.__str__())
from cache import CacheStorage
from config import Config
# ~Локальный импорт


# TODO: acquire
# TODO: async __get_from_level_1 → real async uptime
class CacheStorageTarantool(CacheStorage):
    """Обертка для L1-кеша на базе Tarantool"""

    def __init__(self):

        super().__init__()

        # Эта проверка нужна потому, что вызов «cache = CacheStorage()» (без get_instance) дернет __init__,
        # если наличие атрибута не проверять, то каждый вызов CacheStorage() — super().__init__() — обнулит кэш.
        if not hasattr(self, "_CacheStorageTarantool__tarantool_connection"):
            tarantool_storage_dsn = f'{self.config.tarantool.connection.user}@{self.config.tarantool.connection.host}:{self.config.tarantool.connection.port}'

            try:
                self.__tarantool_connection = TarantoolConnection(**self.config.tarantool.connection)
                self.logger.info(f'Cache storage: Tarantool connection {tarantool_storage_dsn} established!')
            except NetworkError as err:
                self.logger.warning(f"Cache storage: can't connect to Tarantool at {tarantool_storage_dsn}, {err}. Work in single state mode.")

    def __contains__(self, item):
        return super().__contains__(item)

    def __get_from_level_1(self, key, update_hits=False):
        """
        Получаем данные из Тарантула для локального обновления данных.

        :param key:

        :param update_hits: пока не используется, задумывалось для обновления хитов в случае, есл L1-значение
        есть, а локального нет

        :return:
        """

        if self.__tarantool_connection:
            print(f'__get_from_level_1: key "{key}" not in local storage, search in Tarantool...')
            resp = self.__tarantool_connection.select('storage', key)
            data = resp.data

            if len(data) and data[0] is not None:
                print(f'__get_from_level_1: FOUND key {key} in Tarantool!')
                key, value_str, hits, key_uptime = data[0]
                value = json.loads(value_str)

                self.__setitem__(key, value, outer_hits=hits, outer_uptime=key_uptime)

                return value

            print(f'__get_from_level_1: key "{key}" NOT FOUND IN TARANTOOL')

        return None

    def __getitem__(self, key):
        """ВНИМАНИЕ! Здесь мы НЕ БРОСАЕМ ИСКЛЮЧЕНИЕ ПРИ ОТСУТСТВИИ ЭЛЕМЕНТА, а отдаем None"""
        ret = super().__getitem__(key)

        return ret or self.__get_from_level_1(key, update_hits=True)

    def __setitem__(self, key, value, outer_hits: int = 0, outer_uptime: Union[Literal[False], float] = False):
        """

        :param key:

        :param value:

        :param outer_uptime: Может НЕ быть False в случае, когда мы через __getitem__ подгружаем значение из кеша и
        хотим сразу его сохранить в L0.

        :param outer_hits:

        :return:
        """

        new_uptime = outer_uptime or time()

        super().__setitem__(key, value, outer_hits, new_uptime)

        # Если есть соединение с Тарантулом и это значение, пришедшее НЕ из Тарантула (outer_uptime пустой),
        # то пихаем в Тарантул:
        if self.__tarantool_connection and not outer_uptime:
            # НИЧЕГО не приводим к строкам «насильно» (default=str закомментирован), чтобы не пропустить расхождение
            # типов данных из L1-кеша и локального L0: лучше пусть пизданется сразу, чем неведомо когда потом.
            #
            # Чтобы предотвратить падение при сохранении в Tarantool, все запросы (к базе, например), потенциально
            # возвращающие сложные объекты (дата-время), должны оборачиваться либо в модели, либо в jsonable_encoder.

            if isinstance(value, BaseModel):
                value = jsonable_encoder(value)

            value_str = json.dumps(value, ensure_ascii=False, sort_keys=True)  #, default=str)

            print(f'__setitem__: SET item to Tarantool on key "{key}"')

            self.__tarantool_connection.replace(
                self.config.tarantool.space,
                (key, value_str, super().hits(key), new_uptime)
            )

    async def uptime(self, key=None) -> Union[float, Literal[False]]:
        """При вызове без ключа отдает общий uptime текущего инстанса кэша; если ключа в кэше нет, отдаст False."""
        current_perf_counter = perf_counter()

        uptime = await super().uptime(key)

        if uptime is False:
            print(f'uptime: {key} local uptime is False')
            # Здесь обновится uptime:
            if self.__get_from_level_1(key):
                uptime = await super().uptime(key)

        return uptime

    def hits(self, key) -> int:
        """
        Попытка каждый раз обращаться в Тарантул за хитами сильно замедлит процесс...
        :param key:
        :return:
        """
        return super().hits(key)
