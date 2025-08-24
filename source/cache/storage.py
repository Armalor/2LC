import os
from pprint import pprint
from time import perf_counter, time
from typing import Optional, Dict, Union
from fastapi import Request, HTTPException, status
from fastapi.types import DecoratedCallable
import hashlib
from functools import wraps
from copy import copy
import asyncio
from contextlib import asynccontextmanager
import logging
from pydantic import BaseModel

try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal


# Локальный импорт:
import sys
from pathlib import Path
__root__ = Path(__file__).absolute().parent
sys.path.append(__root__.__str__())
from config import Config
# ~Локальный импорт

config = Config(__root__.__str__()).config


class CacheStorage:
    """Обертка для кеша"""

    TTL_1_HOUR = 3_600
    TTL_1_DAY = TTL_1_HOUR * 24

    # Сколько секунд ждем в single_request до следующего вызова:
    SINGLE_REQUEST_AWAIT_LIMIT = 60*1

    __instance: 'CacheStorage' = None

    def __new__(cls, *args, **kwargs):
        """Singleton"""

        # NB! Здесь НЕЛЬЗЯ использовать «cls.__instance», нужно строго «CacheStorage.__instance», т.к. при
        # наследовании вызов CacheStorage.__new__() и CacheStorageTarantool.__new__() даст разные cls
        # (но конкретно для __new__ это не точно).

        if CacheStorage.__instance is None:
            CacheStorage.__instance = super().__new__(cls)

        return CacheStorage.__instance

    @classmethod
    def get_instance(cls):
        """Singleton"""

        # NB! Здесь ТОЖЕ НЕЛЬЗЯ использовать «cls.__instance», нужно строго «CacheStorage.__instance», т.к. при
        # наследовании вызов CacheStorage.get_instance() и CacheStorageTarantool.get_instance() даст разные cls
        # (и это ТОЧНО!)

        if CacheStorage.__instance is None:
            CacheStorage.__instance = cls()

        return CacheStorage.__instance

    def __init__(self):

        self.config = config

        # Эта проверка нужна потому, что вызов «cache = CacheStorage()» (без get_instance) дернет __init__,
        # соответственно, если наличие хранилища не проверять, то каждый вызов CacheStorage() обнулит кэш.
        if not hasattr(self, "_CacheStorage__storage"):
            self.__storage: Dict[str, Union[Dict, str]] = dict()
            self.__storage_uptime: Dict[str, float] = dict()
            self.__storage_locked: Dict[str, float] = dict()
            self.__storage_hits: Dict[str, int] = dict()

            self.logger = logging.getLogger("uvicorn.default")

    async def __ainit__(self):
        self.logger.info(f'There are no async inits at {self.__class__.__name__}')

    def __contains__(self, item):
        return item in self.__storage

    def __increment_hits(self, key):
        if key in self.__storage_hits:
            self.__storage_hits[key] += 1

    def __getitem__(self, key):
        """ВНИМАНИЕ! Здесь мы НЕ БРОСАЕМ ИСКЛЮЧЕНИЕ ПРИ ОТСУТСТВИИ ЭЛЕМЕНТА, а отдаем None"""
        self.__increment_hits(key)

        ret: Union[dict, BaseModel] = self.__storage.get(key, None)

        if isinstance(ret, dict) and ret.get('__EXCEPTION') == 'HTTPException':
            raise HTTPException(
                status_code=ret['status_code'],
                detail=ret['detail'],
            )

        return ret

    def __setitem__(self, key, value, outer_hits: int = 0, outer_uptime: Union[Literal[False], float] = False):
        """

        :param key:

        :param value:

        :param outer_hits: «Внешние» хиты, могут быть не 0 при использовании L1 (классов-наследников).

        :param outer_uptime: Может НЕ быть False в случае, когда мы через __getitem__ подгружаем значение из кеша L1 и
        хотим сразу его сохранить сюда в L0.

        :return:
        """
        self.__storage_uptime[key] = outer_uptime or time()
        # Сбрасываем хиты:
        self.__storage_hits[key] = outer_hits
        # Ставим значение:
        self.__storage[key] = value
        # Снимаем блокировку (конкурирующих) обновлений:
        self.release(key)

    async def uptime(self, key=None) -> Union[float, Literal[False]]:
        """При вызове без ключа отдает общий uptime текущего инстанса кэша; если ключа в кэше нет, отдаст False."""
        if key is None:
            return perf_counter()

        key_uptime = self.__storage_uptime.get(key)
        if key_uptime:
            return int(time()) - key_uptime

        # Отдаем False (сводимое к 0.0, но проверяемое по «is False») для несуществующих ключей:
        return False

    @asynccontextmanager
    async def acquire(self, key) -> bool:
        """
        Запрос на блокировку значения по ключу. Вызывается только в случае, когда есть намерение обновлять значение
        длительным (потенциально конкурирующим) запросом.

        - Если блокировка еще не задана, то выставляем ее и возвращаем True
        - Если блокировка уже есть, то просто отдаем False
        """

        if not self.__storage_locked.get(key, False):
            self.__storage_locked[key] = True
            yield True
            self.release(key)
        else:
            yield False

    def release(self, key):
        self.__storage_locked[key] = False

    def hits(self, key) -> int:
        return self.__storage_hits.get(key, 0)

    @staticmethod
    async def get_cache_key(request: Request, func: callable) -> str:
        """Вместе с запросом ДОЛЖНА быть передана функция, для которой формируется кеш,
        т.к. на один запрос может быть несколько кешируемых функций.
        """

        # Столкнулись с проблемой, что, например, для /auth_organizations мы не можем подсчитать хиты. Поэтому
        # для методов с фиксированным ключом сохраняем значение ключа в самом методе:
        if hasattr(func, '__fixed_cache_key__'):
            return func.__fixed_cache_key__

        key = func.__name__ + request.url.path

        if len(request.query_params):
            # Исключаем user_key из параметров запроса вообще всегда, т.к. он мусорный по определению:
            denied_params = ('user_key', 'userKey')
            # Оставляем для ключа только те параметры, что ожидаются вызываемой функцией, чтобы избежать появления
            # «мусорных» значений в кеше:
            allowed_params = [k for k in func.__annotations__.keys() if k not in denied_params]

            query = [f'{k}={v}' for k, v in request.query_params.items() if k in allowed_params]
            key += f'?{"&".join(query)}'

        if 'POST' == request.method:
            body = await request.body()
            key += f'&body_md5={hashlib.md5(body).hexdigest()}'

        return key

    @staticmethod
    def __get_request(*args, **kwargs) -> Request:
        request = None

        for arg in args:
            if isinstance(arg, Request):
                request = arg

        for arg in kwargs.values():
            if isinstance(arg, Request):
                request = arg

        if request is None:
            raise Exception('Can not find request at *args, **kwargs')

        return request

    @staticmethod
    def cached(fixed_cache_key=None, ttl=TTL_1_DAY) -> DecoratedCallable:
        """
        Декоратор помимо кеширования работает в режиме «один запрос» — оборачивает, например, тяжелые запросы к базе,
        которые могут вызвать гонку: единовременно в обработке только один запрос (среди набора запросов с одинаковой
        сигнатурой), остальные же просто ждут, пока вернется ответ.

        :param fixed_cache_key: ключ кэша. Если пустой, то ищем его в request

        :param ttl: По умолчанию держим в кеше данные сутки.

        :return:
        """

        def decorator(func: DecoratedCallable) -> DecoratedCallable:
            @wraps(func)
            async def wrapper(*args, **kwargs):
                nonlocal fixed_cache_key, ttl

                if fixed_cache_key is None:
                    request = CacheStorage.__get_request(*args, **kwargs)

                    cache_key = await CacheStorage.get_cache_key(request, func)
                else:
                    # Если не копировать, то за счет nonlocal любые изменения
                    # (из-за изменений request от вызова к вызову) будут транслироваться на fixed_cache_key:
                    cache_key = copy(fixed_cache_key)

                cache_storage = CacheStorage.get_instance()

                # print(f'cache_key: {cache_key}')

                # Если ключа нет, то uptime вернет False → 0.0
                uptime = await cache_storage.uptime(cache_key)
                # print(f'key "{cache_key}" UPTIME={uptime}')

                # (на «is not False», а не на больше «<» проверяем потому, что при отставании временных меток
                # может получиться uptime меньше нуля):
                if uptime is not False and uptime < ttl:

                    # В __getitem__ выбрасываем исключение в случае, если это __EXCEPTION
                    return cache_storage[cache_key]

                # Здесь acquire НЕ ЖДЕТ блокировку, а получает сразу True/False:
                async with cache_storage.acquire(cache_key) as has_lock:

                    print(f'PID {os.getpid()} TOTAL LOCK: "{has_lock}" for key "{cache_key}"')
                    if has_lock:
                        print(f'key "{cache_key}" uptime "{uptime}" is False or less then TTL ({ttl})')
                        # Мы сюда попадаем только в случае, если истек TTL и удалось захватить блокировку обновления.
                        # Тут проблема: если кеш пустой и есть несколько конкурирующих запросов, то, не получив
                        # блокировку, запрос провалится до return и отдаст None.
                        # Чтобы этого избежать, ждем появления ключа (uptime'а), см. ниже по тексту.
                        try:
                            ret = await func(*args, **kwargs)
                            cache_storage[cache_key] = ret
                            return ret
                        except HTTPException as http_exception:
                            # Мы должны перехватывать и класть в кеш исключения вида «Справочник не найден» и
                            # «Запрашиваемая версия не существует», иначе при конкурентном выполнении мастер-процесс
                            # покажет ответ, а остальные будут ждать до состояния «Single request await over limit»,
                            # т.к. в кеш у них ничего не ляжет.
                            cache_storage[cache_key] = {
                                # Кладем в кеш специальный ключ «__EXCEPTION», который будет интерпретирован при
                                # обращении к __getitem__ как приказ выбросить исключение:
                                '__EXCEPTION': http_exception.__class__.__name__,
                                'status_code': http_exception.status_code,
                                'detail': http_exception.detail,
                            }

                            raise http_exception

                t0 = perf_counter()
                # Ждем, если ключа еще нет (первый запуск) и мы (в конкурентном запросе) не получили блокировку.
                # Внимание, здесь важный момент! Мы тут НЕ ПРОВЕРЯЕМ uptime < TTL —
                # если значение есть, то на время обновы просто отдадим просрочку.

                while (await cache_storage.uptime(cache_key)) is False:
                    # Внимание! для любого L1-хранилища проверка uptime может быть достаточно нагрузочной операцией,
                    # поэтому в L1 ждем довольно приличное время между вызовами (см. реализацию L1 для Postgres Pro).
                    #
                    # ВНИМАНИЕ! Здесь в L0 тоже ждем — если это ожидание убрать, то L1 при конкурентных
                    # запросах уходит в бесконечный цикл (объяснение ниже).
                    #
                    # UPD: объяснение оказывается предельно простым: «await asyncio.sleep...» ЕДИНСТВЕННАЯ await-точка
                    # во всем этом цикле: без нее он крутится монопольно. Механизм монопольного захвата пока не очень
                    # понимаю, но дело именно в этом. Возможно, захват возникает тогда, когда мы доходим до этой точки
                    # (вот конкретно до этой, допустим, что «await asyncio.sleep...» закомментирован) раньше чем
                    # в «await func(*args, **kwargs)» уходит запрос к целевой базе.
                    # Возможно, uptime вообще надо переделать в асинхронный режим.
                    #
                    # UPD-2: перевели uptime в асинхронный режим.
                    # await asyncio.sleep(0)
                    # UPD-3: Возникает странный артефакт: если запустить 4 «медленных» запроса /passport, параллельно
                    # быстрый запрос /tree, то /tree выполнится, а /passport все повиснут, причем (sic!), даже без
                    # «Single request await over limit» — просто браузер висит и ждет ответа.
                    # Если запустить еще один любой запрос, то ответ отдается.
                    # Хер вообще знает, что это и почему. Еще более хер знает, почему это исчезает при отключении
                    # «await asyncio.sleep»...
                    #
                    # UPD-4: это было, похоже из-за асинхронной части __setitem__. Вернули синхронную.

                    # print(f'PID {os.getpid()}, WE ARE IN UPTIME WAITING CYCLE: {perf_counter()-t0:.2f}')

                    if perf_counter() - t0 >= CacheStorage.SINGLE_REQUEST_AWAIT_LIMIT:
                        raise HTTPException(
                            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
                            detail=f"Single request await over limit! (PID {os.getpid()})",
                            headers={"WWW-Authenticate": "Basic"},
                        )

                # В __getitem__ выбрасываем исключение в случае, если это __EXCEPTION
                return cache_storage[cache_key]

            # Для методов с фиксированным ключом нужно сохранять значение, иначе get_cache_key ничего не найдет:
            if fixed_cache_key is not None:
                wrapper.__fixed_cache_key__ = fixed_cache_key

            return wrapper

        return decorator
