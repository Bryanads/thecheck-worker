import asyncpg
from src.utils.config import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME

_async_pool = None

async def init_async_db_pool():
	global _async_pool
	if _async_pool is None:
		_async_pool = await asyncpg.create_pool(
			user=DB_USER,
			password=DB_PASSWORD,
			host=DB_HOST,
			port=DB_PORT,
			database=DB_NAME,
			min_size=1,
			max_size=10
		)
	return _async_pool

async def get_async_db_connection():
	global _async_pool
	if _async_pool is None:
		raise Exception("Async DB pool not initialized. Call init_async_db_pool() first.")
	return await _async_pool.acquire()

async def release_async_db_connection(conn):
	global _async_pool
	if _async_pool and conn:
		await _async_pool.release(conn)

async def close_db_pool():
    """
    Fecha o pool de conexões para um encerramento limpo.
    """
    global _async_pool
    if _async_pool:
        await _async_pool.close()
        _async_pool = None
        print("Pool de conexões do worker fechado.")