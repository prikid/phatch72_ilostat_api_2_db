#!/usr/bin/env python3
import argparse
import asyncio
import io
import os
import logging
import asyncpg

import aiohttp
import pandas as pd
from aiohttp import ClientSession
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

API_ENDPOINT = 'https://rplumber.ilo.org/data/indicator/'
DB_TABLE_NAME = os.getenv('DB_TABLE_NAME')

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s', datefmt='%d-%m-%Y %I:%M:%S')
logger = logging.getLogger(__name__)

# Parse arguments
argp = argparse.ArgumentParser()
argp.add_argument("-i", "--input", help='Path to the input Excel file with indicators list',
                  default='Indicator List.xlsx')
argp.add_argument("-n", "--max_async_requests", help="Limit of concurrent requests", default=3, type=int)
argp.add_argument("-d", "--delete", help="Delete the existing table", action='store_true', default=False)
args = argp.parse_args()

semaphore = asyncio.Semaphore(args.max_async_requests)

# Database URI
db_uri = "postgresql://{user}:{password}@{host}:{port}/{dbname}".format(
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASS'),
    host=os.getenv('DB_HOST'),
    port=os.getenv('DB_PORT'),
    dbname=os.getenv('DB_NAME')
)

dtype_map = {
    'int64': 'BIGINT',
    'float64': 'FLOAT',
    'object': 'TEXT',
    'bool': 'BOOLEAN',
    'datetime64[ns]': 'TIMESTAMP',
}

schema_lock = asyncio.Lock()  # Defined at the top level

# Global connection pool
conn_pool = None


async def connect_db():
    global conn_pool
    conn_pool = await asyncpg.create_pool(dsn=db_uri)


async def close_db():
    global conn_pool
    await conn_pool.close()


async def is_db_table_exist() -> bool:
    async with conn_pool.acquire() as connection:
        query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1);"
        return await connection.fetchval(query, DB_TABLE_NAME)


async def create_db_table(df: pd.DataFrame):
    columns = ', '.join([f"{col} {dtype_map[str(dtype)]}" for col, dtype in zip(df.columns, df.dtypes)])
    query = f"CREATE TABLE {DB_TABLE_NAME} ({columns});"
    async with conn_pool.acquire() as connection:
        await connection.execute(query)
    logger.info(f"Table '{DB_TABLE_NAME}' created.")


async def sync_columns(df: pd.DataFrame) -> pd.DataFrame:
    async with schema_lock:
        async with conn_pool.acquire() as connection:
            query = f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = '{DB_TABLE_NAME}';
            """
            existing_columns = [row['column_name'] for row in await connection.fetch(query)]

            new_columns = [
                (col, dtype_map[str(df[col].dtype)])
                for col in df.columns
                if col not in existing_columns
            ]

            if new_columns:
                alter_queries = [
                    f"ALTER TABLE {DB_TABLE_NAME} ADD COLUMN {col} {dtype};"
                    for col, dtype in new_columns
                ]
                for alter_query in alter_queries:
                    await connection.execute(alter_query)
                logger.info(f"Added columns: {[col for col, _ in new_columns]} to table {DB_TABLE_NAME}.")

        for col in existing_columns:
            if col not in df.columns:
                df[col] = ''

    return df


async def save_to_db(data, indicator):
    df = pd.read_csv(io.BytesIO(data), low_memory=False)

    async with conn_pool.acquire() as connection:
        async with schema_lock:  # Ensure the table is created only once
            if not await is_db_table_exist():  # Recheck inside the lock
                await create_db_table(df)

        df = await sync_columns(df)
        await load_data_to_db(df, indicator)


async def load_data_to_db(df: pd.DataFrame, indicator: str):
    csv_data = df.to_csv(index=False, header=True)

    async with conn_pool.acquire() as connection:
        try:
            await connection.copy_to_table(
                table_name=DB_TABLE_NAME,
                source=io.BytesIO(csv_data.encode('utf-8')),
                columns=df.columns.to_list(),
                format='csv',
                header=True
            )
            logger.info(f'The {indicator} data successfully loaded into the DB table {DB_TABLE_NAME}')
        except Exception as e:
            logger.exception(f"Error loading the {indicator} data into {DB_TABLE_NAME}: {e}")


async def fetch_data(session: ClientSession, indicator: str) -> bytes | None:
    url = f'{API_ENDPOINT}?id={indicator}'
    retry_attempts = 3
    delay = 30  # Initial delay of 30 seconds

    for attempt in range(retry_attempts):
        try:
            logger.info(f'Fetching data for the indicator {indicator} (Attempt {attempt + 1})')
            async with session.get(url) as response:
                if response.status != 200:
                    logger.error(f'Error while fetching data for indicator {indicator}. '
                                 f'Status code: {response.status}.')
                    return None
                else:
                    return await response.read()
        except asyncio.TimeoutError:
            logger.warning(f'Timeout occurred for indicator {indicator} on attempt {attempt + 1}. '
                           f'Retrying in {delay} seconds...')
            await asyncio.sleep(delay)
            delay *= 2  # Double the delay for the next attempt

    logger.error(f'Failed to fetch data for indicator {indicator} after {retry_attempts} attempts due to timeout.')
    return None


async def bounded_fetch(indicator):
    async with semaphore:  # Control concurrency with the semaphore
        async with aiohttp.ClientSession() as session:
            data = await fetch_data(session, indicator)
        if data is not None:
            await save_to_db(data, indicator)


# Main function to orchestrate fetching and saving
async def main():
    await connect_db()

    try:
        if args.delete:
            async with conn_pool.acquire() as connection:
                async with schema_lock:
                    await connection.execute(f"DROP TABLE IF EXISTS {DB_TABLE_NAME};")
                logger.info(f'The existing table {DB_TABLE_NAME} deleted successfully.')

        # Read indicators from an Excel file
        indicators_df = pd.read_excel(args.input)
        indicators = indicators_df['id'].dropna().drop_duplicates().to_list()

        tasks = [asyncio.create_task(bounded_fetch(indicator)) for indicator in indicators]
        await asyncio.gather(*tasks)
    finally:
        await close_db()


# Run the main function
if __name__ == '__main__':
    asyncio.run(main())
