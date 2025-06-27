import asyncio
import logging
import httpx
from typing import List, Type, TypeVar, Union
from pydantic import BaseModel
import pprint

T = TypeVar('T', bound=BaseModel)

logger = logging.getLogger(__name__)

async def fetch_page(
    url: str, 
    params: dict, 
    client: httpx.AsyncClient, 
    model: Type[T], 
    read_json: bool = True,
    retries: int = 10, 
    backoff_factor: float = 0.5
) -> Union[T, str]:
    delay = backoff_factor
    for attempt in range(retries):
        try:
            resp = await client.get(url, params=params, timeout=30)
            if resp.status_code == 429:
                raise httpx.HTTPStatusError("Too Many Requests", request=resp.request, response=resp)
            resp.raise_for_status()
            if read_json:
                data = resp.json()
                logger.debug(f"Response from {url}:\n{pprint.pformat(data)}")
            else:
                data = resp.text
                logger.debug(f"Raw response from {url}:\n{data}")
            break  # Exit retry loop on successful HTTP response
        except httpx.HTTPStatusError as e:
            if e.response is not None and e.response.status_code == 429:
                logger.warning(
                    f"Attempt {attempt + 1} failed for {url} with params {params} due to 429 Too Many Requests. Retrying in {delay:.2f} seconds."
                )
            else:
                status_code = e.response.status_code if e.response is not None else 'Unknown'
                reason = e.response.reason_phrase if e.response is not None and hasattr(e.response, 'reason_phrase') else str(e)
                logger.warning(
                    f"Attempt {attempt + 1} failed for {url} with params {params} due to HTTP {status_code}: {reason}. Retrying in {delay:.2f} seconds."
                )
            if attempt == retries - 1:
                logger.error(f"Failed to fetch {url} after {retries} attempts.")
                raise
            await asyncio.sleep(delay)
            delay *= 2
        except Exception:
            logger.warning(
                f"Unexpected error.  Attempt {attempt + 1} failed for {url} with params {params}. Retrying in {delay:.2f} seconds."
            )
            if attempt == retries - 1:
                logger.error(f"Failed to fetch {url} after {retries} attempts.")
                raise
            await asyncio.sleep(delay)
            delay *= 2
    logger.info(f"Successfully fetched {url} with params {params}.")
    if read_json:
        return model(**data)
    else:
        return data

async def fetch_pages(
    requests: List[dict],
    read_json: bool = True,
    concurrency: int = 10,
    retries: int = 10,
    backoff_factor: float = 0.5
) -> List[Union[BaseModel, str]]:
    # Validate input format
    for req in requests:
        if not (isinstance(req, dict) and 'url' in req and 'params' in req and 'model' in req):
            raise ValueError("Each request must be a dict with 'url', 'params', and 'model' keys.")
    semaphore = asyncio.Semaphore(concurrency)
    async with httpx.AsyncClient() as client:
        async def sem_fetch(req: dict):
            async with semaphore:
                return await fetch_page(
                    req['url'],
                    req['params'],
                    client,
                    req['model'],
                    retries=retries,
                    backoff_factor=backoff_factor,
                    read_json=read_json
                )
        tasks = [asyncio.create_task(sem_fetch(req)) for req in requests]
        responses = await asyncio.gather(*tasks)
        logger.info(f"Successfully fetched {len(responses)} pages concurrently.")
        return responses
