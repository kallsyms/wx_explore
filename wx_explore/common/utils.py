import datetime
import logging
import requests


logger = logging.getLogger(__name__)


def datetime2unix(dt: datetime.datetime) -> int:
    """
    Convert the given datetime `dt` to an integer unix timestamp.
    If `dt` has no tz associated with it, it is assumed to be utc.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return int(dt.timestamp())


def get_url(url, headers=None, retries=3):
    if headers is None:
        headers = {}

    for i in range(retries):
        try:
            r = requests.get(url, headers=headers)
            break
        except KeyboardInterrupt:
            raise
        except Exception as e:
            logger.exception("Exception while GETing %s. Retrying...", url)
            time.sleep(3**i)
            continue
    else:
        raise Exception(f"Unable to download file after {retries} retries!")

    if not (200 <= r.status_code < 300):
        raise Exception(f"Unable to download file: unexpected status code {r.status_code}")

    return r


def url_exists(url):
    r = requests.head(url)
    return 200 <= r.status_code < 300
