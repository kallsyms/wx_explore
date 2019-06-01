#!/usr/bin/env python3
import logging
import requests
import time

logger = logging.getLogger(__name__)


def get_grib_ranges(idxs, source_fields):
    """
    Given an index file, return a list of tuples that denote the start and length of each chunk
    of the GRIB that should be downloaded
    :param idxs: Index file as a string
    :param source: List of SourceField that should be extracted from the GRIB
    :return: List of (start, length)
    """
    offsets = []
    last = None
    for line in idxs.split('\n'):
        tokens = line.split(':')
        if len(tokens) < 7:
            continue

        _, offset, _, short_name, level, _, _ = tokens

        offset = int(offset)

        if last is not None:
            offsets.append((last, offset-last))
            last = None

        if any(sf.idx_short_name == short_name and sf.idx_level == level for sf in source_fields):
            last = offset

    return offsets


def reduce_grib(grib_url, idx_url, source_fields, out_f):
    for _ in range(3):
        try:
            idxs = requests.get(idx_url).text
            break
        except KeyboardInterrupt:
            raise
        except:
            time.sleep(5)
            continue
    else:
        raise Exception("Unable to download idx file!")

    offsets = get_grib_ranges(idxs, source_fields)

    for offset, length in offsets:
        start = offset
        end = offset + length - 1

        for _ in range(3):
            try:
                out_f.write(requests.get(grib_url, headers={
                    "Range": f"bytes={start}-{end}"
                }).content)
                break
            except KeyboardInterrupt:
                raise
            except:
                time.sleep(5)
                continue
        else:
            logger.warning(f"Couldn't get grib range from {start} to {end}. Continuing anyways...")

    out_f.flush()

    return len(offsets)
