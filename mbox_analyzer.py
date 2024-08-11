#!/usr/bin/env python3
"""
    This script reads a (possibly huge) mbox file (such as a Gmail takeout)
    and prints statistics on message size and count broken down by labels, senders, recepients, etc.
"""
import sys
import os
from dataclasses import dataclass
from collections import defaultdict
from email.message import Message
from email.policy import default
from typing import Iterable
import argparse
import email
import re
import time
import logging


logger = logging.getLogger(__name__)

class MboxReader:
    def __init__(self, filename):
        self.handle = open(filename, 'rb')
        self.handle.seek(0, os.SEEK_END)
        self.size = self.handle.tell()
        self.handle.seek(0, os.SEEK_SET)
        assert self.handle.readline().startswith(b'From ')

    def __enter__(self):
        return self

    @property
    def pos(self) -> int:
        return self.handle.tell()

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.handle.close()

    def __iter__(self):
        return iter(self.__next__())

    def __next__(self) -> Iterable[Message]:
        lines = []
        while True:
            line = self.handle.readline()
            if line == b'' or line.startswith(b'From '):
                bytes_ = b''.join(lines)
                msg = email.message_from_bytes(bytes_, policy=default)
                msg._x_sz = len(bytes_)
                yield msg
                if line == b'':
                    break
                lines = []
                continue
            lines.append(line)


@dataclass
class StatisticsLine:
    count: int
    total_size_bytes: int
    from_addr: str
    labels: str


def filter_gmail_labels(comma_separated_labels: str) -> list[str]:
    """ Skip boring built-in Gmail labels """
    labels = comma_separated_labels.split(',')
    good_labels = []
    for x in labels:
        if x in {'Inbox', 'Important', 'Opened'}:
            continue
        if x.startswith('Category '):
            continue
        good_labels.append(x)
    return good_labels


def extract_address(x: str) -> str:
    """ Extract an email address from a From header """
    m = re.findall(r"<([^>@]+@[^>]+)>", x)
    if m:
        return m[0]
    return x


def read_messages(reader: MboxReader) -> Iterable[StatisticsLine]:
    t0 = int(time.time())
    for msg in reader:
        # size = len(bytes(msg))
        size = msg._x_sz
        from_ = msg["From"]
        to_ = msg["To"]
        gmail_labels = msg["X-Gmail-Labels"]

        if gmail_labels is None:
            gmail_labels = str(gmail_labels)
        if from_ is None:
            from_ = str(from_)
        from_ = extract_address(from_)
        labels = filter_gmail_labels(gmail_labels)
        labels = ','.join(list(sorted(labels)))
        yield StatisticsLine(
            count=1,
            total_size_bytes=size,
            from_addr=from_,
            labels=labels,
        )
        t1 = int(time.time())
        if t1 > t0:
            pos = reader.pos
            progress = 100.0 * pos / reader.size
            logger.info("%.1f%% read (%d / %d)", progress, pos, reader.size)
            t0 = t1
    logger.info("100.0%% read")


def agg_stats(ms: Iterable[StatisticsLine]) -> Iterable[StatisticsLine]:
    sizes = defaultdict(int)
    counts = defaultdict(int)
    def key(x :StatisticsLine) -> tuple:
        return (x.from_addr, x.labels)
    for m in ms:
        k = key(m)
        sizes[k] += m.total_size_bytes
        counts[k] += m.count

    for k, sz in sizes.items():
        line = StatisticsLine(
            total_size_bytes=sz,
            count=counts[k],
            from_addr=k[0],
            labels=k[1],
        )
        yield line


def sort_stats(ms: Iterable[StatisticsLine]) -> Iterable[StatisticsLine]:
    all_stats = list(ms)
    all_stats.sort(key=lambda x: x.total_size_bytes)
    return all_stats


def print_messages(ms: Iterable[StatisticsLine]):
    for m in ms:
        print(m.total_size_bytes, m.count, m.from_addr, m.labels)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mbox_file", help="path to an mbox file")
    parser.add_argument("--agg", action='store_true', help="aggregate statistics rather than printing a line per message")
    parser.add_argument("--sort", action='store_true', help="sort statistics by total size in bytes, ascending")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, stream=sys.stderr, format="%(asctime)s %(levelname)s:  %(message)s")

    reader = MboxReader(args.mbox_file)
    messages = read_messages(reader)
    if args.agg:
        messages = agg_stats(messages)
    if args.sort:
        messages = sort_stats(messages)
    print_messages(messages)

if __name__ == '__main__':
    main()
