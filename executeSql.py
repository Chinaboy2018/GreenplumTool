# coding=utf-8
import os
import sys
import itertools
import getopt
import time
from collections import OrderedDict
import subprocess
from Queue import Queue

try:
    import threading as th
except ImportError:
    import dummy_threading as th


def get_args(argv):
    """

    :param argv:
    :return:
    """
    opts = []

    try:
        opts, args = getopt.getopt(argv, "p:d:f:",
                                   ('database=',
                                    'conf=',
                                    'help',
                                    'parallel'))

    except getopt.GetoptError:
        print("parameter error!!\n")
        exit(1)

    for opt, arg in opts:
        if opt in ['--help']:
            print "" \
                  "-p 设置并发，默认是4个并发" \
                  "-f 指定SQL文件位置，注意该脚本只能对单行命令进行处理，复杂SQL不支持" \
                  "-d 数据库" \
                  "命令示例：python2 distributeTable.py  -f '/home/zch/tmp/distributeTable.sql' -d work -p 5"
            exit(0)
        elif opt in ['-d', '--database']:
            op.DATABASE_NAME = arg
        elif opt in ['-p', '--parallel']:
            op.PARALLEL = arg
        elif opt in ['-f', '--conf']:
            op.TABLE_OF_PATH = arg
        else:
            pass


class Process(th.Thread):
    def __init__(self, thread_id, name, q):
        th.Thread.__init__(self)
        self._thread_id = thread_id
        self._name = name
        self._q = q
        pass

    def run(self):
        print "======>Starting " + self._name + "<======"
        # num = 0
        while True:
            if self._q.not_empty:
                # num = num + 1
                tmp = self._q.get()
                start_run_sql(tmp)
                self._q.task_done()
            else:
                break


class Options:
    def __init__(self):
        pass

    TABLE_OF_PATH = None
    TABLE_OF_FILE = []
    DDL_OF_EXTERNAL, SQL_OF_INSERT, TABLE_OF_EXT = OrderedDict(), OrderedDict(), OrderedDict()
    LINE_OF_DELIMITER, COL_OF_DELIMITER, = '\n', '|@@|'
    DATABASE_NAME, PGPORT, PGHOST, PGUSER = 'work', '5432', '192.168.126.30', 'gpadmin'
    PARALLEL = 4
    queue_lock = th.Lock()
    work_queue = Queue()
    queue_flag = False
    file_path = '/tmp/error.list'


op = Options()


def read_cmd_file():
    with open(op.TABLE_OF_PATH) as fh:
        temp = fh.readlines()

    for i in temp:
        cell = i.strip()
        if cell != '':
            op.TABLE_OF_FILE.append(cell)


def exe_query(sql_str):
    lst_of_col = []
    CMD = "PGDATABASE=%s PGPORT=%s PGHOST=%s PGUSER=%s PGOPTIONS='-c optimizer=off -c client_encoding=UTF8' " % (
        op.DATABASE_NAME, op.PGPORT, op.PGHOST, op.PGUSER)
    CMD = CMD + "psql -R '%s' -tAXF '%s' -v ON_ERROR_STOP=1 2>&1 <<END_OF_SQL \n" % (
        op.LINE_OF_DELIMITER, op.COL_OF_DELIMITER)
    CMD = CMD + sql_str + "\n"
    CMD = CMD + "END_OF_SQL"
    result = subprocess.Popen(CMD, shell=True, stdout=subprocess.PIPE)
    lst_of_line = result.stdout.read().strip().split(op.LINE_OF_DELIMITER, -1)
    for line in lst_of_line:
        lst_of_col.append(line.split(op.COL_OF_DELIMITER, -1))
    return lst_of_col


def get_time():
    curr_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    return curr_time


def out_put_file(string, file_path):
    with open(file_path, 'a+') as fn:
        fn.write(string)


def start_run_sql(sql):
    st = get_time()
    result = exe_query(sql)
    et = get_time()

    print "Query: {2}|Start: {0}|End: {1}|Result:{3}".format(st, et, sql,
                                                             ' '.join(list(itertools.chain.from_iterable(result))))


def do_methed(parallel):
    threads = []
    for t in range(int(parallel)):
        work = Process(t, "thread-{0}".format(t), q=op.work_queue)
        threads.append(work)
        work.setDaemon(True)
        work.start()

    for v in op.TABLE_OF_FILE:
        op.work_queue.put(v)

    op.work_queue.join()

    print "\n\n======>All task done ; Exiting do_methed Thread.<======\n\n"


def run():
    get_args(sys.argv[1:])

    read_cmd_file()
    do_methed(op.PARALLEL)


if __name__ == "__main__":
    run()

