# coding=utf-8
import datetime
import os
import sys
import re
import itertools
import getopt
import time
from collections import OrderedDict
import subprocess
import getpass
import signal
import traceback
from Queue import Queue

try:
    import threading as _threading
except ImportError:
    import dummy_threading as _threading


def get_args(argv):
    """

    :param argv:
    :return:
    """
    opts = []
    # global opts

    try:
        opts, args = getopt.getopt(argv, "d:p:h:f:u:t:s:D:P",
                                   ('database=',
                                    'port=',
                                    'host=',
                                    'user=',
                                    'table-list=',
                                    'help',
                                    'gpfdist-host=',
                                    'gpfdist-port=',
                                    'td=',
                                    'gpfdist-dir=',
                                    'export',
                                    'import',
                                    'schema',
                                    'parallel',
                                    'logfile',
                                    'gpfdist-port-dir='))

    except getopt.GetoptError:
        print("parameter error!!\n")
        exit(1)

    for opt, arg in opts:
        if opt in ['--help']:
            print "python2 ~/PycharmProjects/Tool/exportData/dataload.py -f /home/zch/tmp/table_lst.conf " \
                  "-d tpcds -p 5432 -h 192.168.72.10 -u gpadmin --gpfdist-host 192.168.72.10 -D 8080:/tmp --import"
            exit(0)
        elif opt in ['-f', '--table-list']:
            op.TABLE_OF_PATH = arg
        elif opt in ['-d', '--database']:
            op.DATABASE_NAME = arg
        elif opt in ['-p', '--port']:
            op.PGPORT = arg
        elif opt in ['-u', '--user']:
            op.PGUSER = arg
        elif opt in ['-h', '--host']:
            op.PGHOST = arg
        elif opt in ['--gpfdist-host']:
            op.HOST_OF_GPFDIST = arg
        elif opt in ['--gpfdist-port']:
            op.PORT_OF_GPFDIST = arg
        elif opt in ['-t', '--td']:
            op.TIME_DIR = arg
        elif opt in ['--gpfdist-dir']:
            op.DIR_OF_GPFDIST = arg
        elif opt in ['--export']:
            op.ACTION = False
        elif opt in ['--import']:
            op.ACTION = True
        elif opt in ['-s', '--schema']:
            op.TABLE_OF_SCHEMA = arg
        elif opt in ['-P', '--parallel']:
            op.PARALLEL = arg
        # elif opt in ['-l', '--logfile']:
        #     op.option.l = arg
        elif opt in ['-D', '--gpfdist-port-dir']:
            op.gpfdist = arg
        else:
            pass


def exe_query(sql_str):
    lst_of_col = []
    error_code = 0
    CMD = "PGDATABASE=%s PGPORT=%s PGHOST=%s PGUSER=%s PGOPTIONS='-c optimizer=off -c client_encoding=UTF8' " % (op.DATABASE_NAME, op.PGPORT, op.PGHOST, op.PGUSER)
    CMD = CMD + "psql -R '%s' -tAXF '%s' -v ON_ERROR_STOP=1  <<END_OF_SQL \n" % (op.LINE_OF_DELIMITER, op.COL_OF_DELIMITER)
    CMD = CMD + sql_str + "\n"
    CMD = CMD + "END_OF_SQL"
    # Dataload().log(CMD, level=5)
    result = subprocess.Popen(CMD, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

    out, err = result.communicate()
    # out = result.stdout.read().strip()
    # err = result.stderr.read().strip()

    # result.stderr.close()
    # result.stdout.close()

    if err != '':
        error_code = 1
        return err, error_code
    elif out != '':
        # error_code = 0
        lst_of_line = out.strip().split(op.LINE_OF_DELIMITER, -1)
        for line in lst_of_line:
            lst_of_col.append(line.split(op.COL_OF_DELIMITER, -1))
        return lst_of_col, error_code


def exe_query2(sql_str):
    CMD = "PGDATABASE=%s PGPORT=%s PGHOST=%s PGUSER=%s PGOPTIONS='-c optimizer=off -c client_encoding=UTF8' " % (op.DATABASE_NAME, op.PGPORT, op.PGHOST, op.PGUSER)
    CMD = CMD + "psql -R '%s' -tAXF '%s' -v ON_ERROR_STOP=1 <<END_OF_SQL \n" % (op.LINE_OF_DELIMITER, op.COL_OF_DELIMITER)
    CMD = CMD + sql_str + "\n"
    CMD = CMD + "END_OF_SQL"
    result = subprocess.call(CMD, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return result


def get_time():
    curr_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    return curr_time


def kill():
    curr_time = get_time()
    h, m, s = curr_time.split()[-1].split(':')
    if int(h) == 23:
        if int(m) < 59:
            pass
        elif int(m) >= 59:
            # print get_time() + ':process will be killed!!!\n'
            # exit(1)
            pass
    elif 0 <= int(h) <= 2:
        if int(m) < 40:
            pass
        elif int(m) >= 41:
            print get_time() + ' : process will be killed!!!\n'
            exit(1)


def splice_dict(num, split_obj):
    list_result = []
    len_raw_obj = len(split_obj)

    if isinstance(split_obj, dict):
        if len_raw_obj > num:
            base_num = len_raw_obj / num
            addr_num = len_raw_obj % num
            for i in range(num):
                this_dict = dict()
                keys = list()
                if addr_num > 0:
                    keys = split_obj.keys()[:base_num + 1]
                    addr_num -= 1
                else:
                    keys = split_obj.keys()[:base_num]
                for key in keys:
                    this_dict[key] = split_obj[key]
                    del split_obj[key]
                list_result.append(this_dict)
        else:
            for d in split_obj:
                this_dict = dict()
                this_dict[d] = split_obj[d]
                list_result.append(this_dict)

    elif isinstance(split_obj, list):
        if len_raw_obj > num:
            base_num = len_raw_obj / num
            addr_num = len_raw_obj % num
            for i in range(num):
                this_list = list()
                if addr_num > 0:
                    this_list = split_obj[:base_num + 1]
                    addr_num -= 1
                else:
                    this_list = split_obj[:base_num]
                list_result.append(this_list)
                for d in this_list:
                    split_obj.remove(d)
        else:
            this_list = list()
            this_list = split_obj
            list_result.append(this_list)

    return list_result


class Option:
    def __init__(self):
        pass


class Work(_threading.Thread):
    def __init__(self, thread_id, name, q):
        _threading.Thread.__init__(self)
        self._thread_id = thread_id
        self._name = name
        self._q = q
        pass

    def run(self):
        Dataload().log("======>Starting " + self._name + "<======", level=3)

        while True:
            if self._q.not_empty:
                tmp = self._q.get()
                op.start_transfer_data(tmp, self._name)
                self._q.task_done()
            else:
                break


class Dataload:
    def __init__(self):
        self.options = Option()
        self.exit_value = 0
        self.HOST_OF_GPFDIST = ''
        self.DIR_OF_GPFDIST = None
        self.TABLE_FILE_NAME = ''
        self.TABLE_OF_PATH = ''
        self.TABLE_OF_LIST = []
        self.DDL_OF_EXTERNAL = OrderedDict()
        self.SQL_OF_INSERT = OrderedDict()
        self.TABLE_OF_EXT = OrderedDict()
        self.LINE_OF_DELIMITER = '\n'
        self.COL_OF_DELIMITER = '|@@|'
        self.TIME_DIR = ''
        self.DATABASE_NAME = 'template1'
        self.PGHOST = '127.0.0.1'
        self.PGUSER = 'gpadmin'
        self.PGPORT = '5432'
        self.TABLE_OF_SCHEMA = None
        self.EXTERNL_OF_SCHEMA = 'transfer_data_by_ext'
        self.GPFDIST_PORT = [8080]
        self.ACTION = False
        self.PARALLEL = 4
        self.queue_lock = _threading.Lock()
        self.work_queue = Queue(10)
        self.queue_flag = False
        self.GPFDIST_PID = []
        self.DEBUG = 5
        self.LOG = 4
        self.INFO = 3
        self.WARN = 2
        self.ERROR = 1
        self.options.l = None
        self.options.qv = self.INFO
        self.gpfdist = None
        self.table_count = None

        if self.options.l is None:
            self.options.l = os.path.join(os.environ.get('HOME', '.'), 'gpAdminLogs')
            if not os.path.isdir(self.options.l):
                os.mkdir(self.options.l)

            self.options.l = os.path.join(self.options.l, 'dataload_' + datetime.date.today().strftime('%Y%m%d') + '.log')

        try:
            self.logfile = open(self.options.l, 'a')
        except Exception, e:
            self.log("could not open logfile %s: %s" % (self.options.l, e), self.ERROR)

    SQL_OF_SCHEMA = "select pn.nspname from pg_catalog.pg_namespace pn;"
    GET_NAMESPACE_SQL = "select  oid,nspname from pg_catalog.pg_namespace WHERE oid >=16384 or oid = 2200;"
    SQL_OF_TABLE = ("select c.oid,n.nspname,c.relname\n"
                    "    from pg_class c,pg_namespace n,gp_distribution_policy p\n"
                    "    where c.relnamespace=n.oid and c.oid=p.localoid \n"
                    "    and (c.relnamespace>16384 or n.nspname='public') \n"
                    "    and n.nspname NOT IN ('gpexpand', 'pg_bitmapindex', 'information_schema', 'gp_toolkit', 'pg_aoseg', 'pg_toast', 'pg_catalog')\n"
                    "    and n.nspname not like E'pg\_temp\_%' \n"
                    "    and n.nspname not like E'pg\_toast\_temp\_%'\n"
                    "    and c.relkind='r' \n"
                    "    and relstorage<>'x' \n"
                    "    and c.oid not in(select parchildrelid from pg_partition_rule)\n")
    # GET_TABLE_SQL = ("select n.nspname||'.'||t.tablename as tablename from pg_catalog.pg_tables t,pg_catalog.pg_class c,"
    #                  "pg_catalog.pg_namespace n\n"
    #                  "where c.relname = t.tablename\n"
    #                  "and  c.relnamespace = n.oid\n"
    #                  "and c.relkind = 'r'\n"
    #                  "and c.relstorage <>'x'\n"
    #                  "and t.schemaname not in ('pg_catalog','information_schema','gp_toolkit')\n"
    #                  "and not EXISTS (select 1 from pg_catalog.pg_partition_rule where c.oid = pg_partition_rule.parchildrelid)\n")
    SQL_OF_CHECK = ("SELECT EXISTS\n"
                    "(\n"
                    "SELECT * FROM pg_class JOIN pg_catalog.pg_namespace n ON n.oid = pg_class.relnamespace\n"
                    "WHERE n.nspname=E'{0}' and relname=E'{1}'\n"
                    ")")
    GET_OF_OID = "select pc.oid from pg_class pc, pg_namespace pn where pc.relnamespace = pn.oid and relname = '{0}' and pn.nspname = '{1}'"
    GET_OF_DDL = "pg_dump --gp-syntax -h {0} -p 5432 -U {1} -s -t {2} {3}"
    """
    command description
    exmaple:
    pg_dump --gp-syntax -h 10.5.0.118 -p 5432 -U gpadmin -s -t '"u_liu"."bill_type_code"' 'report'
    """

    def level_transfer(self, level):
        if level == self.DEBUG:
            return "DEBUG"
        elif level == self.LOG:
            return "LOG"
        elif level == self.INFO:
            return "INFO"
        elif level == self.ERROR:
            return "ERROR"
        elif level == self.WARN:
            return "WARN"
        else:
            self.log(self.ERROR, "unknown log type %i" % level)

    def log(self, a, level=3):
        """
        Level is either DEBUG, LOG, INFO, ERROR. a is the message
        """
        message = ''
        try:
            # t = time.localtime()
            message = '|'.join(
                [datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S') + ' ', self.level_transfer(level), a]) + '\n'

            message = message.encode('utf-8')
        except Exception, e:
            self.logfile.write("\nWarning: Log() threw an exception: {0} \n".format(e))

        if level <= self.options.qv:
            sys.stdout.write(message)

        if level <= self.options.qv or level <= self.DEBUG:
            try:
                self.logfile.write(message)
                self.logfile.flush()
            except AttributeError, e:
                pass

        if level == self.ERROR:
            # sys.exit(2)  #  直接退出会导致进程僵住
            pass

    def table_check(self, table, schema='public'):
        """

        :param table:
        :param schema:
        :return:
        """
        check_of_sql = self.SQL_OF_CHECK.format(schema, table)
        # self.log("table_check function:" + check_of_sql, self.INFO)

        row_of_list, error_code = exe_query(check_of_sql)

        if row_of_list[-1][-1] == 't':
            get_of_oid = self.GET_OF_OID.format(table, schema)
            row_of_list, error_code = exe_query(get_of_oid)
            self.log(schema + '.' + table + ' check ok, table oid is :' + row_of_list[-1][-1], self.LOG)
            return row_of_list[-1][-1]
        else:
            self.log("table_check function:relation {0}.{1} dose not exit".format(schema, table), self.ERROR)
            return 0

    def start_gpfdist(self, path, counter=1):
        srcfile = None
        if os.environ.get('GPHOME_LOADERS'):
            srcfile = os.path.join(os.environ.get('GPHOME_LOADERS'), 'greenplum_loaders_path.sh')
        elif os.environ.get('GPHOME'):
            srcfile = os.path.join(os.environ.get('GPHOME'), 'greenplum_path.sh')
        if not (srcfile and os.path.exists(srcfile)):
            self.log('cannot find greenplum environment ' + 'file: environment misconfigured', self.INFO)
            exit(1)

        source_cmd = 'source %s ; exec ' % srcfile

        if counter > 1:

            port_lst = []
            for p in range(counter):
                port_lst.append(str(8081 + p + 1))
            # save port
            self.GPFDIST_PORT = port_lst

            for p in port_lst:
                cmd = source_cmd + "gpfdist -w 300 -t 300 " + ' -d ' + path
                cmd = cmd + ' -p ' + p + ' '
                log_out = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                           close_fds=True)
                # save gpfdist pid
                self.GPFDIST_PID.append(log_out.pid)
                print log_out.stderr.read()
        else:
            cmd = source_cmd + "gpfdist -w 300 -t 300 -d {0} -p {1} ".format(path, "8081")
            log_out = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
            # save gpfdist pid
            self.GPFDIST_PID.append(log_out.pid)
            print log_out.stderr.read()

    def stop_gpfdist(self):
        if self.GPFDIST_PID:
            print "kill gpfdist"
            for p in self.GPFDIST_PID:
                os.kill(p, signal.SIGKILL)

    def get_all_sql_command_by_port(self):
        """
        Only used in version 1, the new version will no longer be enabled
        :return:
        """
        tmp = splice_dict(len(self.GPFDIST_PORT), self.TABLE_OF_LIST)
        for port in self.GPFDIST_PORT:
            [ddl_of_external, sql_of_insert] = OrderedDict(), OrderedDict()
            if op.ACTION:
                ddl_of_external, sql_of_insert = self.generate_import_sql(tmp[op.GPFDIST_PORT.index(port)], str(port))
            else:
                ddl_of_external, sql_of_insert = self.generate_export_sql(tmp[op.GPFDIST_PORT.index(port)], str(port))

            self.DDL_OF_EXTERNAL = dict(self.DDL_OF_EXTERNAL, **ddl_of_external)
            self.SQL_OF_INSERT = dict(self.SQL_OF_INSERT, **sql_of_insert)

    def generate_table_file_name(self, schema, table, oid):
        """

        :param schema:
        :param table:
        :param oid:
        :return:
        """

        if self.TIME_DIR:
            file_name = schema + '__' + table + '__' + str(oid) + '__' + str(self.TIME_DIR) + '.dat'
        else:
            file_name = schema + '__' + table + '__' + str(oid) + '.dat'

        return file_name

    def generate_export_sql(self, table_list, gpfdist_port='8081'):
        ddl_of_external = OrderedDict()
        sql_of_insert = OrderedDict()

        for t in table_list:
            CRE_CMD = "create writable external table "
            INS_CMD = "insert into "
            line = t.split(',')

            table_name = line[0].split('.')[-1]
            schema_name = line[0].split('.')[0]
            oid = self.table_check(table=table_name, schema=schema_name)

            ext_tab = op.EXTERNL_OF_SCHEMA + '.' + table_name + '_' + oid + '_w'

            self.TABLE_OF_EXT[line[0]] = ext_tab

            file_name = self.generate_table_file_name(schema_name, table_name, oid)
            CRE_CMD = '{0}{1}{2})'.format(' drop external table if exists {0};'.format(ext_tab), CRE_CMD, '{1}(like {0}'.format(line[0], ext_tab))
            CRE_CMD = "{0}{1}".format(CRE_CMD, " location ('gpfdist://{1}:{2}/{3}/{0}') format 'text' (delimiter E'\033');".format(file_name, self.HOST_OF_GPFDIST, gpfdist_port, self.TIME_DIR))

            ddl_of_external[line[0]] = CRE_CMD

            if len(line) > 1:
                INS_CMD = INS_CMD + ext_tab
                INS_CMD = INS_CMD + " select * from {0} {1};".format(line[0], line[1])
                sql_of_insert[line[0]] = INS_CMD

            else:
                INS_CMD = INS_CMD + ext_tab
                INS_CMD = INS_CMD + " select * from {0};".format(line[0])
                sql_of_insert[line[0]] = INS_CMD
        return ddl_of_external, sql_of_insert

    def generate_import_sql(self, table_list, gpfdist_port='8081'):
        ddl_of_external = OrderedDict()
        sql_of_insert = OrderedDict()

        for t in table_list:
            CRE_CMD = "create readable external table "
            INS_CMD = "insert into "
            line = t.split(',')

            table_name = line[0].split('.')[-1]
            schema_name = line[0].split('.')[0]
            oid = self.table_check(table=table_name, schema=schema_name)

            trun_cmd = "truncate {0};".format(line[0])

            ext_tab = op.EXTERNL_OF_SCHEMA + '.' + table_name + '_' + oid + '_r'

            self.TABLE_OF_EXT[line[0]] = ext_tab
            file_name = self.generate_table_file_name(schema_name, table_name, oid)

            CRE_CMD = '{0}{1}{2})'.format(' drop external table if exists {0};'.format(ext_tab), CRE_CMD, '{1}(like {0}'.format(line[0], ext_tab))
            CRE_CMD = "{0}{1}".format(CRE_CMD, " location ('gpfdist://{1}:{2}/{3}/{0}.dat') format 'text' (delimiter E'\033');".format(file_name, self.HOST_OF_GPFDIST, gpfdist_port, self.TIME_DIR))

            ddl_of_external[line[0]] = CRE_CMD

            if len(line) > 1:
                INS_CMD = trun_cmd + INS_CMD + line[0]
                INS_CMD = INS_CMD + " select * from {0} {1};".format(ext_tab, line[1])
                sql_of_insert[line[0]] = INS_CMD

            else:
                INS_CMD = trun_cmd + INS_CMD + line[0]
                INS_CMD = INS_CMD + " select * from {0};".format(ext_tab)
                sql_of_insert[line[0]] = INS_CMD
        return ddl_of_external, sql_of_insert

    def generate_export_sql2(self, schema, table, gpfdist_port='8081'):
        """
        export data from database to file.
        :param schema:
        :param table:
        :param gpfdist_port: gpfdist server port
        :return:
        """
        create_of_sql = None
        insert_of_sql = None

        CRE_CMD = "create writable external table "
        INS_CMD = "insert into "

        table_name = table
        schema_name = schema
        oid = self.table_check(table = table_name, schema = schema_name)

        ext_tab = op.EXTERNL_OF_SCHEMA + '.' + table_name + '_' + oid + '_w'

        self.TABLE_OF_EXT[table_name] = ext_tab

        file_name = self.generate_table_file_name(schema_name, table_name, oid)
        CRE_CMD = '{0}{1}{2})'.format(' drop external table if exists {0};'.format(ext_tab), CRE_CMD, '{1}(like {0}'.format(schema_name + '.' + table_name, ext_tab))
        CRE_CMD = "{0}{1}".format(CRE_CMD, " location ('gpfdist://{1}:{2}/{3}/{0}') format 'text' (delimiter E'\033');".format(file_name, self.HOST_OF_GPFDIST, gpfdist_port, self.TIME_DIR))

        create_of_sql = CRE_CMD

        INS_CMD = INS_CMD + ext_tab
        INS_CMD = INS_CMD + " select * from {0};".format(table_name)  # format(schema_name + '.' + table_name)
        insert_of_sql = INS_CMD
        return create_of_sql, insert_of_sql

    def generate_import_sql2(self, schema, table, gpfdist_port='8081'):
        """
        import data from files to database.
        generate INSERT and CREATE EXTERNAL TABLE sql command
        :param schema:
        :param table:
        :param gpfdist_port:gpfdist server port
        :return:
        """
        create_of_sql = None
        insert_of_sql = None

        CRE_CMD = "create readable external table "
        INS_CMD = "insert into "

        table_name = table
        schema_name = schema
        oid = self.table_check(table = table_name, schema = schema_name)

        trun_cmd = "truncate {0};".format(schema_name + '.' + table_name)

        ext_tab = op.EXTERNL_OF_SCHEMA + '.' + table_name + '_' + oid + '_r'

        self.TABLE_OF_EXT[table_name] = ext_tab
        file_name = self.generate_table_file_name(schema_name, table_name, oid)

        CRE_CMD = '{0}{1}{2})'.format(' drop external table if exists {0};'.format(ext_tab), CRE_CMD, '{1}(like {0}'.format(schema_name + '.' + table_name, ext_tab))
        CRE_CMD = "{0}{1}".format(CRE_CMD, " location ('gpfdist://{1}:{2}/{3}/{0}') format 'text' (delimiter E'\033');".format(file_name, self.HOST_OF_GPFDIST, gpfdist_port, self.TIME_DIR))

        create_of_sql = CRE_CMD

        INS_CMD = trun_cmd + INS_CMD + schema_name + '.' + table_name
        INS_CMD = INS_CMD + " select * from {0};".format(ext_tab)
        insert_of_sql = INS_CMD
        return create_of_sql, insert_of_sql

    def start_transfer_data(self, sql_dict, work_id):
        """

        :param sql_dict:key is table name (schema.table); values is a list. it can by modified
        :param work_id:thread number
        :return: None
        """
        cmd_out_ins = ''
        # error_code_ins = 0
        for key, values in sql_dict.items():

            schema_name = key.split('.')[0]
            table_name = key.split('.')[1]

            st_ins = get_time()
            self.log(values[0])
            cmd_result_ins, error_code_ins = exe_query(values[0])
            # print "error_code_ins" + str(error_code_ins)
            ed_ins = get_time()
            cmd_out_ins = ''.join(list(itertools.chain.from_iterable(cmd_result_ins)))
            # print cmd_out_ins

            if error_code_ins == 1:
                if op.ACTION:
                    messages = '{5}|{2}|{3}|{1}/{6} (Current/Total)|{4} --> {0:<55s}|'.format(key, values[1], st_ins, ed_ins, self.get_ext_table_name(schema_name, table_name), work_id, self.table_count) + cmd_out_ins + '\n'
                else:
                    messages = '{5}|{2}|{3}|{1}/{6} (Current/Total)|{0} --> {4:<55s}|'.format(key, values[1], st_ins, ed_ins, self.get_ext_table_name(schema_name, table_name), work_id, self.table_count) + cmd_out_ins + '\n'

                self.log(work_id + '|' + key + " have failed: " + messages.strip(), self.ERROR)
                # self.log("start_transfer_data function error: " + messages.strip(), self.ERROR)
            elif error_code_ins == 0:
                if op.ACTION:
                    messages = '{5}|{2}|{3}|{1}/{6} (Current/Total)|{4} --> {0:<55s}|'.format(key, values[1], st_ins, ed_ins, self.get_ext_table_name(schema_name, table_name), work_id, self.table_count) + cmd_out_ins + '\n'
                else:
                    messages = '{5}|{2}|{3}|{1}/{6} (Current/Total)|{0} --> {4:<55s}|'.format(key, values[1], st_ins, ed_ins, self.get_ext_table_name(schema_name, table_name), work_id, self.table_count) + cmd_out_ins + '\n'

                self.log(messages.strip(), self.INFO)

                self.touch_ok_file(schema_name, table_name, values[-1])
                self.log(work_id + '|' + key + " have completed", self.INFO)

    def start_transfer_data2(self, sql_dict, work_id):
        cmd_out_cre = ''
        cmd_out_ins = ''
        num = 0
        for key, values in sql_dict.items():

            schema_name = key.split('.')[0]
            table_name = key.split('.')[1]
            # oid = self.table_check(table=table_name, schema=schema_name)

            try:
                # if self.DIR_OF_GPFDIST:
                #     self.touch_data_file(schema_name, table_name)
                # self.log(self.DDL_OF_EXTERNAL[key], self.DEBUG)
                # cmd_result_cre = exe_query2(self.DDL_OF_EXTERNAL[key])

                # self.log(values, self.DEBUG)
                st_ins = get_time()
                cmd_result_ins, error_code_ins = exe_query(values)
                ed_ins = get_time()
                cmd_out_ins = ''.join(list(itertools.chain.from_iterable(cmd_result_ins)))

            except Exception, e:
                self.log(e.message + cmd_out_cre + cmd_out_cre, self.ERROR)
                continue

            # if cmd_result_cre != 0:
            #     self.log(work_id + cmd_result_cre, self.ERROR)
            #     continue
            else:
                pass

            if error_code_ins == 1:
                if op.ACTION:
                    messages = '{5}|{2}|{3}|{1:<8d}|{4} --> {0:<55s}|'.format(key, num, st_ins, ed_ins, self.TABLE_OF_EXT[key], work_id) + cmd_out_ins + '\n'
                else:
                    messages = '{5}|{2}|{3}|{1:<8d}|{0} --> {4:<55s}|'.format(key, num, st_ins, ed_ins, self.TABLE_OF_EXT[key], work_id) + cmd_out_ins + '\n'

                self.log(messages.strip(), self.ERROR)
                self.log(work_id + '|' + key + " have failed", self.INFO)
            elif error_code_ins == 0:
                if op.ACTION:
                    messages = '{5}|{2}|{3}|{1:<8d}|{4} --> {0:<55s}|'.format(key, num, st_ins, ed_ins, self.TABLE_OF_EXT[key], work_id) + cmd_out_ins + '\n'
                else:
                    messages = '{5}|{2}|{3}|{1:<8d}|{0} --> {4:<55s}|'.format(key, num, st_ins, ed_ins, self.TABLE_OF_EXT[key], work_id) + cmd_out_ins + '\n'

                self.log(messages.strip(), self.INFO)

                # self.touch_ok_file(schema_name, table_name)
                self.log(work_id + '|' + key + " have completed", self.INFO)

    def cre_ext_schema(self):
        list_of_schema = []
        cmd_result_list, error_code = exe_query(self.SQL_OF_SCHEMA)
        for row in cmd_result_list:
            list_of_schema.append(row[-1])
        if self.EXTERNL_OF_SCHEMA in list_of_schema:
            message = "{0} schema already exists! will not create! ".format(self.EXTERNL_OF_SCHEMA)
            self.log(message, self.INFO)
        else:
            cmd = "create schema {0};".format(self.EXTERNL_OF_SCHEMA)
            cmd_result = exe_query2(cmd)
            cmd_out = ' '.join(list(itertools.chain.from_iterable(str(cmd_result))))
            if cmd_result != 0:
                self.log(cmd_out, self.ERROR)

    def read_table_file(self):
        self.log("start read table file list", self.INFO)
        with open(self.TABLE_OF_PATH) as fh:
            temp = fh.readlines()

        for i in temp:
            cell = i.strip()
            if cell != '':
                self.TABLE_OF_LIST.append(cell)

        self.table_count = len(self.TABLE_OF_LIST)

    def get_table_from_database(self):
        if self.TABLE_OF_SCHEMA == '':
            sql_of_table = self.SQL_OF_TABLE
        else:
            sql_of_table = self.SQL_OF_TABLE + "and upper(n.nspname) = upper('{0}');".format(self.TABLE_OF_SCHEMA)
        cmd_result, error_code = exe_query(sql_of_table)

        if error_code == 1:
            Dataload().log(cmd_result, self.ERROR)
            exit(1)

        for row in cmd_result:
            self.TABLE_OF_LIST.append(row[1] + '.' + row[2])

        self.table_count = len(self.TABLE_OF_LIST)

    def touch_data_file(self, schema, table, path):
        oid = self.table_check(table=table, schema=schema)
        file_name = self.generate_table_file_name(schema, table, oid)
        touch_cmd = 'touch ' + path + '/' + file_name.split('.')[0] + '.dat'
        sc = subprocess.Popen(touch_cmd, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE, universal_newlines=True)
        out, err = sc.communicate()
        if err:
            self.log(file_name + '  touch faild message:' + err, self.ERROR)
        elif out:
            pass

    def touch_ok_file(self, schema, table, path):
        oid = self.table_check(table=table, schema=schema)
        file_name = self.generate_table_file_name(schema, table, oid)
        touch_cmd = 'touch ' + path + '/' + file_name.split('.')[0] + '.ok'
        sc = subprocess.Popen(touch_cmd, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE, universal_newlines=True)
        out, err = sc.communicate()
        if err:
            self.log(file_name + '  touch faild message:' + err, self.ERROR)
        elif out:
            pass

    def get_ok_file(self):

        files = None
        try:
            files = os.listdir(self.DIR_OF_GPFDIST)
        except Exception as e:
            self.log(self.DIR_OF_GPFDIST + 'directory not found ' + str(e), self.ERROR)

        re_obj = re.compile('^\w+__.+__\d+\.ok$')

        for f in files:
            if re_obj.search(f):
                # self.log(f + " is ok file ", self.DEBUG)
                continue
            else:
                # self.log(f + " file will be remove ", self.DEBUG)
                files.remove(f)
        return files

    def get_of_ddl(self, schema, table):
        """
        plan to transfer data manager language
        :param schema:
        :param table:
        :return:
        """

        pg_dump_cmd = """pg_dump --gp-syntax -h 192.168.72.30 -p 5432 -U gpadmin -s -t u_liu.bill_type_code report"""
        # self.table_check(schema, table)
        tablename = schema + '.' + table
        cmd = self.GET_OF_DDL.format(self.PGHOST, self.PGUSER, tablename, self.DATABASE_NAME)
        tmp = subprocess.Popen(pg_dump_cmd, shell=True, stdout=subprocess.PIPE)

    def parse_para(self):

        port = []
        directory = []

        tmp = self.gpfdist.split(',')

        for g in tmp:
            port.append(g.split(":")[0])
            directory.append(g.split(":")[-1])
        return {'port': port, 'directory': directory}

    def get_ext_table_name(self, schema_name, table_name):
        if self.ACTION:
            oid = self.table_check(table = table_name, schema = schema_name)

            ext_tab = op.EXTERNL_OF_SCHEMA + '.' + table_name + '_' + oid + '_r'
        else:
            oid = self.table_check(table = table_name, schema = schema_name)

            ext_tab = op.EXTERNL_OF_SCHEMA + '.' + table_name + '_' + oid + '_w'
        return ext_tab

    def do_load(self, parallel):
        threads = []
        for t in range(parallel):
            work = Work(t, "thread-{0}".format(t), q=self.work_queue)
            threads.append(work)
            work.setDaemon(True)
            work.start()

        # insert sql
        count = 1
        for k, v in self.SQL_OF_INSERT.items():
            tmp = {k: [v, count]}
            count = count + 1
            self.work_queue.put(tmp)

        self.work_queue.join()

        self.log("======>All task done ; Exiting do_methed Thread.<======", self.INFO)

    def do_load2(self):
        """
        version 2
        :param parallel: Multi-task parallel execution
        :return:
        """
        tmp_d = self.parse_para()
        port, directory = tmp_d['port'], tmp_d['directory']

        gpfdist_port_len = len(port)  # The gpfdist port is allocated through a loop by length

        parallel = self.PARALLEL
        self.log("parallel" + str(parallel))

        # Start multiple threads and wait for the elements in the queue to be processed
        # threads = []
        for t in range(parallel):
            work = Work(t, "Work-{0}".format(t), q = self.work_queue)
            # threads.append(work)
            work.setDaemon(True)
            work.start()

        num = 0  # gpfdist counter
        count = 1  # elements counter

        # Put the pending elements into the queue
        for t in self.TABLE_OF_LIST:
            schema = t.split('.')[0]
            table = t.split('.')[-1]

            if self.ACTION:
                create_table_sql, insert_table_sql = self.generate_import_sql2(schema, table, port[num])
            else:
                create_table_sql, insert_table_sql = self.generate_export_sql2(schema, table, port[num])

            out = exe_query2(create_table_sql)
            # self.log(create_table_sql, self.INFO)

            if out != 0:
                self.log("create table failed", self.ERROR)

            self.touch_data_file(schema, table, directory[num])

            # The gpfdist port is allocated through a loop by length
            num = num + 1
            if num > gpfdist_port_len - 1:
                num = 0

            tmp = {t: [insert_table_sql, count, directory[num]]}
            count = count + 1

            self.work_queue.put(tmp)

        self.work_queue.join()

        self.log("======>All task done ; Exiting do_methed Thread.<======", self.INFO)

    def run(self):
        """
        version 1
        :return:
        """
        # get_args(sys.argv[1:])
        if self.TABLE_OF_SCHEMA is None:
            self.read_table_file()
        else:
            self.get_table_from_database()
        self.cre_ext_schema()
        # self.start_gpfdist(self.DATA_PATH, self.PARALLEL)
        self.get_all_sql_command_by_port()

        self.do_load(self.PARALLEL)
        # self.stop_gpfdist()

    def run2(self):
        """
        version 2
        :return:
        """
        if self.ACTION:
            self.log("import data model")
        else:
            self.log("export data model")

        get_args(sys.argv[1:])
        # print self.PARALLEL

        if self.TABLE_OF_SCHEMA is None:
            self.read_table_file()
        else:
            self.get_table_from_database()

        self.cre_ext_schema()
        self.do_load2()
        # files = self.get_ok_file()
        #
        # for okfile in files:
        #     print okfile


def set_test_para():
    """
    Shield functions to pre-set the necessary input parameters
    :return:
    """
    op.DATABASE_NAME = 'report'
    op.PGUSER = 'gpadmin'
    op.PGHOST = '192.168.72.30'
    op.PGPORT = '5432'
    op.HOST_OF_GPFDIST = '192.168.72.30'
    op.PARALLEL = 1
    op.DIR_OF_GPFDIST = '/tmp'
    op.ACTION = True
    op.TABLE_OF_PATH = '/home/zch/tmp/ods.list'
    # op.TABLE_OF_SCHEMA = 'backup_data_circ'
    op.gpfdist = '8080:/tmp,8081:/tmp,8083:/tmp,8084:/tmp'


if __name__ == "__main__":
    op = Dataload()
    set_test_para()
    # op.run()
    op.run2()
    # print op.gpfdist
    # print op.GPFDIST
    # op.table_check(table='copy_all')
    # op.run2()
