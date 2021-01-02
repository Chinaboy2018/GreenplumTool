# coding=utf-8
import os
import sys
import signal
import subprocess
import datetime


class Option:
    """

    """
    def __init__(self):
        pass


class Cmd(object):
    """

    """

    def __init__(self):
        self.options = Option()
        self.DEBUG = 5
        self.LOG = 4
        self.INFO = 3
        self.WARN = 2
        self.ERROR = 1
        self.options.l = None
        self.options.qv = self.INFO

        self.filePath = None
        self.gpVersion = None
        self.cmd = None
        self.segDir = None
        help = """
-c 需要在节点目录执行bash命令，该参数必须被指定，否则程序报错退出；
-f 节点配置文件，可省略，不可和-v同时使用：
    可以通过如下SQL获取Greenplum5.x和Greenplum4.x的配置文件：
        SELECT conf.hostname||','||pgfse.fselocation
        FROM   pg_filespace_entry pgfse, gp_segment_configuration conf
        WHERE  pgfse.fsefsoid=3052 AND conf.dbid=pgfse.fsedbid
        ORDER BY conf.dbid;
    可以通过如下SQL获取Greenplum6.x的配置文件：
        SELECT conf.hostname||','||conf.datadir
        FROM   gp_segment_configuration conf
        ORDER BY conf.dbid;
-v Greenplum 版本信息，可省略，不可和-f同时使用
命令示例：
   python2 runcmdtosegdir.py 'ls -l postgresql.conf*'
   python2 runcmdtosegdir.py -c 'ls -l postgresql.conf*'
   python2 runcmdtosegdir.py -c 'ls -l postgresql.conf*' -f '/home/zch/tmp/segDir.conf'
   python2 runcmdtosegdir.py -c 'ls -l postgresql.conf*' -v 5
        """
        if self.options.l is None:
            self.options.l = os.path.join(os.environ.get('HOME', '.'), 'gpAdminLogs')
            if not os.path.isdir(self.options.l):
                os.mkdir(self.options.l)

            self.options.l = os.path.join(self.options.l, 'runCmdToSegDir_' +
                                          datetime.date.today().strftime('%Y%m%d') + '.log')

        try:
            self.logfile = open(self.options.l, 'a')
        except Exception, e:
            self.log("could not open logfile %s: %s" % (self.options.l, e), self.ERROR)

        self.arg = sys.argv[1:]
        # self.arg = ['ls -l postgresql.conf']
        if self.arg:
            pass
        else:
            self.log(help, self.ERROR)
            exit(1)
        if 2 >= len(self.arg) >= 1:
            if self.arg[0] != '-c':
                self.cmd = self.arg[0]
                self.gpVersion = self.getGpVersion()
                self.segDir = self.getSegDir()
            if self.arg[0] == '-c':
                self.cmd = self.arg[-1]
                self.gpVersion = self.getGpVersion()
                self.segDir = self.getSegDir()

        elif len(self.arg) > 2:
            parameters = self.arg[::2]
            values = self.arg[1::2]
            if all(word in parameters for word in ['-f', '-v']):  # 判断一个列表是否包含另外一个，返回true或者false
                self.log(help, self.ERROR)
                exit(1)

            if '-c' in parameters:
                self.cmd = values[parameters.index('-c')]
            else:
                self.log(help, self.ERROR)

            if '-v' in parameters:
                self.gpVersion = values[parameters.index('-v')]
                self.segDir = self.getSegDir()
            else:
                self.gpVersion = self.getGpVersion()
                self.segDir = self.getSegDir()

            if '-f' in parameters:
                self.filePath = values[parameters.index('-f')]
                self.segDir = self.openConfFile()
            else:
                self.segDir = self.getSegDir()
        else:
            self.log(help, self.ERROR)

    def openConfFile(self):
        tmp = []
        try:
            with open(self.filePath) as fd:
                fileList = fd.readlines()
        except Exception, e:
            self.log(e, self.ERROR)

        for l in fileList:
            tmp.append(l.strip())
        return tmp

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
            sys.exit(2)  # 多线程下直接退出会导致进程僵住
            pass

    def exe_query(self, sql_str):
        """
        通过psql命令执行传入的SQL，并处理数据库返回的结果。如果数据库执行成功则错误代码error_code为0，否则为1。并将标准错误输出
        :param sql_str:传入待执行的SQL命令
        :return:返回数据库执行结果和错误代码
        """
        lst_of_col = []
        error_code = 0
        CMD = "PGDATABASE=postgres PGPORT=5432 PGHOST=192.168.126.30 PGUSER=gpadmin PGOPTIONS='-c optimizer=off " \
              "-c client_encoding=UTF8' "
        CMD = CMD + "psql -R '\n' -tAXF '|+|' -v ON_ERROR_STOP=1  <<END_OF_SQL \n"
        CMD = CMD + sql_str + "\n"
        CMD = CMD + "END_OF_SQL"

        result = subprocess.Popen(CMD, shell = True, stdout = subprocess.PIPE, stderr = subprocess.PIPE,
                                  universal_newlines = True)

        out, err = result.communicate()

        if err != '':
            error_code = 1
            return err, error_code
        elif out != '':
            lst_of_line = out.strip().split('\n', -1)
            for line in lst_of_line:
                lst_of_col.append(line.split('|+|', -1))
            return lst_of_col, error_code

    def exeCmd(self, cmdStr):
        """
        :param cmdStr:传入待执行的命令
        :return:返回命令输出和作物代码
        """
        error_code = 0
        result = subprocess.Popen(cmdStr, shell = True, stdout = subprocess.PIPE, stderr = subprocess.PIPE,
                                  universal_newlines = True)

        out, err = result.communicate()
        if err != '':
            error_code = 1
            return err, error_code

        return out, error_code

    def getGpVersion(self):
        """
        :return: 返回Greenplum版本信息
        """

        getSql = """
                 select case when version() ~* '.*Greenplum Database 5.*' then '5' 
                 when version() ~* '.*Greenplum Database 4.*' then '4'
                 when version() ~* '.*Greenplum Database 6.*' then '6' end;
                 """
        lst_of_col, error_code = self.exe_query(getSql)
        if error_code == 1:
            self.log(str(lst_of_col), self.ERROR)
            exit()
        if lst_of_col[-1][-1] in ('5', '4'):
            return '5'
        else:
            return '6'

    def getSegDir(self):
        """
        :return: 返回Greenplum数据节点目录信息；数据结构为：segment host,segment data direceory
        """
        sqlStr = ''
        if self.gpVersion >= '6':
            sqlStr = """
                     SELECT conf.hostname||','||conf.datadir
                     FROM gp_segment_configuration conf
                     ORDER BY conf.dbid;
                     """
        else:
            sqlStr = """
                     SELECT conf.hostname||','||pgfse.fselocation
                     FROM pg_filespace_entry pgfse, gp_segment_configuration conf
                     WHERE pgfse.fsefsoid=3052 AND conf.dbid=pgfse.fsedbid
                     ORDER BY conf.dbid;
                     """
        segDirList, errCode = self.exe_query(sqlStr)
        if errCode == 1:
            self.log(str(segDirList), self.ERROR)
            exit()
        segDirList = [i for item in segDirList for i in item]  # 二维列表转一维列表
        return segDirList

    def runCmdBySSH(self):
        """
        :return:返回ssh命令在每个实例上的执行结果
        """
        sshCmdList = []
        for sl in self.segDir:
            tmp = []
            host, segdir = sl.split(',')
            sshStr = "ssh gpadmin@" + host + " 'cd {0};".format(segdir) + "{0}'".format(self.cmd)
            tmp.append(sl)
            tmp.append(sshStr)
            sshCmdList.append(tmp)

        for sc in sshCmdList:
            tmpOut = []
            out, errCode = self.exeCmd(sc[-1])
            out = out.strip()
            outList = out.split('\n')

            for o in outList:
                tmpOut.append(sc[0] + ' ==> ' + o)  # 对每条SSH输出进行标记
            for to in tmpOut:
                result = to + '\n'
                self.log(result.strip(), self.INFO)

            # self.log(sc[0] + ' ==> ' + out.strip(), self.INFO)


def run():
    c.runCmdBySSH()


if __name__ == "__main__":
    c = Cmd()
    run()
