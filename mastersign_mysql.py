# _*_ coding: utf-8 _*_

"""
Helper function for connecting to a MySQL database
while loading the connection informations from a
``mastersign_config.Configuration`` object
"""

import sys
import os
import subprocess
import re
import tempfile
import pymysql.cursors


class OutputStream:
    def __init__(self, f):
        if isinstance(f, str):
            self._path = f
            self.file = None
        else:
            self._path = None
            self.file = f

    def __enter__(self):
        if self._path:
            self.file = open(self._path, 'a')
        return self

    def __exit__(self, *args):
        if self._path:
            self.file.close()
            self.file = None


class TempFile:
    def __enter__(self):
        try:
            cfd, self.path = tempfile.mkstemp(text=True)
        finally:
            os.close(cfd)
        return self

    def __exit__(self, *args):
        if self.path and os.path.exists(self.path):
            os.remove(self.path)
            self.path = None


def split_host(host):
    port = 3306
    host = host.split(':', 1)
    if len(host) == 2:
        host, port = host
    else:
        host = host[0]
    return host, int(port)


def write_client_config(cfg, host_cfg_name, file):
    host, port = split_host(cfg.str('database.' + host_cfg_name, 'host'))
    close_file = False
    if isinstance(file, str):
        file = open(file, 'w')
        close_file = True
    try:
        file.writelines(l + '\n' for l in [
            '[client]',
            'host = ' + host,
            'port = ' + str(port),
            'user = ' + cfg.str('database.' + host_cfg_name, 'user') or 'root',
            'password = "' + (cfg.str('database.' + host_cfg_name, 'password') or '') + '"'
        ])
    finally:
        if close_file:
            file.close()


def _execute_sql_script(cfg, host_cfg_name,
                        script_file=None, script_text=None,
                        log=sys.stdout, logerr=sys.stderr,
                        client_command='mysql', use_database=True, default_charset='utf8mb4'):
    # script_file takes priority over script_text

    with OutputStream(log) as s_std, \
         OutputStream(logerr) as s_err, \
         TempFile() as tmp_file:

        proc = None
        write_client_config(cfg, host_cfg_name, tmp_file.path)
        args = [
            client_command,
            '--defaults-extra-file=' + tmp_file.path,
            '--default-character-set=' + default_charset,
        ]
        if use_database:
            args.append(cfg.str('database.' + host_cfg_name, 'schema'))
        if script_file is not None:
            with open(script_file, 'br') as sfd:
                proc = subprocess.run(args, stdin=sfd,
                                      stdout=s_std.file, stderr=s_err.file)
        else:
            script_data = bytearray(script_text, 'utf-8')
            proc = subprocess.run(args, input=script_data,
                                        stdout=s_std.file, stderr=s_err.file)
        return proc.returncode == 0 if proc else False


def execute_sql_file(cfg, host_cfg_name, script_file,
                     log=sys.stdout, logerr=sys.stderr,
                     client_command='mysql', use_database=True, default_charset='utf8mb4'):

    return _execute_sql_script(cfg, host_cfg_name, script_file=script_file,
                     log=log, logerr=logerr,
                     client_command=client_command,
                     use_database=use_database,
                     default_charset=default_charset)


def execute_sql(cfg, host_cfg_name, sql,
                log=sys.stdout, logerr=sys.stderr,
                client_command='mysql', use_database=True, default_charset='utf8mb4'):

    return _execute_sql_script(cfg, host_cfg_name, script_text=sql,
                     log=log, logerr=logerr,
                     client_command=client_command,
                     use_database=use_database,
                     default_charset=default_charset)


def _get_mysqldump_version(command):
    version_text = subprocess.check_output((command, '--version'), timeout=10, encoding='utf-8')
    m8 = re.search(r"Ver (8\.[\d\.]+)", version_text)
    if m8:
        return m8[1]
    m5 = re.search(r"Distrib (5\.[\d\.]+)", version_text)
    if m5:
        return m5[1]
    return None


def mirror(cfg, src_cfg_name, trg_cfg_name,
           src_schema, trg_schema, table_name=None,
           export_command='mysqldump', import_command='mysql',
           drop_table=True,
           add_locks=True, quick=True, single_transaction=True,
           buffer_length=1048576,
           log=sys.stdout, logerr=sys.stderr):
    with OutputStream(log) as s_std, \
         OutputStream(logerr) as s_err, \
         TempFile() as src_cfg_file, \
         TempFile() as trg_cfg_file:

        write_client_config(cfg, src_cfg_name, src_cfg_file.path)
        write_client_config(cfg, trg_cfg_name, trg_cfg_file.path)
        export_args = [
            export_command,
            '--defaults-extra-file=' + src_cfg_file.path,
            '--default-character-set=utf8mb4',
            '--net_buffer_length=' + str(buffer_length),
        ]
        mysqldump_version = _get_mysqldump_version(export_command)
        if mysqldump_version and mysqldump_version[:2] == '8.':
            export_args.append('--column-statistics=0')
        if drop_table:
            export_args.append('--add-drop-table')
        else:
            export_args.append('--skip-add-drop-table')
        if add_locks:
            export_args.append('--add-locks')
        else:
            export_args.append('--skip-add-locks')
        if quick:
            export_args.append('--quick')
        else:
            export_args.append('--skip-quick')
        if single_transaction:
            export_args.append('--skip-lock-tables')
            export_args.append('--single-transaction')
        if table_name:
            export_args.append(src_schema)
            export_args.append(table_name)
        else:
            export_args.append(src_schema)
        import_args = [
            import_command,
            '--defaults-extra-file=' + trg_cfg_file.path,
            '--default-character-set=utf8mb4',
            trg_schema,
        ]

        export_proc = subprocess.Popen(export_args, stdout=subprocess.PIPE)
        import_proc = subprocess.Popen(import_args, stdin=export_proc.stdout,
                                       stdout=s_std.file, stderr=s_err.file)
        export_proc.stdout.close()
        try:
            status = import_proc.wait()
            if export_proc.poll() is None:
                export_proc.terminate()
        except KeyboardInterrupt as e:
            export_proc.terminate()
            import_proc.terminate()
            raise e
        return status == 0


def connect(cfg, host_cfg_name):
    host, port = split_host(cfg.str('database.' + host_cfg_name, 'host'))
    return pymysql.connect(
        host=host,
        port=port,
        user=cfg.str('database.' + host_cfg_name, 'user') or 'root',
        password=cfg.str('database.' + host_cfg_name, 'password'),
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor)
