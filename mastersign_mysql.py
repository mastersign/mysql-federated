# _*_ coding: utf-8 _*_

"""
Helper function for connecting to a MySQL database
while loading the connection informations from a
``mastersign_config.Configuration`` object
"""

import sys
import os
import subprocess
import tempfile
import pymysql.cursors


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


def execute_sql_file(cfg, host_cfg_name, script_file,
                     log=sys.stdout, logerr=sys.stderr,
                     client_command='mysql', default_charset='utf8mb4'):
    close_log = False
    if isinstance(log, str):
        log = open(log, 'a')
        close_log = True
    close_logerr = False
    proc = None
    tmp_file = None
    if isinstance(logerr, str):
        logerr = open(logerr, 'a')
        close_logerr = True
    try:
        cfd, tmp_file = tempfile.mkstemp()
    finally:
        os.close(cfd)
    write_client_config(cfg, host_cfg_name, tmp_file)
    try:
        with open(script_file, 'br') as sfd:
            proc = subprocess.run([
                client_command,
                '--defaults-extra-file=' + tmp_file,
                '--default-character-set=' + default_charset,
            ], stdin=sfd, stdout=log, stderr=logerr)
    finally:
        if close_log:
            log.close()
        if close_logerr:
            logerr.close()
    if tmp_file and os.path.exists(tmp_file):
        os.remove(tmp_file)
    if proc:
        return proc.returncode == 0
    else:
        return False


def execute_sql(cfg, host_cfg_name, sql,
                log=sys.stdout, logerr=sys.stderr,
                client_command='mysql', default_charset='utf8mb4'):
    close_log = False
    if isinstance(log, str):
        log = open(log, 'a')
        close_log = True
    close_logerr = False
    proc = None
    tmp_file = None
    if isinstance(logerr, str):
        logerr = open(logerr, 'a')
        close_logerr = True
    try:
        cfd, tmp_file = tempfile.mkstemp(text=True)
    finally:
        os.close(cfd)
    write_client_config(cfg, host_cfg_name, tmp_file)
    try:
        proc = subprocess.run([
            client_command,
            '--defaults-extra-file=' + tmp_file,
            '--default-character-set=' + default_charset,
        ], input=sql, stdout=log, stderr=logerr)
    finally:
        if close_log:
            log.close()
        if close_logerr:
            logerr.close()
    if tmp_file and os.path.exists(tmp_file):
        os.remove(tmp_file)
    if proc:
        return proc.returncode == 0
    else:
        return False


def connect(cfg, host_cfg_name):
    host, port = split_host(cfg.str('database.' + host_cfg_name, 'host'))
    return pymysql.connect(
        host=host,
        port=port,
        user=cfg.str('database.' + host_cfg_name, 'user') or 'root',
        password=cfg.str('database.' + host_cfg_name, 'password'),
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor)
