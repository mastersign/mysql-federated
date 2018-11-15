#!/usr/bin/env python3
# _*_ coding: utf-8 _*_

import os
import configparser
import argparse
import fnmatch
import pymysql.cursors


__version__ = '0.1.0'


def _option(v):
    if '=' not in v:
        raise argparse.ArgumentTypeError(
            '"{}" is not a valid configuration option.'.format(v))
    key, value = v.split('=', 1)
    if '.' not in key:
        raise argparse.ArgumentTypeError(
            'The key of the configuration option "{}" is invalid.'.format(v))
    section, name = key.split('.', 1)
    return section, name, value


class Configuration(object):

    def __init__(self, config_data):
        self.data = config_data

    def bool(self, section, name):
        return self.data[section].getboolean(name)

    def int(self, section, name):
        return self.data[section].getint(name)

    def float(self, section, name):
        return self.data[section].getfloat(name)

    def str(self, section, name):
        return self.data[section].get(name)

    def str_list(self, section, name):
        lst = self.str(section, name)
        return list(map(lambda v: v.strip(), lst.split(','))) if lst is not None else []

    @staticmethod
    def load(args, default_config_file=None):
        p = configparser.ConfigParser()
        if default_config_file and os.path.exists(default_config_file):
            p.read(default_config_file, encoding='utf-8')
        if args.config_files:
            p.read(args.config_files, encoding='utf-8')
        if args.options:
            for section, name, value in args.options:
                p.set(section, name, value)
        return Configuration(p)

    @staticmethod
    def add_config_arguments(parser):
        parser.add_argument('-c', '--config-file', dest='config_files', action='append', required=False,
                            help='A path to a configuration file in UTF-8 encoded INI format. '
                            'This argument can be used multiple times.')
        parser.add_argument('-o', '--options', nargs='+', type=_option, required=False,
                            help='One or more configuration options, given in the format <section>.<option>=<value>.')


def parse_args():
    parser = argparse.ArgumentParser(
        description='Link a database from one server to another, '
        'by creating federated tables.')
    parser.add_argument('-v', '--version', action='version', version=__version__,
                        help='print the program version and exit')
    parser.add_argument('target',
                        help='The name of the target database in the configuration. '
                        'This is the database to create the federated tables in.')
    parser.add_argument('remote',
                        help='The name of the remote database in the configuration. '
                        'This is the existing database, which actually stores the data.')

    Configuration.add_config_arguments(parser)
    return parser.parse_args()


def split_host(host):
    port = 3306
    host = host.split(':', 1)
    if len(host) == 2:
        host, port = host
    else:
        host = host[0]
    return host, int(port)


def connect(cfg, host_cfg_name):
    host, port = split_host(cfg.str('database.' + host_cfg_name, 'host'))
    return pymysql.connect(
        host=host,
        port=port,
        user=cfg.str('database.' + host_cfg_name, 'user') or 'root',
        password=cfg.str('database.' + host_cfg_name, 'password'),
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor)


def get_schemas(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('mysql','sys','information_schema','performance_schema')
            """)
        return list(map(lambda r: r['schema_name'], cur.fetchall()))


def get_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_name, table_schema
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
              AND table_schema NOT IN ('mysql','sys','information_schema','performance_schema')
            """)
        return cur.fetchall()


def get_create_table_statement(conn, table_name, table_schema):
    with conn.cursor() as cur:
        cur.execute("SHOW CREATE TABLE `{}`.`{}`".format(
            table_schema, table_name))
        return cur.fetchone()['Create Table']


def schemas(tables):
    return set(map(lambda t: t['table_schema'], tables))


def drop_schema(conn, name):
    print("dropping schema `{}`".format(name))
    with conn.cursor() as cur:
        cur.execute('DROP DATABASE IF EXISTS `{}`'.format(name))


def create_schema(conn, name):
    print("creating schema `{}`".format(name))
    with conn.cursor() as cur:
        cur.execute(
            'CREATE DATABASE `{}` DEFAULT CHARACTER SET utf8mb4'.format(name))


def filter_tables(tables, includes=None, excludes=None):
    def pred(name):
        if includes:
            if not any(map(lambda inc: fnmatch.fnmatch(name, inc), includes)):
                return False
        if excludes:
            if any(map(lambda exc: fnmatch.fnmatch(name, exc), excludes)):
                return False
        return True
    return list(filter(lambda t: pred(t['table_name']), tables))


def set_server(conn, cfg, remote_cfg_group, remote_schema, name):
    host, port = split_host(cfg.str(remote_cfg_group, 'host'))
    with conn.cursor() as cur:
        cur.execute("DROP SERVER IF EXISTS '{}'".format(name))
        print("updating server connection")
        cur.execute("""
            CREATE SERVER '{server}'
            FOREIGN DATA WRAPPER 'mysql'
            OPTIONS (HOST '{host}', PORT {port}, DATABASE '{db}', USER '{user}', PASSWORD '{passwd}')
            """.format(
            server=name,
            host=host,
            port=port,
            db=remote_schema,
            user=cfg.str(remote_cfg_group, 'user') or 'root',
            passwd=cfg.str(remote_cfg_group, 'password')))


def drop_table(conn, table_name, table_schema):
    print('dropping table `{}`.`{}`'.format(table_schema, table_name))
    with conn.cursor() as cur:
        cur.execute('DROP TABLE IF EXISTS `{}`.`{}`'.format(
            table_schema, table_name))


def create_federated_table(conn, create_stmt, table_name, table_schema, server_name):
    p = create_stmt.rindex(')')
    stmt = "{} ENGINE=FEDERATED DEFAULT CHARSET=utf8mb4 CONNECTION='{}'".format(create_stmt[:p+1], server_name)
    print('creating federated table `{}`'.format(table_name))
    with conn.cursor() as cur:
        cur.execute('USE `{}`'.format(table_schema))
        cur.execute(stmt)


def run():
    args = parse_args()
    config = Configuration.load(args)
    target_cfg_group = 'database.' + args.target
    remote_cfg_group = 'database.' + args.remote
    link_cfg_group = 'link.' + args.target + '.' + args.remote

    remote_schema = config.str(link_cfg_group, 'remote_schema') or config.str(
        remote_cfg_group, 'schema')
    if not remote_schema:
        raise Exception('No remote schema specified.')
    target_schema = config.str(link_cfg_group, 'target_schema') or config.str(
        target_cfg_group, 'schema')
    if not target_schema:
        raise Exception('No target schema specified.')

    remote_conn = connect(config, args.remote)
    try:
        remote_tables = get_tables(remote_conn)
        selected_tables = filter_tables(
            remote_tables,
            includes=config.str_list(link_cfg_group, 'include'),
            excludes=config.str_list(link_cfg_group, 'exclude'))
        print('selected tables')
        for rt in selected_tables:
            print('- ' + rt['table_name'])
            rt['create_statement'] = get_create_table_statement(
                remote_conn, **rt)
    finally:
        remote_conn.close()

    target_conn = connect(config, args.target)
    try:
        existing_schemas = get_schemas(target_conn)
        if target_schema in existing_schemas:
            if config.bool(link_cfg_group, 'drop_schema'):
                drop_schema(target_conn, target_schema)
                create_schema(target_conn, target_schema)
        else:
            create_schema(target_conn, target_schema)
        existing_table_names = set(
            map(lambda t: t['table_name'],
                filter(lambda t: t['table_schema'] == target_schema,
                       get_tables(target_conn))))

        set_server(target_conn, config, remote_cfg_group,
                   remote_schema, args.remote)
        for t in selected_tables:
            if t['table_name'] in existing_table_names:
                continue
            create_federated_table(target_conn, t['create_statement'], t['table_name'], target_schema, args.remote)

    finally:
        target_conn.close()


if __name__ == '__main__':
    run()
