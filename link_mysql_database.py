#!/usr/bin/env python3
# _*_ coding: utf-8 _*_

import argparse
import fnmatch
from mastersign_config import Configuration
from mastersign_mysql import connect, split_host


__version__ = '0.1.6'


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


def run():
    args = parse_args()
    config = Configuration.load(args)
    remote_cfg_group = 'database.' + args.remote
    link_cfg_group = 'link.' + args.target + '.' + args.remote

    remote_schema = config.str(link_cfg_group, 'remote_schema') or config.str(
        remote_cfg_group, 'schema')
    if not remote_schema:
        raise Exception('No remote schema specified.')
    target_schema = config.str(link_cfg_group, 'target_schema') or config.str(
        remote_cfg_group, 'schema')
    if not target_schema:
        raise Exception('No target schema specified.')

    remote_conn = connect(config, args.remote)
    try:
        remote_tables = list(filter(
            lambda t: t['table_schema'] == remote_schema,
            get_tables(remote_conn)))
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
