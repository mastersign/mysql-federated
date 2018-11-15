# MySQL Federated

> Mirror tables from one MySQL server as federated tables into another MySQL server.

## Usage
Create federated tables on server _b_, which point to the data on server _a_.
For the `config.ini` see section _Example Configuration_.

~~~sh
link_mysql_database.py -c config.ini b a
~~~

## Example Configuration

The configuration contains the connection information for the servers
in individual sections of the INI file.
The server sections must be called `database.<name>`.
The name used here is not the actual hostname of the server,
but rather an alias inside the configuration.
There can be more than two server sections in the configuration.

The fields in a server section are:

* `host`: The IP address or hostname, optionally with a port number like `servername:3306`
* `schema`: (_optional_) The default schema on this server
* `user`: (_optional_) The username the login at the MySQL server (default is `root`)
* `password`: (_optional_) The password for the login at the MySQL server (default is empty)

There can be multiple sections to customize a link between two databases.
They must be called `link.<target-alias>.<remote-alias>`.
Both aliases point to a server described in a server section.

A link section supports the following fields:

* `include`: A comma separated list with tablename patterns to include in the linking
* `exclude`: A comma separated list with tablename patterns to exclude from the linking
* `target_schema`: Override for the schema on the target server
* `remote_schema`: Override for the schema on the remote server
* `drop_schema`: A yes/no flag for dropping the target schema before recreating it
  and the federated tables in it

`config.ini`

~~~ini
[database.a]
host = server-a
schema = source_db
user = root
password =

[database.b]
host = server-b:3306
schema = target_db
user = root
password =

[link.b.a]
include = abc_*
exclude = abc_*_x, abc_?y
drop_schema = false
~~~

## Help Text

~~~
usage: link_mysql_database.py [-h] [-v] [-c CONFIG_FILES]
                              [-o OPTIONS [OPTIONS ...]]
                              target remote

Link a database from one server to another, by creating federated tables.

positional arguments:
  target                The name of the target database in the configuration.
                        This is the database to create the federated tables
                        in.
  remote                The name of the remote database in the configuration.
                        This is the existing database, which actually stores
                        the data.

optional arguments:
  -h, --help            show this help message and exit
  -v, --version         print the program version and exit
  -c CONFIG_FILES, --config-file CONFIG_FILES
                        A path to a configuration file in UTF-8 encoded INI
                        format. This argument can be used multiple times.
  -o OPTIONS [OPTIONS ...], --options OPTIONS [OPTIONS ...]
                        One or more configuration options, given in the format
                        <section>.<option>=<value>.
~~~

## License

This project is published under the BSD-3-Clause license.
Copyright &copy; 2018 Tobias Kiertscher <dev@mastersign.de>.
