# ruby-etl
Provides Ruby classes for working with data in files and SQL Server databases

## Classes

### SQLConnection
A class that provides a connection to a SQL Server database.
```ruby
  # get help for the SQLConnection class
  SQLConnection.help
```

### SQLQuery
A class for executing a SQL statement against a SQLConnection object. Exposes methods for getting the connection, sql_statement, query results, columns, last error, and error stack trace.

### SQLBulkCopy
A class for building and executing bcp commands.

### SQLScript
A class for executing a SQL script file against a SQLConnection object.

## Miscellaneous
The classes in this project have been unit tested and added to a gem. The checked in code is a placeholder while I locate/recreate the gem source and unit tests.
