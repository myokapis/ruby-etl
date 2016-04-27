Gem::Specification.new do |s|
  s.name = 'ruby-etl'
  s.summary = 'ETL functionality for SQL Server'
  s.description = 'Provides Ruby classes for working with data in files and SQL Server databases'
  s.version = '1.0.0'
  s.date = '2016-04-22'
  s.license = 'BSD 2'
  s.author = 'Gene Graves'
  s.email = 'gemdeveloper@myokapis.net'
  s.homepage = 'https://github.com/myokapis/ruby-etl'
  s.files = ['lib/ruby-etl.rb', 'lib/ruby-etl/sql_bulk_copy.rb', 'lib/ruby-etl/sql_connection.rb',
    'lib/ruby-etl/sql_ole.rb', 'lib/ruby-etl/sql_query.rb', 'lib/ruby-etl/sql_script.rb',
    'lib/extensions/file_extensions.rb', 'lib/extensions/hash_extensions.rb',
    'test/test_sql_bulk_copy.rb', 'test/test_sql_connection.rb', 'test/test_sql_ole.rb',
    'test/test_sql_query.rb', 'test/test_sql_script.rb', 'LICENSE', README.md]
end