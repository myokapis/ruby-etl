require 'dbf'
require 'yaml'
require_relative 'file_extensions'
require_relative 'sql_bulk_copy'
require_relative 'sql_connection'
require_relative 'sql_script'

# example code that utilizes the RubyETL code base
# scans directories for Foxpro files, parses them, and writes each of them to a database table
# code is provided for an example only
class DBFImport

  def initialize
    @config = {}
    @sources = {}
    @targets = {}
    @transforms = {}
    @imports = []
    @working_dir = nil
    @error_dir = nil
    
    load_config
  end
  
  # import each file, create a table based on the file structure, and import the file data into the table
  def import
    @imports.each do |item|
      source = @sources[item['source']] || {}
      source_dir = source['path']
      source_files = item['files'] || []
      target = @targets[item['target']] || {}
      table_name = target['table']
      
      file_list = build_file_list(source_dir, source_files)
      transform = @transforms[item['transform']] || {}
      
      file_list.each do |file_name|
        columns = parse_file(file_name, source_dir, transform)
        
        create_table(target, columns)
        
        data_file_name = File.basename(file_name, '.*') + '.dat'
        data_file_path = File.full_path(@working_dir, data_file_name)
        error_file_name = File.basename(file_name, '.*') + '.err'
        error_file_path = File.full_path(@error_dir, error_file_name)
        
        bcp_file(table_name, data_file_path, error_file_path, target)
      end
    end
  end
  
  protected
  
  # bulk copy a file to a database table
  def bcp_file(table_name, data_file_path, error_file_path, connection_hash)
    bulk_copy = SQLBulkCopy.new(table_name, data_file_path, error_file_path, connection_hash, {c: nil})
    bulk_copy.execute
  end
  
  # return a list of files to be processed
  def build_file_list(source_dir, source_files)
    files = []
    
    source_files.each do |source_file|
      [get_files(source_dir, source_file)].flatten.compact.each do |file|
        files.push(file) if !files.include?(file)
      end
    end
    
    return files
  end
  
  # creates a database table based on a Foxpro file format
  def create_table(target, file_name, columns)
    table_prefix = target['prefix'] || ''
    table_name = target['table'] || "#{table_prefix}#{File.basename(file_name, '.*')}"
    column_sql = []
    
    sql =
    [
      "USE #{target['database']};",
      "IF OBJECT_ID('dbo.#{table_name}') IS NOT NULL DROP TABLE dbo.#{table_name}",
      "BEGIN",
      "CREATE TABLE dbo.#{table_name}",
      "("
    ]
    
    columns.each do |column|
      column_sql.push("#{column.name} #{get_data_type(column.type_name, column.length)} NULL")
    end
    
    sql.push(column_sql.join(',\r\n')
    sql.push(');')
    sql.push('END')
    
    sql_connection = SQLConnection.new(target)
    sql_query = SQLQuery.new(sql.join('\r\n'), sql_connection)
    sql_query.execute
  end
  
  # returns the data type of each file field
  # dummy method (for now)
  def get_data_type(type_name, length)
    data_type =
      case type_name
        when  then 
        else "varchar(255)"
      end
      
    return data_type
  end

  # get a list of files in a directory that match the file name pattern
  def get_files(source_dir, file_name)
    file_list = []
    
    if file_name.to_s.index('*')
      Dir.chdir(source_dir) do
        file_list = Dir.glob(file_name) || []
      end
    else
      file_list.push(file_name)
    end
    
    file_list.each do |file|
      file_path = File.full_path(source_dir, file)
      file_list.delete(file) if !File.exists?(file_path)
    end
    
    return file_list
  end
  
  # read the configuration file
  def load_config
    config_path = File.full_path(File.dirname(__FILE__), '../Config/config.yml')

    File.open(config_path, 'r') do |f_in|
      @config = YAML::load(f_in)
    end
    
    @sources = @config['sources'] || {}
    @targets = @config['targets'] || {}
    @transforms = @config['transforms'] || {}
    @imports = @config['imports'] || []
    @working_dir = @config['working_dir']
    @error_dir = @config['error_dir']
  end
  
  # parse a Foxpro file, transforms the data, outputs the data to a file, and returns a list of columns
  def parse_file(file_name, source_dir, transform)
    source_path = File.full_path(source_dir, file_name)
    data_file_name = File.basename(file_name, '.*') + '.dat'
    data_file_path = File.full_path(@working_dir, data_file_name)
    out = []
    columns = []
    
    table = DBF::Table.new(source_path)

    if table.respond_to?(:record)
      columns = table.columns
    
      File.open(data_file_path, 'w') do |f_out|
        table.each do |record|
          out = []
          
          transform.each do |field_sql, field_dbf|
            out.push(record.attributes[field_dbf])
          end
          
          f_out.puts out.join('|') if out.compact.length > 0
        end

      end
    end
    
    return columns
  end

end

if $0 == __FILE__
  dbf_import = DBFImport.new
  dbf_import.import
end
