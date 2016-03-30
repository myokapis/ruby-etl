require 'tempfile'
require_relative 'sql_connection'

# executes a sql script against a sql connection using osql
class SQLScript

  attr_reader :command, :last_error, :last_error_trace

  # initializes the script object
  # script_path - network path to the sql script to be executed
  # sql_connection - an instance of SQLConnection
  # options - (optional) list of osql command options
  #
  def initialize(script_path, sql_connection, options={})
    @script_path = script_path
    @sql_connection = sql_connection
    @last_error = nil
    @last_error_trace = nil

    build_options(options)
    get_temp_file
    build_command
  end

  # executes the osql command
  def execute()
    @last_error = nil
    @last_error_trace = nil
    return_value = nil
    clear_command_output

    begin
      @last_error_trace = ["command: #{@command}"]
      return_value = execute_shell(@command)
    rescue Exception => ex
      @last_error = ex.message
    ensure
      @last_error_trace.push(get_command_output) if !return_value
    end

    return return_value
  end

  protected

  # builds the osql command text
  def build_command
    # gets the connection hash from the SQLConnection object
    connection_hash = @sql_connection.connection_hash
    # builds the base osql command
    command = "osql -S #{connection_hash[:instance_name]} "

    # adds authentication
    if connection_hash[:trusted_connection]
      command << "-E "
    else
      command << "-U #{connection_hash[:user_id]} -P #{connection_hash[:password]} "
    end

    # adds a database if one is specified
    command << "-d #{connection_hash[:database]} " if connection_hash[:database]

    # adds each osql option
    @options.each do |key, val|
      option_value = [nil, '-'].include?(val[0]) ? val : " #{val}"
      command << "-#{key}#{option_value} "
    end

    # sets the input script path
    command << "-i \"#{@script_path}\""
    # redirects the osql output to a temp file
    command << " > \"#{@temp_file_path}\""

    @command = command
  end

  # builds a set of options for the bcp command
  def build_options(options)
    out = {}
    
    options.each do |key, val|
      out[key.to_sym] = val.nil? ? '' : val.to_s
    end

    @options = out
  end

  # clears the temp file by overwriting it
  def clear_command_output
    File.open(@temp_file_path, 'w') do |f_out|

    end
  end

  # helper method to execute a shell command
  def execute_shell(command)
    return Kernel.system(@command)
  end

  # reads the osql output from the temp file
  def get_command_output
    out = []
    File.open(@temp_file_path, 'r') do |f_in|
      f_in.each do |line|
        out.push(line.chomp)
      end
    end
    return out.join("\r\n")
  end

  # initializes a unique temp file
  def get_temp_file
    @temp_file = Tempfile.new(['command', '.out'])
    @temp_file.close
    @temp_file_path = File.expand_path(@temp_file.path).gsub('/', '\\')
  end

end

# this block is only executed if the script is run directly from the command line
if $0 == __FILE__
  # some example code
  hash = {instance_name: 'MySQLInstance', trusted_connection: true, persist_security_info: false}
  conn = SQLConnection.new(hash)
  path = 'somefilepath'
  script = SQLScript.new(path, conn, {n: nil})
  ret = script.execute
  puts script.last_error, script.last_error_trace, ret
end
