require 'tempfile'
require_relative 'sql_connection'
require_relative 'hash_extensions'

# defines and executes a bcp command against a target connection
class SQLBulkCopy

  attr_reader :command, :last_error, :last_error_trace

  # setup the bcp command object
  #
  # db_object - the database target object
  # data_file_path - network path to the source data file
  # error_file_path - network path to the error output file (nil = no error file)
  # connection_hash - a hash of connection parameters
  # options - (optional) hash of bcp command options
  #
  def initialize(db_object, data_file_path, error_file_path, connection_hash, options={})
    @db_object = db_object
    @data_file_path = data_file_path
    @error_file_path = error_file_path
    @connection_hash = connection_hash
    @last_error = nil
    @last_error_trace = nil

    build_options(options)
    get_temp_file
    build_command
  end

  # execute the bcp command
  def execute()
    @last_error = nil
    @last_error_trace = nil
    return_value = nil
    clear_command_output

    begin
      @last_error_trace = ["command: #{@command}"]
      return_value = execute_shell(@command)
      #@last_error_trace = ["command: #{@command}", get_command_output].join("\r\n") if !return_value
    rescue Exception => ex
      @last_error = ex.message
    ensure
      @last_error_trace.push(get_command_output) if !return_value
    end

    return return_value
  end

  protected

  # build the bcp command text (current only supports bcp "in")
  def build_command
    connection_hash = @connection_hash.intern

    # TODO: handle in, out, and queryout
    # create the base command and add sql instance and error file
    command = "bcp #{@db_object} in \"#{@data_file_path}\" "
    command << "-e \"#{@error_file_path}\" " if @error_file_path
    command << "-S #{connection_hash[:instance_name]} "

    # add authentication
    if connection_hash[:trusted_connection]
      command << "-T "
    else
      command << "-U #{connection_hash[:user_id]} -P #{connection_hash[:password]} "
    end

    # set the database if one is provided
    command << "-d #{connection_hash[:database]} " if connection_hash[:database]

    # include each option
    @options.each do |key, val|
      option_value = ['t', 'r'].include?(key.to_s) ? " \"#{val}\"" : "#{val}"
      command << "-#{key}#{option_value} "
    end

    # redirect the command output to the temp file
    command << "> \"#{@temp_file_path}\""
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

  # clear the temp file by overwriting it
  def clear_command_output
    File.open(@temp_file_path, 'w') do |f_out|
      dummy = 1
    end
  end

  # helper method to execute a shell command
  def execute_shell(command)
    return Kernel.system(@command)
  end

  # read the contents of the temp file
  def get_command_output
    out = []
    File.open(@temp_file_path, 'r') do |f_in|
      f_in.each do |line|
        out.push(line.chomp)
      end
    end
    return out.join("\r\n")
  end

  # get a temp file that will be cleaned up at the end of the session
  def get_temp_file
    @temp_file = Tempfile.new(['command', '.out'])
    @temp_file.close
    @temp_file_path = File.expand_path(@temp_file.path).gsub('/', '\\')
  end

end

# this block only executes if the script is run directly from the command line
if $0 == __FILE__
  # some example code
  hash = {instance_name: 'MySQLInstance', trusted_connection: true, persist_security_info: false}
  conn = SQLConnection.new(hash)
  path = 'myfilepath'
  script = SQLScript.new(path, conn, {n: nil})
  ret = script.execute
  puts script.last_error, script.last_error_trace, ret
end
