require 'open3'

##
# defines and executes a bcp command against a target connection
class SQLBulkCopy

  attr_reader :command, :command_output, :last_error, :last_error_trace

  ##
  # setup the bcp command object
  # db_object:: - the database target object
  # data_file_path:: - network path to the source data file
  # error_file_path:: - network path to the error output file (nil = no error file)
  # connection_hash:: - a hash of connection parameters
  # options:: - (optional) hash of bcp command options
  def initialize(db_object, data_file_path, error_file_path, connection_hash, options={}, direction=:in)
    @db_object = db_object
    @data_file_path = data_file_path
    @error_file_path = error_file_path
    @connection_hash = connection_hash || {}
    @last_error = nil
    @last_error_trace = []
    @command_output = []
    @direction = direction.to_sym

    @options = build_options(options)
    @command = build_command
  end

  ##
  # execute the bcp command
  def execute()
    @last_error = nil
    @last_error_trace = ["command: #{@command}"]
    @command_output = ["command: #{@command}"]
    return_value = nil

    begin
      output, status = Open3.capture2e(@command)
      return_value = status.success?
    rescue Exception => ex
      @last_error = ex.message
      @last_error_trace.concat(ex.backtrace)
    ensure
      @command_output.concat([output].flatten).compact
    end

    return return_value
  end

  protected

  # build the bcp command text
  def build_command
    connection_hash = @connection_hash.intern

    # create the base command and add sql instance and error file
    wrapper = @direction == :queryout ? '"' : ''
    command = "bcp #{wrapper}#{@db_object}#{wrapper} #{@direction} \"#{@data_file_path}\" "
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
      option_value = [:f, :h, :i, :o, :r, :t].include?(key) ? "\"#{val}\"" : "#{val}"
      command << "-#{key}#{option_value} " if [:a, :b, :c, :C, :d, :E, :f, :F, :h, :i, :k, :K, :L, :m, :n, :N, :o, :q, :r, :R, :t, :V, :w, :x].include?(key)
    end

    return command.strip
  end

  # builds a set of options for the bcp command
  def build_options(options)
    out = {}
    
    options.each do |key, val|
      out[key.to_sym] = val.nil? ? '' : val.to_s
    end

    return out
  end

end
