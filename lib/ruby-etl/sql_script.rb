require 'open3'

# executes a sql script against a sql connection using osql
class SQLScript

  attr_reader :command, :command_output, :last_error, :last_error_trace

  # initializes the script object
  # file_or_query - network path to the sql script to be executed or query text
  # connection_hash - connection options
  # options - (optional) list of osql command options
  #
  def initialize(file_or_query, connection_hash, options={})
    @file_or_query = file_or_query
    @connection_hash = connection_hash || {}
    @last_error = nil
    @last_error_trace = []
    @command_output = []
    @options = build_options(options)
    @command = build_command
  end

  # executes the osql command
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

  # builds the osql command text
  def build_command

    # builds the base osql command
    command = "osql -S #{@connection_hash[:instance_name]} "

    # adds authentication
    if @connection_hash[:trusted_connection]
      command << "-E "
    else
      command << "-U #{@connection_hash[:user_id]} -P #{@connection_hash[:password]} "
    end

    # adds a database if one is specified
    command << "-d #{@connection_hash[:database]} " if @connection_hash[:database]

    # adds each osql option
    @options.each do |key, val|
      if [:H, :l, :t, :h, :s, :w, :a, :e, :I, :c, :n, :m, :r, :o, :p, :b, :u, :R, :O, :X].include?(key)
        spacing = [nil, '-'].include?(val[0]) ? '' : ' '
        quoting = [:s, :o].include?(key) ? '"' : ''
        command << "-#{key}#{spacing}#{quoting}#{val}#{quoting} "
      end
    end

    # sets the input script path or query
    command << "-#{@options[:Q] ? 'Q' : 'i'} \"#{@file_or_query}\""

    return command
  end

  # builds a set of options for the osql command
  def build_options(options)
    out = {}
    options.each {|key, val| out[key.to_sym] = val.nil? ? '' : val.to_s}
    return out
  end

end
