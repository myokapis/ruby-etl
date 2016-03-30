require_relative 'sql_connection'
require_relative 'sql_ole'
require 'pp'

# defines and executes a sql query against a connection
class SQLQuery
 
  attr_reader :connection, :sql_statement, :results, :fields, :last_error, :last_error_trace
 
  # initializes the query object
  # sql_statement - the sql text to be executed
  # connection - an instance of SQLConnection
  #
  def initialize(sql_statement, connection)
    @connection = connection
    @sql_statement = sql_statement.to_s
    @fields = []
    @results = []
  end
  
  # clears the results
  def clear_results
    @results = []
  end
 
  # executes the query against the connection
  def execute
    begin
      is_successful = false
      # open the connection if it is closed
      @connection.open if @connection.connection_state_name.upcase == 'CLOSED'

      # open a recordset from the sql statement
      recordset = SQLOLE.get_recordset
      recordset.Open(@sql_statement, @connection.ole_connection, 1)

      begin
   
        # get the field names
        @fields = []
        recordset.Fields.each do |field|
            @fields << field.Name
        end

        # get the rows from the recordset
        begin
          while recordset.State > 0 && !recordset.EOF 
            data = []
            # get each column from the current record
            @fields.each_with_index do |field_name, field_index|
              # get the data from the current column
              field_data = recordset[field_name].Value
              # add an empty column if no data was found
              data << field_data | []
            end

            # add the current record's data to the results
            @results << data
            # fetch the next record
            recordset.MoveNext
          end

          is_successful = true

        # handle an error in processing rows
        rescue Exception => ex
          @results = []
          @last_error = [@sql_statement, ex.message].join("\r\n")
          @last_error_trace = ex.backtrace.join("\r\n")
        end
   
      # make sure the recordset and connection get closed and cleaned up
      ensure
        recordset.Close if recordset && recordset.State > 0
        recordset = nil
        @connection.close if !@connection.connection_state_name.upcase == 'CLOSED'
      end

    # handle an error in opening the recordset
    rescue Exception => ex
      @last_error = [@sql_statement, ex.message].join("\r\n")
      @last_error_trace = ex.backtrace.join("\r\n")
    end

    return is_successful
  end
 
end

# this block only executes when the script is run directly from the command line
if $0 == __FILE__
  # some example code
  hash = {instance_name: 'MySQLInstance', trusted_connection: true}
  conn = SQLConnection.new(hash)
  query = SQLQuery.new("SELECT 1;RAISERROR('bomb this', 16, 1)", conn)
  return_value = query.execute
  puts return_value
  puts query.results
end
