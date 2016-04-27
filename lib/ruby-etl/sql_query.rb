# defines and executes a sql query against a connection
class SQLQuery
 
  ##
  # returns the connection object
  attr_reader :connection

  # returns the sql statement
  attr_reader :sql_statement

  # returns the query results as an array of rows
  attr_reader :results

  # returns an array of field names in the query results
  attr_reader :fields

  # returns an array of text from the last error
  attr_reader :last_error

  # returns the stack trace from the last error as an array
  attr_reader :last_error_trace
 
  ##
  # initializes the query object
  # sql_statement:: - the sql text to be executed
  # connection:: - an instance of SQLConnection
  def initialize(sql_statement, connection)
    @connection = connection
    @sql_statement = sql_statement.to_s
    @fields = []
    @results = []
  end
  
  ##
  # clears the results
  def clear_results
    @results = []
  end
 
  ##
  # executes the query against the connection
  def execute
    is_successful = false

    begin
      # open the connection if it is closed
      @connection.open if @connection.connection_state_name.upcase == 'CLOSED'

      # open a recordset from the sql statement
      recordset = SQLOLE.get_recordset
      recordset.Open(@sql_statement, @connection.ole_connection, 1)

      # get the field names
      @fields = []
      recordset.Fields.each {|field| @fields << field.Name}

      # get the rows from the recordset
      while recordset.State > 0 && !recordset.EOF 
        data = []

        # get each column from the current record
        @fields.each do |field_name|

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

    rescue Exception => ex
      @results = []
      @last_error = [@sql_statement, ex.message].flatten
      @last_error_trace = [@sql_statement, ex.backtrace].flatten
    ensure
      recordset.Close if recordset && recordset.State > 0
      recordset = nil
      @connection.close if !@connection.connection_state_name.upcase == 'CLOSED'
    end

    return is_successful
  end
 
end
