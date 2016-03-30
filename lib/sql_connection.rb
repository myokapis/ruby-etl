require_relative 'sql_ole'

# defines an ole connection to sql server
class SQLConnection
  attr_reader :connection_hash, :connection_params, :ole_connection
 
  # valid connection states
  STATECLOSED = 0
  STATEOPEN = 1
  STATECONNECTING = 2
  STATEEXECUTING = 4
  STATEFETCHING = 8
 
  STATECODES = [STATEFETCHING, STATEEXECUTING, STATECONNECTING, STATEOPEN, STATECLOSED]
  STATENAMES = ['Fetching', 'Executing', 'Connecting', 'Open', 'Closed']
 
  # creates an instance of an ole connection and configures the connection parameters
  def initialize(connection_hash)
    @ole_connection = SQLOLE.get_connection
    @connection_hash = connection_hash
    @connection_params =
    {
      :provider => "Provider=SQLOLEDB.1",
      :library => "Network Library=dbmssocn"
    }
    set_connection_params(connection_hash)
  end
 
  # closes the ole connection
  def close
    @ole_connection.Close
  end
 
  # returns the connection state code
  def connection_state_code
    conn_state = @ole_connection.State
   
    STATECODES.each do |value|
      return value if ((conn_state & value) == value)
    end
 
    return nil
  end
 
  # returns the connection state name
  def connection_state_name
    conn_state = @ole_connection.State
   
    STATECODES.each_with_index do |value, index|
      return STATENAMES[index] if ((conn_state & value) == value)
    end
 
    return 'Unknown'
  end
 
  # returns a hash of connection states
  def connection_states
    hash = {}
    names = STATENAMES.reverse
 
    STATECODES.sort.each_with_index do |code, index|
      hash[names[index]] = code
    end
 
    return hash
  end
 
  # builds a connection string from the connection params
  def connection_string
    return @connection_params.values.compact.join(';')
  end
 
  # shows help for this object
  def self.help(command=nil)
    if command.nil? || ['initialize', 'new'].include?(command.to_s)
      puts 'SQLConnection.new(conn_hash)'
      puts
      puts 'Valid conn_hash options:'
      puts "  :persist_security_info = {true|false}"
      puts "  :trusted_connection = {true|false}"
      puts "  :user_id"
      puts "  :password"
      puts "  :database"
      puts "  :instance_name"
      puts
    end
  end
 
  # opens the ole connection
  def open
    @ole_connection.Open(connection_string)
  end
 
  # TODO: ensure that disposing of an instance also disposes of child objects
 
  protected
 
  # base connection params
  def base_params
    return [:persist_security_info, :trusted_connection, :user_id, :password, :database, :instance_name]
  end
 
  # creates a connection string element for the database
  def get_database_option(value)
    return value.nil? ? nil : "Initial Catalog=#{value}"
  end
 
  # creates a connection string element for the sql instance
  def get_instance_name_option(value)
    return value.nil? ? nil : "Data Source=#{value}"
  end
 
  # creates a connection string element for the password
  def get_password_option(value)
    return value.nil? ? nil : "Password=#{value}"
  end
 
  # creates a connection string element for persisting security info
  def get_persist_security_info_option(value)
    return value.nil? ? nil : "Persist Security Info=#{value ? "True" : "False"}"
  end
 
  # creates a connection string element for a trusted connection
  def get_trusted_connection_option(value)
    return value ? "Integrated Security=SSPI" : nil
  end
 
  # creates a connection string element for the user
  def get_user_id_option(value)
    return value ? "User Id=#{value}" : nil
  end
 
  # builds connection params from the connection hash
  def set_connection_params(connection_hash)
    # use each base connection param
    base_params.each do |param_name|
      parm_name = param_name.to_sym
      # compose the method name that corresponds to the parameter
      method_name = "get_#{param_name}_option".to_sym
      # call the method to get the formatted param
      @connection_params[parm_name] = send(method_name, connection_hash[parm_name])
    end
  end
 
end
 
# this block only executes when the script is run directly from the command line
if $0 == __FILE__
  # some example code
  SQLConnection.help
  hash = {instance_name: 'HQ1SVDSQL002', trusted_connection: true, persist_security_info: true}
  conn = SQLConnection.new(hash)
  puts conn.connection_string
  conn.open
  puts conn.connection_state_name
  conn.close
  puts conn.connection_state_name
  puts conn.connection_states.inspect
end
