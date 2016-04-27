require 'test/unit'
require 'rubygems'
require 'mocha/test_unit'
require_relative '../lib/ruby_etl'

class TestSQLConnection < Test::Unit::TestCase

  def test_new
    SQLOLE.expects(:get_connection)
    obj = SQLConnection.new(nil)
    assert_instance_of(SQLConnection, obj)
  end

  def test_no_conn_params
    expected_params = {}.tap{|hash| helper_conn_params.each {|key, val| hash[key] = [:provider, :library].include?(key) ? val : nil}}
    SQLOLE.expects(:get_connection)
    obj = SQLConnection.new(nil)
    assert_equal(expected_params, obj.connection_params)
    assert_equal({}, obj.connection_hash)
  end

  def test_all_conn_params
    SQLOLE.expects(:get_connection)
    obj = SQLConnection.new(helper_conn_hash)
    assert_equal(helper_conn_params, obj.connection_params)
    assert_equal(helper_conn_hash, obj.connection_hash)
  end

  def test_connection_string
    expected = helper_conn_params.values.join(';')
    SQLOLE.expects(:get_connection)
    obj = SQLConnection.new(helper_conn_hash)
    assert_equal(expected, obj.connection_string)
  end

  def test_ole_connection
    ole_obj = Object.new
    SQLOLE.expects(:get_connection).returns(ole_obj)
    obj = SQLConnection.new(nil)
    assert_equal(ole_obj.class, obj.ole_connection.class)
  end

  def test_close
    mock_ole = Object.new
    mock_ole.expects(:Close)
    SQLOLE.expects(:get_connection).returns(mock_ole)
    obj = SQLConnection.new(nil)
    obj.close
  end

  def test_connection_state_code_invalid
    mock_ole = Object.new
    mock_ole.expects(:State).returns(128)
    SQLOLE.expects(:get_connection).returns(mock_ole)
    obj = SQLConnection.new(nil)
    assert_nil(obj.connection_state_code)
  end

  def test_connection_state_code_valid
    mock_ole = Object.new
    mock_ole.expects(:State).returns(1)
    SQLOLE.expects(:get_connection).returns(mock_ole)
    obj = SQLConnection.new(nil)
    assert_equal(1, obj.connection_state_code)
  end

  def test_connection_state_name_invalid
    mock_ole = Object.new
    mock_ole.expects(:State).returns(128)
    SQLOLE.expects(:get_connection).returns(mock_ole)
    obj = SQLConnection.new(nil)
    assert_equal('Unknown', obj.connection_state_name)
  end

  def test_connection_state_name_valid
    mock_ole = Object.new
    mock_ole.expects(:State).returns(1)
    SQLOLE.expects(:get_connection).returns(mock_ole)
    obj = SQLConnection.new(nil)
    assert_equal('Open', obj.connection_state_name)
  end

  def test_connection_states
    SQLOLE.expects(:get_connection)
    obj = SQLConnection.new(nil)
    assert_equal(helper_connection_states, obj.connection_states)
  end

  def test_open
    mock_ole = Object.new
    mock_ole.expects(:Open)
    SQLOLE.expects(:get_connection).returns(mock_ole)
    obj = SQLConnection.new(nil)
    obj.open
  end

  private

  def helper_conn_params
  {
    :provider => "Provider=SQLOLEDB.1",
    :library => "Network Library=dbmssocn",
    :persist_security_info => 'Persist Security Info=False',
    :trusted_connection => 'Integrated Security=SSPI',
    :user_id => 'User Id=MyUserName',
    :password => 'Password=MyPassword',
    :database =>  'Initial Catalog=MyDatabase',
    :instance_name => 'Data Source=MySQLInstance'
  }
  end

  def helper_conn_hash
  {
    :persist_security_info => false,
    :trusted_connection => true,
    :user_id => 'MyUserName',
    :password => 'MyPassword',
    :database =>  'MyDatabase',
    :instance_name => 'MySQLInstance'
  }
  end

  def helper_connection_states
  {
    'Closed' => 0,
    'Open' => 1,
    'Connecting' => 2,
    'Executing' => 4,
    'Fetching' => 8
  }
  end

end



