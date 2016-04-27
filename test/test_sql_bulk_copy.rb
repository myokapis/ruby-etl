require 'test/unit'
require_relative '../lib/ruby_etl'

class TestSQLBulkCopy < Test::Unit::TestCase

  # perform class setup
  def initialize(name)
    super name
    
    # add an instance method to the class under test so that non-bcp commands can be executed
    SQLBulkCopy.send(:define_method, :set_command) do |command|
      @command = command
    end
  end

  def test_new
    obj = SQLBulkCopy.new(nil, nil, nil, nil)
    assert_instance_of(SQLBulkCopy, obj)
  end

  def test_command_minimal
    conn_hash = {instance_name: 'MySQLInstance', trusted_connection: true, persist_security_info: false}
    expected = "bcp MyDBObject in \"data_file_path\" -S MySQLInstance -T"
    obj = SQLBulkCopy.new('MyDBObject', 'data_file_path', nil, conn_hash)
    assert_equal(expected, obj.command)
  end

  def test_command_full
    expected = "bcp MyDBObject in \"data_file_path\" -e \"error_file_path\" -S MySQLInstance -U MyUser -P MyPassword -d MyDatabase #{helper_option_string}"
    obj = SQLBulkCopy.new('MyDBObject', 'data_file_path', 'error_file_path', helper_connection_hash, helper_option_hash)
    assert_equal(expected, obj.command)
  end

  def test_command_out
    conn_hash = {instance_name: 'MySQLInstance', trusted_connection: true, persist_security_info: false}
    expected = "bcp MyDBObject out \"data_file_path\" -S MySQLInstance -T"
    obj = SQLBulkCopy.new('MyDBObject', 'data_file_path', nil, conn_hash, {}, :out)
    assert_equal(expected, obj.command)
  end

  def test_command_output
    conn_hash = {instance_name: 'MySQLInstance', trusted_connection: true, persist_security_info: false}
    obj = SQLBulkCopy.new('SELECT * FROM sometable', 'data_file_path', nil, conn_hash, {}, 'queryout')
    obj.set_command('dir /b')
    obj.execute
    assert_equal('command: dir /b', obj.command_output[0])
    assert_instance_of(Array, obj.command_output)
    assert(obj.command_output.length >= 2)
  end

  def test_command_queryout
    conn_hash = {instance_name: 'MySQLInstance', trusted_connection: true, persist_security_info: false}
    expected = "bcp \"SELECT * FROM sometable\" queryout \"data_file_path\" -S MySQLInstance -T"
    obj = SQLBulkCopy.new('SELECT * FROM sometable', 'data_file_path', nil, conn_hash, {}, 'queryout')
    assert_equal(expected, obj.command)
  end

  def test_execute
    obj = SQLBulkCopy.new(nil, nil, nil, nil)
    obj.set_command('cls')
    assert_equal(true, obj.execute)
  end

  def test_last_error
    conn_hash = {instance_name: 'MySQLInstance', trusted_connection: true, persist_security_info: false}
    expected = [nil, 'No such file or directory - bogus command']
    obj = SQLBulkCopy.new('SELECT * FROM sometable', 'data_file_path', nil, conn_hash, {}, 'queryout')
    obj.set_command('bogus command')
    result = [obj.execute]
    result[1] = obj.last_error
    assert_equal(expected, result)
  end

  def test_last_error_trace
    conn_hash = {instance_name: 'MySQLInstance', trusted_connection: true, persist_security_info: false}
    obj = SQLBulkCopy.new('SELECT * FROM sometable', 'data_file_path', nil, conn_hash, {}, 'queryout')
    obj.set_command('bogus command')
    obj.execute
    assert_equal('command: bogus command', obj.last_error_trace[0])
    assert_instance_of(Array, obj.last_error_trace)
    assert(obj.last_error_trace.length >= 2)
  end

  private

  def helper_connection_hash
  {
    instance_name: 'MySQLInstance',
    user_id: 'MyUser',
    password: 'MyPassword',
    database: 'MyDatabase',
    persist_security_info: false
  }
  end

  def helper_option_hash
  {
    a: 100,
    b: 1000,
    c: nil,
    C: 'RAW',
    E: nil,
    f: 'format_file_path',
    F: 3,
    h: 'ORDER(Column1 ASC), TABLOCK',
    i: 'input_file_path',
    k: nil,
    K: 'ReadOnly',
    L: 50,
    m: 14,
    n: nil,
    N: nil,
    o: 'output_file_path',
    q: nil,
    r: '\n',
    R: nil,
    t: '\t',
    V: 100,
    w: nil,
    x: nil
  }
  end

  def helper_option_string
    out = []

    helper_option_hash.each do |key, val|
      fmt_val = val.nil? ? '' : [:f, :h, :i, :o, :r, :t].include?(key) ? "\"#{val}\"" : val
      out.push("-#{key}#{fmt_val}")
    end

    return out.join(' ')
  end

end