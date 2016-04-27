require 'test/unit'
require 'rubygems'
require 'mocha/test_unit'
require_relative '../lib/ruby_etl'
require_relative 'helpers/ruby_etl_mocks'

class TestSQLQuery < Test::Unit::TestCase

  def initialize(name)
    super name

    SQLQuery.send(:define_method, :set_results) do |results|
      @results = [results].flatten
    end
  end

  def test_clear_results
    expected = [[1, 2, 3, 4], []]
    obj = SQLQuery.new('', Object.new)
    obj.set_results(expected[0])
    results = [obj.results, nil]
    obj.clear_results
    results[1] = obj.results
    assert_equal(expected, results)
  end

  def test_connection
    conn = Object.new
    conn.stubs(:who_am_i?).returns(:connection)
    obj = SQLQuery.new('', conn)
    assert_equal(:connection, obj.connection.who_am_i?)
  end

  def test_execute
    rs = RubyETLMocks::MockRecordset.new(helper_get_fields, nil)
    SQLOLE.expects(:get_recordset).returns(rs)
    obj = SQLQuery.new('', RubyETLMocks::MockConnection.new)
    assert_equal(true, obj.execute)
  end

  def test_fields
    rs = RubyETLMocks::MockRecordset.new(helper_get_fields, nil)
    SQLOLE.expects(:get_recordset).returns(rs)
    obj = SQLQuery.new('', RubyETLMocks::MockConnection.new)
    obj.execute
    assert_equal(helper_get_fields, obj.fields)
  end

  def test_last_error
    rs = RubyETLMocks::MockRecordset.new(helper_get_fields, nil)

    def rs.MoveNext
      raise Exception, 'Failure for testing'
    end

    SQLOLE.expects(:get_recordset).returns(rs)
    obj = SQLQuery.new('SELECT * FROM MyTable', RubyETLMocks::MockConnection.new)
    result = obj.execute
    assert_equal(false, result)
    assert_equal(['SELECT * FROM MyTable', 'Failure for testing'], obj.last_error)
  end

  def test_last_error_trace
    rs = RubyETLMocks::MockRecordset.new(helper_get_fields, nil)

    def rs.MoveNext
      raise Exception, 'Failure for testing'
    end

    SQLOLE.expects(:get_recordset).returns(rs)
    obj = SQLQuery.new('SELECT * FROM MyTable', RubyETLMocks::MockConnection.new)
    result = obj.execute

    assert_equal(false, result)
    assert_instance_of(Array, obj.last_error_trace)
    assert_equal('SELECT * FROM MyTable', obj.last_error_trace[0])
    assert_equal(true, obj.last_error_trace.length > 1)
  end

  def test_new
    obj = SQLQuery.new('', Object.new)
    assert_instance_of(SQLQuery, obj)
  end

  def test_results
    expected = [1, 2, 3, 4, 5]
    obj = SQLQuery.new('', Object.new)
    obj.set_results(expected)
    assert_instance_of(Array, obj.results)
    assert_equal(expected, obj.results)
  end

  def test_sql_statement
    sql = 'SELECT * FROM SomeTable'
    obj = SQLQuery.new(sql.to_sym, Object.new)
    assert_equal(sql, obj.sql_statement)
  end

  def helper_get_data
  [
    [1, 2, 3, 4, 5],
    [6, 7, 8, 9 ,10],
    [11, 12, 13, 14, 15]
  ]
  end

  def helper_get_fields
  ['Field1', 'Field2', 'Field3', 'Field4', 'Field5']
  end

end
