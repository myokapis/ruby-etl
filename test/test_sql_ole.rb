require 'test/unit'
require 'rubygems'
require 'mocha/test_unit'
require_relative '../lib/ruby_etl'

class TestSQLOLE < Test::Unit::TestCase

  def test_get_connection
    mock_conn = Object.new
    mock_conn.stubs(:who_am_i).returns(:adodb_connection)
    WIN32OLE.expects(:new).with('ADODB.Connection').returns(mock_conn)
    conn = SQLOLE.get_connection
    assert_equal(:adodb_connection, conn.who_am_i)
  end

  def test_get_recordset

    mock_rs = Object.new
    mock_rs.stubs(:who_am_i).returns(:adodb_recordset)
    WIN32OLE.expects(:new).with('ADODB.Recordset').returns(mock_rs)
    rs = SQLOLE.get_recordset
    assert_equal(:adodb_recordset, rs.who_am_i)
  end

end
