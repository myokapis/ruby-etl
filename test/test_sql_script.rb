require 'test/unit'
require 'rubygems'
require 'mocha/test_unit'
require_relative '../lib/ruby_etl'
require_relative 'helpers/ruby_etl_mocks'

class TestSQLScript< Test::Unit::TestCase

  # perform class setup
  def initialize(name)
    super name
    
    # add an instance method to the class under test so that non-bcp commands can be executed
    SQLScript.send(:define_method, :set_command) do |command|
      @command = command
    end
  end

  def test_command_minimal
    conn_hash = {:instance_name => 'MySQLInstance', :trusted_connection => true}
    obj = SQLScript.new('input_file_path', conn_hash)
    assert_equal('osql -S MySQLInstance -E -i "input_file_path"', obj.command)
  end

  def test_command_full
    conn_hash = {:instance_name => 'MySQLInstance', :user_id => 'MyUserID', :password => 'MyPassword', :database => 'MyDatabase'}
    options = helper_get_options
    options['Q'] = nil
    expected = "osql -S MySQLInstance -U MyUserID -P MyPassword -d MyDatabase #{helper_get_options_string} -Q \"some query\""
    obj = SQLScript.new('some query', conn_hash, options)
    assert_equal(expected, obj.command)
  end

  def test_command_output
    conn_hash = {:instance_name => 'MySQLInstance', :trusted_connection => true}
    expected = ['command: osql -S MySQLInstance -E -i "input_file_path"', 'some output']
    process_status = Object.new
    process_status.expects(:success?).returns(true)
    Open3.expects(:capture2e).returns([['some output'], process_status])
    obj = SQLScript.new('input_file_path', conn_hash)
    obj.execute
    assert_equal(expected, obj.command_output)
  end

  def test_execute
    process_status = Object.new
    process_status.expects(:success?).returns(true)
    Open3.expects(:capture2e).returns([[], process_status])
    obj = SQLScript.new('', {})
    assert_equal(true, obj.execute)
  end

  def test_last_error
    Open3.expects(:capture2e).raises(Exception, 'test_last_error')
    obj = SQLScript.new('', {})
    assert_equal(nil, obj.execute)
    assert_equal('test_last_error', obj.last_error)
  end

  def test_last_error_trace
    conn_hash = {:instance_name => 'MySQLInstance', :trusted_connection => true}
    expected = 'command: osql -S MySQLInstance -E -i "input_file_path"'
    Open3.expects(:capture2e).raises(Exception, 'test_last_error')
    obj = SQLScript.new('input_file_path', conn_hash)
    assert_equal(nil, obj.execute)
    assert_equal(expected, obj.last_error_trace[0])
    assert_equal(true, obj.last_error_trace.length > 1)
  end

  def test_new
    obj = SQLScript.new('', nil)
    assert_instance_of(SQLScript, obj)
  end

  protected

  def helper_get_conn_hash
  {
    :instance_name => 'MySQLInstance',
    :user_id => 'MyUserID',
    :password => 'MyPassword',
    :database => 'MyDatabase'
  }
  end
  
  def helper_get_options
  {
    :H => 'MyWorkstation',
    :l => 30,
    :t => 300,
    :h => -1,
    :s => '|',
    :w => 120,
    :a => 1000,
    :e => nil,
    :I => nil,
    :c => 'GEHEN',
    :n => nil,
    :m => -1,
    :r => 1,
    :o => 'output_file_path',
    :p => nil,
    :b => nil,
    :u => nil,
    :R => nil,
    :O => nil,
    :X => 1
  }
  end

  def helper_get_options_string
    out = []

    helper_get_options.each do |key, val|
      if val.nil?
        out.push("-#{key}")
      elsif [:s, :o].include?(key)
        out.push("-#{key} \"#{val}\"")
      elsif val.to_s.slice(0) == '-'
        out.push("-#{key}#{val}")
      else
        out.push("-#{key} #{val}")
      end
    end

    return out.join(' ')
  end

end