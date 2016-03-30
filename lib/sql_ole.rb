require 'win32ole'

# wraps Win32OLE objects
class SQLOLE

  def self.get_connection
    return WIN32OLE.new('ADODB.Connection')
  end

  def self.get_recordset
    return WIN32OLE.new('ADODB.Recordset')
  end

end
