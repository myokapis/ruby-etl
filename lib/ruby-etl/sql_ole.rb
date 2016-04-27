require 'win32ole'

##
# wraps Win32OLE objects to facilitate testing
class SQLOLE

  ##
  # returns a new instance of an ADODB connection
  def self.get_connection
    return WIN32OLE.new('ADODB.Connection')
  end

  ##
  # returns a new instance of an ADODB recordset
  def self.get_recordset
    return WIN32OLE.new('ADODB.Recordset')
  end

end
