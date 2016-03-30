# extends the base File class
class File

  # joins a list of elements into a file path and expands the path
  # returns a Windows-friendly file path
  def self.full_path(*elements)
    File.expand_path(File.join(elements)).gsub('/', '\\')
  end

end
