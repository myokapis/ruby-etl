# extends the Hash base class
class Hash

  # converts the hash keys to symbols (does not affect nested keys)
  def intern
    out = {}
    self.each do |key, val|
      out[key.to_s.to_sym] = val
    end
    return out
  end

end