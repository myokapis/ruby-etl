module RubyETLMocks

  class MockConnection
    attr_reader :connection_state_name

    def initialize
      @connection_state_name = 'CLOSED'
    end

    def close
      @connection_state_name = 'CLOSED'
    end

    def ole_connection
      return Object.new
    end

    def open
      @connection_state_name = 'OPEN'
    end
  end

  class MockRecordset
    def initialize(fields, data)
      @fields = fields || []
      @data = data || [[]]
      @row_index = 0
      @state = 0
    end

    def [](field_name)
      return get_field(field_name)
    end

    def Close
      @state = 0
    end

    def EOF
      return @row_index > @data.length
    end

    def Fields
      return @fields.map{|field_name| get_field(field_name)}
    end

    def MoveNext
      @row_index += 1
    end
    
    def Open(sql_statement, ole_connection, option)
      @state = 1
    end

    def State
      return @state
    end

    private

    def get_field(field_name)
      field_index = @fields.index(field_name)
      row = @data[@row_index] || []
      return MockField.new(field_name, row[field_index])
    end

  end

  class MockField

    def initialize(field_name, field_value)
      @field_name = field_name
      @field_value = field_value
    end

    def Name
      @field_name
    end

    def Value
      @field_value
    end

  end

end