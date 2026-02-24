module SalesforceBulk

  class Connection

    @@XML_HEADER = '<?xml version="1.0" encoding="utf-8" ?>'
    @@API_VERSION = nil
    @@LOGIN_HOST = 'login.salesforce.com'
    @INSTANCE_HOST = nil # Gets set in login()

    def self.new_with_credentials(username, password, api_version, in_sandbox = false)
      connection = self.new(api_version)
      connection.username = username
      connection.password = password
      connection.in_sandbox = in_sandbox
      @@LOGIN_HOST = in_sandbox ? 'test.salesforce.com' : 'login.salesforce.com'
      connection.login()
      connection
    end

    def self.new_with_token(token, domain, api_version)
      connection = self.new(api_version)
      connection.session_id = token
      connection.server_url = domain
      @@INSTANCE_HOST = "#{connection.instance}.salesforce.com"
      connection
    end

    def initialize(api_version)
      @session_id = nil
      @server_url = nil
      @instance = nil
      @@API_VERSION = api_version
      @@LOGIN_PATH = "/services/Soap/u/#{@@API_VERSION}"
      @@PATH_PREFIX = "/services/async/#{@@API_VERSION}/"
    end

    def session_id=(session_id)
      @session_id = session_id
    end

    def server_url=(server_url)
      @server_url = server_url
    end


    def username=(username)
      @username = username
    end

    def password=(password)
      @password = password
    end

    def in_sandbox=(in_sandbox)
      @in_sandbox = in_sandbox
    end

    def instance()
      @instance ||= parse_instance()
    end

    #private

    def login()
      xml = '<?xml version="1.0" encoding="utf-8" ?>'
      xml += "<env:Envelope xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\""
      xml += "    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\""
      xml += "    xmlns:env=\"http://schemas.xmlsoap.org/soap/envelope/\">"
      xml += "  <env:Body>"
      xml += "    <n1:login xmlns:n1=\"urn:partner.soap.sforce.com\">"
      xml += "      <n1:username>#{@username}</n1:username>"
      xml += "      <n1:password>#{@password}</n1:password>"
      xml += "    </n1:login>"
      xml += "  </env:Body>"
      xml += "</env:Envelope>"
      headers = Hash['Content-Type' => 'text/xml; charset=utf-8', 'SOAPAction' => 'login']

      response = post_xml(@@LOGIN_HOST, @@LOGIN_PATH, xml, headers, true)

      # response_parsed = XmlSimple.xml_in(response)
      response_parsed = parse_response response

      @session_id = response_parsed['Body'][0]['loginResponse'][0]['result'][0]['sessionId'][0]
      @server_url = response_parsed['Body'][0]['loginResponse'][0]['result'][0]['serverUrl'][0]
      @@INSTANCE_HOST = "#{instance()}.salesforce.com"
    end

    def post_xml(host, path, xml, headers, debug = false)

      host = host || @@INSTANCE_HOST

      if host != @@LOGIN_HOST # Not login, need to add session id to header
        headers['X-SFDC-Session'] = @session_id;
        path = "#{@@PATH_PREFIX}#{path}"
      end

      ::Rails.logger.debug "Posting XML to host #{host} on path #{path} with headers #{headers} and XML #{xml}"

      https(host, debug).post(path, xml, headers).body
    end

    def get_request(host, path, headers)
      host = host || @@INSTANCE_HOST
      path = "#{@@PATH_PREFIX}#{path}"

      if host != @@LOGIN_HOST # Not login, need to add session id to header
        headers['X-SFDC-Session'] = @session_id;
      end

      https(host).get(path, headers).body
    end

    def https(host, debug = false)
      req = Net::HTTP.new(host, 443)
      req.use_ssl = true
      req.set_debug_output $stdout if debug
      req.verify_mode = OpenSSL::SSL::VERIFY_NONE
      req
    end

    def parse_instance()
      @instance = @server_url.match(/https:\/\/[a-z]{2}[0-9]{1,3}/).to_s.gsub("https://","")
      @instance = @server_url.split(".salesforce.com")[0].split("://")[1] if @instance.nil? || @instance.empty?
      return @instance
    end

    def parse_response response
      response_parsed = XmlSimple.xml_in(response)

      if response.downcase.include?("faultstring") || response.downcase.include?("exceptionmessage")
        begin
          if response.downcase.include?("faultstring")
            error_message = response_parsed["Body"][0]["Fault"][0]["faultstring"][0]
          elsif response.downcase.include?("exceptionmessage")
            error_message = response_parsed["exceptionMessage"][0]
          end

        rescue
          raise "An unknown error has occured within the salesforce_bulk gem. This is most likely caused by bad request, but I am unable to parse the correct error message. Here is a dump of the response for your convenience. #{response}"
        end

        raise error_message
      end

      response_parsed
    end

  end

end
