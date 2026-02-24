require 'net/https'
require 'xmlsimple'
require 'csv'
require "salesforce_bulk/version"
require 'salesforce_bulk/job'
require 'salesforce_bulk/connection'

module SalesforceBulk
  # Your code goes here...
  class Api

    @@SALESFORCE_API_VERSION = '54.0'

    def self.new_with_credentials(username, password, in_sandbox=false, concurrencyMode = nil)
      api = self.new(concurrencyMode)
      api.connection = SalesforceBulk::Connection.new_with_credentials(username, password, @@SALESFORCE_API_VERSION, in_sandbox)
      api
    end

    def self.new_with_token(token, domain, concurrencyMode = nil)
      api = self.new(concurrencyMode)
      api.connection = SalesforceBulk::Connection.new_with_token(token, domain, @@SALESFORCE_API_VERSION)
      api
    end

    def initialize(concurrencyMode = nil)
      @concurrencyMode = concurrencyMode
    end

    def connection=(connection) 
      @connection = connection
      self
    end

    def upsert(sobject, records, external_field, wait=false)
      self.do_operation('upsert', sobject, records, external_field, wait)
    end

    def update(sobject, records, wait=false)
      self.do_operation('update', sobject, records, nil, wait)
    end
    
    def create(sobject, records, wait=false)
      self.do_operation('insert', sobject, records, nil, wait)
    end

    def delete(sobject, records, wait=false)
      self.do_operation('delete', sobject, records, nil, wait)
    end

    def query(sobject, query)
      self.do_operation('query', sobject, query, nil)
    end

    def do_operation(operation, sobject, records, external_field, wait=false)
      job = SalesforceBulk::Job.new(operation, sobject, records, external_field, @connection, @concurrencyMode)

      # TODO: put this in one function
      job_id = job.create_job()
      if(operation == "query")
        batch_id = job.add_query()
      else
        batch_id = job.add_batch()
      end
      job.close_job()

      if wait or operation == 'query'
        while true
          state = job.check_batch_status()
          if state != "Queued" && state != "InProgress"
            break
          end
          sleep(2) # wait x seconds and check again
        end
        
        if state == 'Completed'
          job.get_batch_result()
          job
        else
          job.result.message = "There is an error in your job. The response returned a state of #{state}. Please check your query/parameters and try again."
          job.result.success = false
          return job

        end
      else
        return job
      end

    end

    def parse_batch_result result
      begin
        CSV.parse(result, :headers => true)
      rescue
        result
      end
    end

  end  # End class
end # End module
