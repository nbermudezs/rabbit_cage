require "rabbit_cage/version"
require 'bunny'

module RabbitCage
  def self.client
	  unless @client
	    connection = Bunny.new(ENV['CLOUDAMQP_URL'])
	    connection.start
	    @client = connection
	  end
	  @client
	end

	def self.channel
		@channel ||= client.create_channel
	end

	def self.exchanger
	  @exchanger ||= channel.direct('direct_exchanger')
	end

	def self.queues
		@queues ||= {}
	end

	def self.queue queue_name		
		q = queues[queue_name] || channel.queue(queue_name).bind(exchanger, :routing_key => queue_name)
		@queues[queue_name] = q unless @queues[queue_name]
		q
	end

	def self.publish payload, opts = {}
		exchanger.publish payload, opts
	end

	def self.unsubscribe queue_name
		puts "unsubscribing #{queue_name}"
		q = queue queue_name
		q.unsubscribe
		@queues[queue_name] = nil
	end

	def self.subscribe_to_queue queue_name, &callback
	    r = queue(queue_name).subscribe(:ack => true, :timeout => ENV['QUEUE_TIMEOUT'].to_i, :message_max => 1) do |delivery_info, metadata, msg_payload|
	      callback.call(delivery_info, metadata, msg_payload)
	    end
	    unsubscribe(queue_name) if r == :timed_out
	end
end