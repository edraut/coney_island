module ActiveJob
  module QueueAdapters
    class ConeyIslandAdapter
      # == ConeyIsland adapter for Active Job
      #
      # ConeyIsland is an industrial-strength background worker system for Rails using RabbitMQ. Read more about
      # {here}[http://edraut.github.io/coney_island/].
      #
      # To use ConeyIsland set the queue_adapter config to +:coney_island+.
      #
      #   Rails.application.config.active_job.queue_adapter = :coney_island
      def enqueue(job) #:nodoc:
        ConeyIsland::Submitter.submit job.class, :perform, args: job.arguments, work_queue: job.queue_name, timeout: get_timeout_from_args(job),
          retry_limit: get_retry_from_args(job), singleton: true
      end

      def enqueue_at(job, timestamp) #:nodoc:
        params = {args: job.arguments, work_queue: job.queue_name, timeout: get_timeout_from_args(job),
          retry_limit: get_retry_from_args(job), singleton: true}
        delay = timestamp - Time.current.to_f
        if delay > 0
          params[:delay] = delay.round
        end
        ConeyIsland::Submitter.submit job.class, :perform, params
      end

      def get_timeout_from_args(job)
        job.class::TIMEOUT if job.class.const_defined? :TIMEOUT
      end

      def get_retry_from_args(job)
        job.class::RETRY_LIMIT if job.class.const_defined? :RETRY_LIMIT
      end

    end
  end
end
