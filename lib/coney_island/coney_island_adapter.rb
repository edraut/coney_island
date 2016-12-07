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
        ConeyIsland::Submitter.submit JobWrapper, :perform, args: [job.serialize], work_queue: job.queue_name, timeout: get_timeout_from_args(job), retry_limit: get_retry_from_args(job)
      end

      def enqueue_at(job, timestamp) #:nodoc:
        delay = timestamp - Time.current.to_f
        ConeyIsland::Submitter.submit JobWrapper, :perform, args: [job.serialize], work_queue: job.queue_name, delay: delay, timeout: get_timeout_from_args(job), retry_limit: get_retry_from_args(job)
      end

      def get_timeout_from_args(job)
        job.class::TIMEOUT if job.class.const_defined? :TIMEOUT
      end

      def get_retry_from_args(job)
        job.class::RETRY_LIMIT if job.class.const_defined? :RETRY_LIMIT
      end

      class JobWrapper #:nodoc:
        class << self
          def perform(job_data)
            Base.execute job_data.stringify_keys!
          end
        end
      end
    end
  end
end
