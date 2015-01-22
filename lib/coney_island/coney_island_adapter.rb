class ConeyIslandAdapter
  # == ConeyIsland adapter for Active Job
  #
  # ConeyIsland is an industrial-strength background worker system for Rails using RabbitMQ. Read more about
  # {here}[http://edraut.github.io/coney_island/].
  #
  # To use ConeyIsland set the queue_adapter config to +:coney_island+.
  #
  #   Rails.application.config.active_job.queue_adapter = :coney_island
  class << self
    def enqueue(job) #:nodoc:
      ConeyIsland::Worker.submit JobWrapper, :perform, args: [ job.arguments ], work_queue: job.queue_name, timeout: get_timeout_from_args(job)
    end

    def enqueue_at(job, timestamp) #:nodoc:
      delay = timestamp - Time.current.to_f
      ConeyIsland::Worker.submit JobWrapper, :perform, args: [ job.arguments ], work_queue: job.queue_name, delay: delay, timeout: get_timeout_from_args(job)
    end

    def get_timeout_from_args(job)
      job.arguments['timeout']
    end
  end

  class JobWrapper #:nodoc:
    class << self
      def perform(job_data)
        Base.execute job_data
      end
    end
  end
end
