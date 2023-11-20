# frozen_string_literal: true

require "json"
require "yaml"

require "net-http2"

require_relative "lib/string_helper"

class DumpOHLC
  include StringHelper

  # https://www.gate.io/docs/developers/apiv4/en/
  ADDRESS = "https://api.gateio.ws"
  # ADDRESS = "http://nghttp2.org"
  BASE_PATH = "/api/v4"
  DEBUG = true

  attr_reader :config, :work_queue

  def initialize(config: "dump.conf")
    @config = YAML.load_file(config).freeze
    @work_queue = Thread::Queue.new
    @connection_semaphore = Thread::Mutex.new

    # https://www.gate.io/docs/developers/apiv4/en/#frequency-limit-rule
    # I assume we have to limit as globally as possible, hence the class variable
    @@candlestick_limiter = RateLimiter.new(10, 199)
  end

  def start
    workers # instantiate worker threads

    puts symbols.join(",") if DEBUG

    repeat_hourly do
      connection # instantiate the connection
      statuses.each { work_queue << _1 }
      sleep 1 until work_queue.empty?
      puts("\nFound:\n", statuses.filter_map { _1.match })
    end
  end

  def update_status(status)
    # without limit we should get 3 entries and last would be partial, so skip it
    response = get_candlesticks_sync(symbol: status.symbol, interval: "1h", limit: 2, from: Time.now.utc.to_i - 2 * 60 * 60) { _1 }
    ohlc_data = JSON.load(response.body).map { OHLC.new(_1) }
    status.push ohlc_data[0]
    status.push ohlc_data[1]
  end

  def workers
    @workers = Array.new(5) do
      Thread.new do
        while status = work_queue.pop
          begin
            update_status(status)
            print "." if DEBUG
          rescue => e
            warn "#{status.symbol}: #{e.inspect}"
            # reconnect we should reconnect by error handler already
            # they should be synchronized when trying to obtain connection
            retry # TODO: limit this one
          end
        end
      end
    end
  end

  def statuses
    @statuses ||= symbols.map { Status.new(_1) }
  end

  def new_connection
    client = NetHttp2::Client.new(ADDRESS)
  end

  def reconnect
    # it seems like reconnect is done automatically when there is an error handler
    # if @connection_semaphore.try_lock
    #   begin
    #     # disconnect rescue nil # this has to be better thought out not to kill concurrent connections
    #     sleep 5 if @last_connection # space out reconnects a little
    #     @last_connection = monotonic
        @connection = NetHttp2::Client.new(ADDRESS)
    #     # still a thread may grab the connection before we nil it
        @connection.on(:error) do |exception|
    #       @connection = nil
          warn "connection: #{exception.inspect}"
        end
        @connection
    #   ensure
    #     @connection_semaphore.unlock
    #   end
    # else
    #   @connection_semaphore.synchronize { @connection }
    # end
  end

  def connection
    @connection || reconnect
  end

  def disconnect
    @connection&.close rescue nil
    @connection = nil
  end

  def common_headers
    {
      "Accept" => "application/json",
    }
  end

  # see https://www.gate.io/docs/developers/apiv4/en/#market-candlesticks
  # last entry can be partial
  def get_candlesticks_sync(symbol:, interval:, from:, to: nil, limit: nil)
    @@candlestick_limiter.tick

    params = {
      currency_pair: symbol,
      from:,
      interval:,
      to:,
      limit:,
    }.compact

    response = connection.call(:get, BASE_PATH + "/spot/candlesticks", headers: common_headers, params:)

    block_given? ? yield(response) : puts("#{symbol}: #{response.body}")
  end

  def symbols
    return @tradable_symbols if @tradable_symbols

    response = connection.call(:get, BASE_PATH + "/spot/currency_pairs", headers: common_headers)
    list = JSON.load(response.body)
    @tradable_symbols = list.filter_map { _1["id"] if _1["trade_status"] == "tradable" }
  end

  def repeat_hourly
    yield

    loop do
      time = Time.now
      time_ref = time + 60 * 60
      time_target = Time.new(time_ref.year, time_ref.month, time_ref.day, time_ref.hour, 1, 0)
      sleep time_target - time
      yield
    end
  end

  def monotonic
    Process.clock_gettime(Process::CLOCK_MONOTONIC)
  end

  class OHLC
    attr_accessor *%i(t v c h l o a)

    def initialize(arr)
      t, v, c, h, l, o, a = *arr
    end

    def volume
      @volume ||= v.to_f
    end
  end

  class Status
    attr_reader :symbol

    def initialize(symbol)
      @symbol = symbol
      @state = Array.new(2)
    end

    def push(candlestick)
      return if @state.find { _1 && _1.t == candlestick.t } # skip existing entries

      @state.rotate! -1
      @state[0] = candlestick
    end

    def match
      return unless @state[1]

      now = @state[0].volume
      prev = @state[1].volume
      diff = now - prev
      return if diff < prev # volume hasn't grown 100% or more

      "#{symbol} was #{@state[1].v} and now is #{@state[0].v}"
    end
  end

  # a naive rate limiter
  class RateLimiter
    # @param interval [Numeric] seconds
    def initialize(interval, count)
      @semaphore = Thread::Mutex.new
      @interval = interval
      @count = count
      @current_count = 0
    end

    def tick
      sleep_to_next_interval until _increment
    end

    def self.test
      limiter = new(2, 3)
      loop do limiter.tick ; print("."); end
    end

    private

    def now
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end

    def begin_time
      @begin_time ||= now
    end

    def sleep_to_next_interval
      sleep begin_time + @interval - now + 0.1 # safety margin
    end

    def _increment
      !! @semaphore.synchronize do
        if now - begin_time > @interval
          @begin_time = now
          @current_count = 1
        elsif @current_count < @count
          @current_count += 1
        end
      end
    end
  end
end

if __FILE__ == $PROGRAM_NAME
  # DumpOHLC::RateLimiter.test
  options = { config: ARGV[0] }.compact
  DumpOHLC.new(**options).start
end
