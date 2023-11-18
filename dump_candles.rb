# frozen_string_literal: true

require "json"
require "yaml"
require "faye/websocket"
require "permessage_deflate"

require_relative "lib/string_helper"

class Dump
  include StringHelper

  URL = "wss://api.gateio.ws/ws/v4/"
  DEBUG = true

  attr_reader :config

  def initialize(config: "dump.conf")
    @config = YAML.load_file(config).freeze
  end

  def start
    EM.run do
      new_connection
      start_streaming
    end
  end

  def new_connection
    self.ws = Faye::WebSocket::Client.new(URL, nil, headers:, ping: 20, extensions: [PermessageDeflate])
  end

  def ws
    @ws ||= new_connection
  end

  def headers
    {
      "Origin" => "https://gate.io/",
      "User-Agent" => "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/115.0",
    }
  end

  def random_alphanum(size)
    Array.new(size) { [*("a".."z"), *("0".."9")].sample }.join
  end

  def file_prefix(symbol)
    symbol.split(/[:\/]/).join("_")
  end

  def start_streaming
    ws.on :open do |_event|
      warn "[:open]"
      subscriptions!

      # EM.add_periodic_timer(0.3) do
      #   send_one_message.call
      # end
      # EM.add_shutdown_hook { puts "b" }
    end

    ws.on :message do |event|
      warn "< #{event.data}" if DEBUG
    end

    ws.on :close do |event|
      warn [:close, event.code, event.reason].inspect
      reset_state
      EM.stop_event_loop
    end
  end

  private

  attr_writer :ws

  # see https://www.gate.io/docs/developers/apiv4/ws/en/
  def request(channel:, event: nil, payload: nil)
    data = {
      time: Time.now.utc.to_i,
      channel:,
      event:,
      payload:,
    }

    msg = data.to_json
    warn "request: #{msg}" if DEBUG
    ws.send msg
  end

  def subscribe(channel:, payload: nil)
    request(channel:, event: "subscribe", payload:)
  end

  def unsubscribe(channel:, payload: nil)
    request(channel:, event: "unsubscribe", payload:)
  end

  def raw_write_csv(prefix, str)
    IO.write("#{prefix}_price_entries.csv", str, 0, mode: "a")
  end

  def subscriptions!
    # https://www.gate.io/docs/developers/apiv4/ws/en/#candlesticks-channel
    config["symbols"].each do |symbol|
      # payload e.g. ["1m", "BTC_USDT"]
      subscribe(channel: "spot.candlesticks", payload: ["1h", symbol])
    end
  end

  def reset_state
    self.ws = nil
  end
end

if __FILE__ == $PROGRAM_NAME
  options = { config: ARGV[0] }.compact
  Dump.new(**options).start
end
