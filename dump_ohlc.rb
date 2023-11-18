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

  attr_reader :config

  def initialize(config: "dump.conf")
    @config = YAML.load_file(config).freeze
  end

  def start
    connection

    t1 = Thread.new {
      get_candlesticks_sync(symbol: "BTC_USDT", interval: "1h", from: Time.now.utc.to_i - 2 * 60 * 60)
    }
    t2 = Thread.new {
      get_candlesticks_sync(symbol: "ETH_USDT", interval: "1h", from: Time.now.utc.to_i - 2 * 60 * 60)
    }
    t3 = Thread.new {
      get_candlesticks_sync(symbol: "ETH_BTC", interval: "1h", from: Time.now.utc.to_i - 2 * 60 * 60)
    }

    t1.join
    t2.join
    t3.join
  end

  def new_connection
    client = NetHttp2::Client.new(ADDRESS)
  end

  # TODO: use error callback to reopen closed connection
  def connection
    @connection ||= NetHttp2::Client.new(ADDRESS)
  end

  def common_headers
    {
      "Accept" => "application/json",
    }
  end

  # see https://www.gate.io/docs/developers/apiv4/en/#market-candlesticks
  # last entry can be partial
  def get_candlesticks_sync(symbol:, interval:, from:, to: nil, limit: nil)
    params = {
      currency_pair: symbol,
      from:,
      interval:,
      to:,
      limit:,
    }.compact

    response = connection.call(:get, BASE_PATH + "/spot/candlesticks", headers: common_headers, params:)

    puts "#{symbol}: #{response.body}"
  end
end

if __FILE__ == $PROGRAM_NAME
  options = { config: ARGV[0] }.compact
  DumpOHLC.new(**options).start
end
