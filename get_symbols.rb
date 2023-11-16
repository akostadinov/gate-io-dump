# frozen_string_literal: true


# curl -X GET https://api.gateio.ws/api/v4/spot/currency_pairs -H 'Accept: application/json' | jq -r '.[] | select(.trade_status == "tradable") | .id'



require "json"

class GateIOAPI
  def apiconfig
    config["api"]
  end

  # https://www.gate.io/docs/developers/apiv4/en/
  URL = "https://api.gateio.ws/api/v4"
  DEBUG = true

  attr_reader :config

  def initialize(config: "dump.conf")
    @config = YAML.load_file(config).freeze
  end
end
