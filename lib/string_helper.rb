# frozen_string_literal: true

module StringHelper
  SPECIAL_RULES = {
    "du" => "DU"
  }.freeze
  private_constant :SPECIAL_RULES

  def snake_case(str)
    str.gsub(/(?<=[a-z])([A-Z])/, "_\\1").gsub(/(?<=[A-Z])([A-Z])(?=[a-z])/, "_\\1").downcase
  end

  def camelize(str)
    str.split("_").map { |part| capitalize_special(part) }.join
  end

  def capitalize_special(word)
    SPECIAL_RULES[word] || word.capitalize
  end

  module_function :camelize, :capitalize_special, :snake_case
end
