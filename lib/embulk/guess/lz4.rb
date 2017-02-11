module Embulk
  module Guess

    # TODO implement guess plugin to make this command work:
    #      $ embulk guess -g "lz4" partial-config.yml

    #class Lz4 < GuessPlugin
    #  Plugin.register_guess("lz4", self)
    #
    #  FOO_BAR_HEADER = "\x1f\x8b".force_encoding('ASCII-8BIT').freeze
    #
    #  def guess(config, sample_buffer)
    #    if sample_buffer[0,2] == FOO_BAR_HEADER
    #      return {"decoders" => [{"type" => "lz4"}]}
    #    end
    #    return {}
    #  end
    #end

  end
end
