Embulk::JavaPlugin.register_decoder(
  "lz4", "org.embulk.decoder.lz4.Lz4DecoderPlugin",
  File.expand_path('../../../../classpath', __FILE__))
