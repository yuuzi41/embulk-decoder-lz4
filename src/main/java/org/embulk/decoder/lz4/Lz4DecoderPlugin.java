package org.embulk.decoder.lz4;

import java.io.InputStream;
import java.io.IOException;

import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.DecoderPlugin;
import org.embulk.spi.FileInput;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.util.FileInputInputStream;
import org.embulk.spi.util.InputStreamFileInput;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lz4 Decoder Plugin for Embulk.
 */
public class Lz4DecoderPlugin implements DecoderPlugin {
  private static final Logger LOGGER = LoggerFactory.getLogger(Lz4DecoderPlugin.class);

  /**
   * PluginTask for Lz4 Decoder Plugin.
   * <p>
   * This plugin has no configurable parameters.
   */
  public interface PluginTask extends Task {
    @ConfigInject
    public BufferAllocator getBufferAllocator();
  }

  @Override
  public void transaction(ConfigSource config, DecoderPlugin.Control control) {
    PluginTask task = config.loadConfig(PluginTask.class);

    control.run(task.dump());
  }

  @Override
  public FileInput open(TaskSource taskSource, FileInput fileInput) {
    final PluginTask task = taskSource.loadTask(PluginTask.class);

    final FileInputInputStream files = new FileInputInputStream(fileInput);

    return new InputStreamFileInput(
        task.getBufferAllocator(),
        new InputStreamFileInput.Provider() {
          public InputStream openNext() throws IOException {
            if (!files.nextFile()) {
              return null;
            }
            LOGGER.debug("generate LZ4FrameInputStream for {}", files.toString());
            return new LZ4FrameInputStream(files);
          }

          public void close() throws IOException {
            files.close();
          }
        }
    );
  }
}