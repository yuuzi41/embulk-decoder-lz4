package org.embulk.decoder.lz4;

import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * LZ4 Frame Format Decoding InputStream.
 *
 * This filter is decompressing the stream formatted by lz4 frame format such as .lz4 file.
 *
 * @see <a href="https://github.com/lz4/lz4/wiki/lz4_Frame_format.md">lz4 Frame format</a>
 */
public class LZ4FrameInputStream extends FilterInputStream {
  private boolean flagBlockChecksum = false;
  private boolean flagContentSize = false;
  private boolean flagContentChecksum = false;
  private long contentSize = 0;
  private int decompressMaxSize = 0;

  private boolean flagFrameStart = false;
  private boolean flagFrameEnd = false;

  private LZ4SafeDecompressor decompressor;

  private byte[] decompressBuffer;
  private int decompressBufferOffset = Integer.MAX_VALUE;
  private int decompressBufferSize = 0;

  private final Logger logger = LoggerFactory.getLogger(LZ4FrameInputStream.class);

  public LZ4FrameInputStream(InputStream in) {
    super(in);
    this.decompressor = LZ4Factory.fastestInstance().safeDecompressor();
  }

  @Override
  public int read() throws IOException {
    if (this.flagFrameEnd) {
      return -1;
    }

    if (this.decompressBufferOffset >= this.decompressBufferSize) {
      this.refill();
    }

    if (this.flagFrameEnd) {
      return -1;
    }

    int ret = this.decompressBuffer[this.decompressBufferOffset] & 0xff;
    this.decompressBufferOffset++;

    return ret;
  }

  @Override
  public int read(byte[] bytes) throws IOException {
    return this.read(bytes, 0, bytes.length);
  }

  @Override
  public int read(byte[] bytes, int idx, int length) throws IOException {
    if (this.flagFrameEnd) {
      return -1;
    }

    if (this.decompressBufferOffset >= this.decompressBufferSize) {
      this.refill();
    }

    if (this.flagFrameEnd) {
      return -1;
    }

    final int readable = this.decompressBufferSize - this.decompressBufferOffset;
    final int actualLength = Math.min(length, readable);

    System.arraycopy(
        this.decompressBuffer, this.decompressBufferOffset,
        bytes, idx, actualLength
    );
    this.decompressBufferOffset += actualLength;

    return actualLength;
  }

  @Override
  public long skip(long length) throws IOException {
    if (this.flagFrameEnd) {
      return -1;
    }

    if (this.decompressBufferOffset >= this.decompressBufferSize) {
      this.refill();
    }

    if (this.flagFrameEnd) {
      return -1;
    }

    final int readable = this.decompressBufferSize - this.decompressBufferOffset;
    final int actualLength = Math.min((int) length, readable);

    this.decompressBufferOffset += actualLength;
    return (long) actualLength;
  }

  @Override
  public int available() throws IOException {
    return this.decompressBufferSize - this.decompressBufferOffset;
  }

  @Override
  public void close() throws IOException {
    super.close();
    this.flagFrameEnd = true;
  }

  private void refill() throws IOException {
    if (!this.flagFrameStart) {
      byte[] compressedBuffer = new byte[11];
      this.readFully(compressedBuffer, 0, 6);

      if (compressedBuffer[0] != 0x04
          || compressedBuffer[1] != 0x22
          || compressedBuffer[2] != 0x4D
          || compressedBuffer[3] != 0x18) {
        throw new IOException("Illegal LZ4 Magic Number");
      }

      if ((compressedBuffer[4] & 0xc0) != 0x40) {
        throw new IOException("Unsupported LZ4 version");
      }

      flagBlockChecksum = ((compressedBuffer[4] & 0x10) != 0x00);
      flagContentSize = ((compressedBuffer[4] & 0x08) != 0x00);
      flagContentChecksum = ((compressedBuffer[4] & 0x04) != 0x00);

      switch ((compressedBuffer[5] & 0x70) >> 4) {
        case 4:
          decompressMaxSize = 64 * 1024;
          break;
        case 5:
          decompressMaxSize = 256 * 1024;
          break;
        case 6:
          decompressMaxSize = 1024 * 1024;
          break;
        case 7:
        default:
          decompressMaxSize = 4 * 1024 * 1024;
          break;
      }
      this.decompressBuffer = new byte[this.decompressMaxSize];

      if (flagContentSize) {
        this.readFully(compressedBuffer, 6, 9);
      } else {
        this.readFully(compressedBuffer, 6, 1);
      }

      this.flagFrameStart = true;
    }
    final byte[] blockSizeBuffer = new byte[4];
    int blockSize;
    this.readFully(blockSizeBuffer, 0, 4);
    blockSize = (blockSizeBuffer[0] & 0xff) | ((blockSizeBuffer[1] & 0xff) << 8)
        | ((blockSizeBuffer[2] & 0xff) << 16) | ((blockSizeBuffer[3] & 0xff) << 24);

    if (blockSize == 0) {
      if (this.flagContentChecksum) {
        final byte[] contentChecksumBuffer = new byte[4];
        this.readFully(contentChecksumBuffer, 0, 4);
      }
      this.flagFrameEnd = true;
    } else {
      // read data
      final byte[] blockCompressedBuffer = new byte[blockSize & 0x7fffffff];
      this.readFully(blockCompressedBuffer, 0, blockSize & 0x7fffffff);

      if (this.flagBlockChecksum) {
        final byte[] blockChecksumBuffer = new byte[4];
        this.readFully(blockChecksumBuffer, 0, 4);
      }

      //--------
        /*
        for (int i = 0; i < blockCompressedBuffer.length; i++) {
          if ((i % 16) == 0) {
            System.out.print(String.format("%08x : ", i));
          }
          System.out.print(String.format("%02x ", blockCompressedBuffer[i]));
          if ((i % 16) == 15) {
            System.out.println();
          }
        }
        */
      //--------

      if ((blockSize & 0x80000000) == 0) {
        this.decompressBufferSize =
            this.decompressor.decompress(blockCompressedBuffer, this.decompressBuffer);
        logger.info("load {} bytes, lz4, decompressed to {} bytes.",
            (blockSize & 0x7fffffff), this.decompressBufferSize);
      } else {
        System.arraycopy(
            blockCompressedBuffer, 0,
            this.decompressBuffer, 0, blockCompressedBuffer.length
        );
        this.decompressBufferSize = (blockSize & 0x7fffffff);
        logger.info("load {} bytes, uncompressed.", (blockSize & 0x7fffffff));
      }
      this.decompressBufferOffset = 0;
    }
  }

  private void readFully(byte[] buffer, int offset, int length) throws IOException {
    int read = 0;
    while (read < length) {
      final int r = this.in.read(buffer, read + offset, length - read);
      if (r < 0) {
        throw new EOFException("Stream ended prematurely");
      }
      read += r;
    }
    assert length == read;
  }
}
