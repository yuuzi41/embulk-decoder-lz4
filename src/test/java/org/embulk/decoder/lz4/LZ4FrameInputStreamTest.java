package org.embulk.decoder.lz4;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Unit test code for LZ4FrameInputStream.
 */
public class LZ4FrameInputStreamTest {
  private byte[] testLz4FrameUncompressed;
  private byte[] getTestLz4FrameCompressed;

  @Before
  public void setUp() throws Exception {
    // "abcd\n"
    this.testLz4FrameUncompressed = new byte[]{
        (byte) 0x04, (byte) 0x22, (byte) 0x4d, (byte) 0x18,
        (byte) 0x64, (byte) 0x40, (byte) 0xa7, (byte) 0x05, (byte) 0x00, (byte) 0x00, (byte) 0x80,
        (byte) 0x61, (byte) 0x62, (byte) 0x63, (byte) 0x64, (byte) 0x0a,
        (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
        (byte) 0xfd, (byte) 0xc0, (byte) 0xe2, (byte) 0x70
    };

    // "aaaabbbbccccddddaaaabbbbccccdddd\n"
    this.getTestLz4FrameCompressed = new byte[]{
        (byte) 0x04, (byte) 0x22, (byte) 0x4d, (byte) 0x18,
        (byte) 0x64, (byte) 0x40, (byte) 0xa7, (byte) 0x1a, (byte) 0x00, (byte) 0x00, (byte) 0x00,
        (byte) 0xf8, (byte) 0x01, (byte) 0x61, (byte) 0x61, (byte) 0x61, (byte) 0x61,
        (byte) 0x62, (byte) 0x62, (byte) 0x62, (byte) 0x62,
        (byte) 0x63, (byte) 0x63, (byte) 0x63, (byte) 0x63,
        (byte) 0x64, (byte) 0x64, (byte) 0x64, (byte) 0x64,
        (byte) 0x10, (byte) 0x00, (byte) 0x50, (byte) 0x64, (byte) 0x64, (byte) 0x64, (byte) 0x64,
        (byte) 0x0a, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
        (byte) 0x94, (byte) 0x5c, (byte) 0x06, (byte) 0x5c
    };
  }

  @Test
  public void testLoadUncompressed() throws Exception {
    InputStream in = new ByteArrayInputStream(testLz4FrameUncompressed);

    LZ4FrameInputStream decompressed = new LZ4FrameInputStream(in);
    BufferedReader reader = new BufferedReader(new InputStreamReader(decompressed));

    assertEquals("abcd", reader.readLine());
  }

  @Test
  public void testLoadLz4Compressed() throws Exception {
    InputStream in = new ByteArrayInputStream(getTestLz4FrameCompressed);

    LZ4FrameInputStream decompressed = new LZ4FrameInputStream(in);
    BufferedReader reader = new BufferedReader(new InputStreamReader(decompressed));

    assertEquals("aaaabbbbccccddddaaaabbbbccccdddd", reader.readLine());
  }
}
