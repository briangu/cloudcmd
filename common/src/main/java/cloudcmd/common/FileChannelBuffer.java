package cloudcmd.common;

// https://raw.github.com/playframework/play/master/framework/src/play/server/FileChannelBuffer.java

import org.jboss.netty.buffer.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.*;


/**
 * Useless channel buffer only used to wrap the input stream....
 */
public class FileChannelBuffer extends AbstractChannelBuffer implements WrappedChannelBuffer {

  private final InputStream is;
  private final Integer _capacity;
  private final ReadableByteChannel _channel;

  public FileChannelBuffer(File file) {
    if (file == null) {
      throw new NullPointerException("file");
    }
    try {
      FileInputStream fis = new FileInputStream(file);
      this.is = fis;
      this.writerIndex(new Long(file.length()).intValue());
      _capacity = null;
      _channel = fis.getChannel();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public FileChannelBuffer(InputStream is, int capacity) {
    this.is = is;
    _capacity = capacity;
    _channel = Channels.newChannel(is);
    this.writerIndex(capacity);
  }

  public InputStream getInputStream() {
    return is;
  }

  public ChannelBuffer unwrap() {
    throw new RuntimeException();
  }

  public ChannelBufferFactory factory() {
    throw new RuntimeException();
  }

  public ByteOrder order() {
    return ByteOrder.BIG_ENDIAN;
  }

  public boolean isDirect() {
    return true;
  }

  public boolean hasArray() {
    return false;
  }

  public byte[] array() {
    throw new RuntimeException();
  }

  public int arrayOffset() {
    throw new RuntimeException();
  }

  @Override
  public void discardReadBytes() {
    throw new RuntimeException();
  }

  public void setByte(int index, byte value) {
    throw new RuntimeException();
  }

  public void setBytes(int index, ChannelBuffer src, int srcIndex, int length) {
    throw new RuntimeException();
  }

  public void setBytes(int index, byte[] src, int srcIndex, int length) {
    throw new RuntimeException();
  }

  public void setBytes(int index, ByteBuffer src) {
    throw new RuntimeException();
  }

  public void setShort(int index, short value) {
    throw new RuntimeException();
  }

  public void setMedium(int index, int value) {
    throw new RuntimeException();
  }

  public void setInt(int index, int value) {
    throw new RuntimeException();
  }

  public void setLong(int index, long value) {
    throw new RuntimeException();
  }

  public int setBytes(int index, InputStream in, int length)
    throws IOException {
    throw new RuntimeException();
  }

  public int setBytes(int index, ScatteringByteChannel in, int length)
    throws IOException {
    throw new RuntimeException();

  }

  public int readerIndex() {
    return 0;
  }


  public int getBytes(int index, GatheringByteChannel out, int length)
    throws IOException {
    byte[] b = new byte[length];
    is.read(b, index, length);
    ByteBuffer bb = ByteBuffer.wrap(b);
    return out.write(bb);
  }

  public void setByte(int i, int i1) {
    throw new RuntimeException();
  }

  public void getBytes(int index, OutputStream out, int length)
    throws IOException {
    byte[] b = new byte[length];
    is.read(b, index, length);
    out.write(b, index, length);
  }

  public void getBytes(int index, byte[] dst, int dstIndex, int length) {
    try {
      byte[] b = new byte[length];
      is.read(b, index, length);
      System.arraycopy(b, 0, dst, dstIndex, length);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void getBytes(int index, ChannelBuffer dst, int dstIndex, int length) {
    try {
      byte[] b = new byte[length];
      is.read(b, index, length);
      dst.writeBytes(b, dstIndex, length);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void getBytes(int index, ByteBuffer dst) {
    try {
      byte[] b = new byte[capacity() - index];
      is.read(b, index, b.length);
      dst.put(b);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public ChannelBuffer duplicate() {
    throw new RuntimeException();
  }

  public ChannelBuffer copy(int index, int length) {
    throw new RuntimeException();
  }

  public ChannelBuffer slice(int index, int length) {
    if (index != 0) {
      throw new IllegalArgumentException("indexing not supported");
    }

    ByteBuffer byteBuffer = ByteBuffer.allocate(length);
    try {
      while (byteBuffer.position() < byteBuffer.limit()) {
        _channel.read(byteBuffer);
      }
      byteBuffer.flip();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return ChannelBuffers.wrappedBuffer(byteBuffer);
/*
    byte[] buff = new byte[length];
    try {
      is.read(buff, index, length);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return ChannelBuffers.wrappedBuffer(buff);
*/
  }

  public byte getByte(int index) {
    throw new RuntimeException();
  }

  public short getShort(int index) {
    throw new RuntimeException();
  }

  public int getUnsignedMedium(int index) {
    throw new RuntimeException();

  }

  public int getInt(int index) {
    throw new RuntimeException();

  }

  public long getLong(int index) {
    throw new RuntimeException();

  }

  public ByteBuffer toByteBuffer(int index, int length) {
    throw new RuntimeException();
  }

  @Override
  public ByteBuffer[] toByteBuffers(int index, int length) {
    throw new RuntimeException();
  }

  public int capacity() {
    try {
      return _capacity == null ? is.available() : _capacity;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }

  public ChannelBuffer readBytes(int length) {
//          ChannelBuffer buf = ChannelBuffers.buffer(length);
//          getBytes(0, buf);
//          return buf;
    throw new RuntimeException();
  }

  public ChannelBuffer readSlice(int length) {
    throw new RuntimeException();
  }

  public void readBytes(byte[] dst, int dstIndex, int length) {
    checkReadableBytes(length);
    getBytes(0, dst, dstIndex, length);
  }

  public void readBytes(byte[] dst) {
    readBytes(dst, 0, dst.length);
  }

  public void readBytes(ChannelBuffer dst) {
    readBytes(dst, dst.writableBytes());
  }

  public void readBytes(ChannelBuffer dst, int length) {
    if (length > dst.writableBytes()) {
      throw new IndexOutOfBoundsException();
    }
    readBytes(dst, dst.writerIndex(), length);
    dst.writerIndex(dst.writerIndex() + length);
  }

  public void readBytes(ChannelBuffer dst, int dstIndex, int length) {
    getBytes(0, dst, dstIndex, length);
  }

  public void readBytes(ByteBuffer dst) {
    int length = dst.remaining();
    checkReadableBytes(length);
    getBytes(0, dst);
  }

  public int readBytes(GatheringByteChannel out, int length)
    throws IOException {
    checkReadableBytes(length);
    return getBytes(0, out, length);
  }

  public void readBytes(OutputStream out, int length) throws IOException {
    checkReadableBytes(length);
    getBytes(0, out, length);
  }

  public void setShort(int a, int b) {
    throw new RuntimeException();
  }
}