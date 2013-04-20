package cloudcmd.common;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;

public class RandomAccessFileInputStream extends InputStream {

  private RandomAccessFile file = null;
  private int header = 0;
  private int size = 0;

  public static RandomAccessFileInputStream create(File file) throws FileNotFoundException {
    return new RandomAccessFileInputStream(new RandomAccessFile(file, "r"));
  }

  public RandomAccessFileInputStream(RandomAccessFile raf) {
    file = raf;
    try {
      size = (int) file.length();
    } catch (Exception e) {

    }
  }

  public RandomAccessFileInputStream(RandomAccessFile raf, int header, int length) {
    file = raf;
    this.header = header;
    this.size = length;
  }

  @Override
  public int read() throws IOException {
    return file.read();

  }

  @Override
  public int read(byte[] b) throws IOException {
    return file.read(b);
  }

  @Override
  public int read(byte[] b, int offset, int length) throws IOException {
    return file.read(b, offset, length);
  }

  @Override
  public boolean markSupported() {
    return true;
  }

  @Override
  public void mark(int limit) {
  }

  @Override
  public int available() throws IOException {
    return (int) (size - (file.getFilePointer() - header));
  }

  @Override
  public long skip(long byteCount) throws IOException {
    long start = file.getFilePointer();
    file.skipBytes((int) byteCount);
    return file.getFilePointer() - start;
  }

  @Override
  public void close() throws IOException {
    file.close();
  }

  @Override
  public synchronized void reset() throws IOException {
    file.seek(header);
  }
}
