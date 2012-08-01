package cloudcmd.common;

import java.io.*;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class CryptoUtil {
  public static String digestToString(byte[] digest) {
    if (digest == null) return null;
    BigInteger bigInt = new BigInteger(1, digest);
    String hash = bigInt.toString(16);
    if (hash.length() == 63) hash = "0" + hash;
    return hash;
  }

  public static String computeHashAsString(File targetFile) throws IOException {
    return digestToString(computeHash(targetFile));
  }

  public static byte[] computeHash(File targetFile) throws IOException {
    FileInputStream fis = null;
    try {
      return computeDigest(Channels.newChannel(fis = new FileInputStream(targetFile)), "SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("SHA-256 is not supported");
    } finally {
      FileUtil.SafeClose(fis);
    }
  }

  public static byte[] computeMD5Hash(File targetFile) throws IOException {
    FileInputStream fis = null;
    try {
      return computeDigest(Channels.newChannel(fis = new FileInputStream(targetFile)), "MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("SHA-256 is not supported");
    } finally {
      FileUtil.SafeClose(fis);
    }
  }

  public static byte[] writeAndComputeHash(InputStream srcData, File destFile) throws IOException {
    FileOutputStream fos = null;
    try {
      return writeAndComputeHash(srcData, fos = new FileOutputStream(destFile));
    } finally {
      FileUtil.SafeClose(fos);
    }
  }

  public static byte[] writeAndComputeHash(InputStream is, OutputStream os) throws IOException {
    try {
      return writeAndComputeHash(Channels.newChannel(is), Channels.newChannel(os), "SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("SHA-256 is not supported");
    }
  }

  public static byte[] writeAndComputeHash(ReadableByteChannel in, WritableByteChannel out, String digestId) throws IOException, NoSuchAlgorithmException {
    MessageDigest md = MessageDigest.getInstance(digestId);
    ByteBuffer buff = ByteBuffer.allocate(1024 * 1024);
    while (in.read(buff) != -1) {
      buff.flip();
      md.update(buff);
      buff.flip();
      out.write(buff);
      buff.clear();
    }
    return md.digest();
  }

  public static String computeHashAsString(InputStream is) throws IOException {
    return digestToString(computeHash(is));
  }

  public static byte[] computeHash(InputStream is) throws IOException {
    try {
      return computeDigest(Channels.newChannel(is), "SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("SHA-256 is not supported");
    }
  }

  // http://stackoverflow.com/questions/9321912/very-slow-when-generaing-md5-using-java-with-large-file
  public static byte[] computeMD5Hash(ReadableByteChannel channel) throws IOException {
    try {
      return computeDigest(channel, "MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("MD5 is not supported");
    }
  }

  private static byte[] computeDigest(ReadableByteChannel channel, String digestId) throws IOException, NoSuchAlgorithmException {
    MessageDigest md = MessageDigest.getInstance(digestId);
    ByteBuffer buff = ByteBuffer.allocate(1024 * 1024);
    while (channel.read(buff) != -1) {
      buff.flip();
      md.update(buff);
      buff.clear();
    }
    return md.digest();
  }
}
