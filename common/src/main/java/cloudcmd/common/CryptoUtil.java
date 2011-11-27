package cloudcmd.common;

import java.io.*;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class CryptoUtil
{
  public static String computeHashAsString(File targetFile)
  {
    byte[] digest = computeHash(targetFile);
    if (digest == null) return null;
    BigInteger bigInt = new BigInteger(1, digest);
    String hash = bigInt.toString(16);
    return hash;
  }

  public static String computeHashAsString(InputStream is)
  {
    byte[] digest = computeHash(is);
    if (digest == null) return null;
    BigInteger bigInt = new BigInteger(1, digest);
    String hash = bigInt.toString(16);
    return hash;
  }

  public static byte[] computeHash(File targetFile)
  {
    byte[] digest = null;

    RandomAccessFile file = null;
    int buff = 16384;
    try
    {
      file = new RandomAccessFile(targetFile, "r");

      MessageDigest hash = MessageDigest.getInstance("SHA-256");

      byte[] buffer = new byte[buff];
      long read = 0;
      long offset = file.length();
      int unitsize;

      while (read < offset)
      {
        unitsize = (int) (((offset - read) >= buff) ? buff : (offset - read));
        file.read(buffer, 0, unitsize);
        hash.update(buffer, 0, unitsize);
        read += unitsize;
      }

      digest = hash.digest();
    }
    catch (FileNotFoundException e)
    {
      e.printStackTrace();
    }
    catch (NoSuchAlgorithmException e)
    {
      e.printStackTrace();
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
    finally
    {
      if (file != null) try
      {
        file.close();
      }
      catch (IOException e)
      {
        e.printStackTrace();
      }
    }

    return digest;
  }

  public static byte[] computeHash(InputStream is)
  {
    byte[] digest = null;

    int buff = 16384;
    try
    {
      MessageDigest hash = MessageDigest.getInstance("SHA-256");

      byte[] buffer = new byte[buff];
      long read = 0;
      long offset = is.available();
      int unitsize;

      while (read < offset)
      {
        unitsize = (int) (((offset - read) >= buff) ? buff : (offset - read));
        is.read(buffer, 0, unitsize);
        hash.update(buffer, 0, unitsize);
        read += unitsize;
      }

      digest = hash.digest();
    }
    catch (FileNotFoundException e)
    {
      e.printStackTrace();
    }
    catch (NoSuchAlgorithmException e)
    {
      e.printStackTrace();
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }

    return digest;
  }
}
