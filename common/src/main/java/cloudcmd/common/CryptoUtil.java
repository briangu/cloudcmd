package cloudcmd.common;

import java.io.*;
import java.math.BigInteger;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

public class CryptoUtil
{
  public static String computeHashAsString(File targetFile)
  {
    return digestToString(computeHash(targetFile));
  }

  public static String computeHashAsString(InputStream is)
  {
    return digestToString(computeHash(is));
  }

  public static String digestToString(byte[] digest)
  {
    if (digest == null) return null;
    BigInteger bigInt = new BigInteger(1, digest);
    String hash = bigInt.toString(16);
    if (hash.length() == 63) hash = "0" + hash;
    return hash;
  }

  public static byte[] computeHash(File targetFile)
  {
    byte[] digest = null;

    FileInputStream fis = null;
    DigestInputStream dis = null;
    final int buff = 1024 * 1024;
    try
    {
      fis = new FileInputStream(targetFile);
      MessageDigest hash = MessageDigest.getInstance("SHA-256");
      dis = new DigestInputStream(fis, hash);
      byte[] buffer = new byte[buff];
      while (dis.read(buffer) != -1) {}
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
      if (dis != null)
      {
        try
        {
          dis.close();
        }
        catch (IOException e)
        {
          e.printStackTrace();
        }
      }
      if (fis != null)
      {
        try
        {
          fis.close();
        }
        catch (IOException e)
        {
          e.printStackTrace();
        }
      }
    }

    return digest;
  }

  public static byte[] writeAndComputeHash(InputStream srcData, File destFile)
  {
    byte[] digest = null;

    DigestInputStream dis = null;
    FileOutputStream fos = null;

    final int buff = 1024 * 1024;
    try
    {
      MessageDigest hash = MessageDigest.getInstance("SHA-256");
      dis = new DigestInputStream(srcData, hash);
      fos = new FileOutputStream(destFile);
      byte[] buffer = new byte[buff];
      int read;
      while ((read = dis.read(buffer)) != -1) {
        fos.write(buffer, 0, read);
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
      FileUtil.SafeClose(dis);
      FileUtil.SafeClose(fos);
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
