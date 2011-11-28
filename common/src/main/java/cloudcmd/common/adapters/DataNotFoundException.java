package cloudcmd.common.adapters;

public class DataNotFoundException extends Exception
{
  public String Hash;

  public DataNotFoundException(String hash)
  {
    Hash = hash;
  }
}
