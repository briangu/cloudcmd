package cloudcmd.common;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class StringUtil
{
  public static String join(Collection<?> s, String delimiter)
  {
    StringBuilder builder = new StringBuilder();
    Iterator iter = s.iterator();
    while (iter.hasNext())
    {
      builder.append(iter.next().toString());
      if (!iter.hasNext())
      {
        break;
      }
      builder.append(delimiter);
    }
    return builder.toString();
  }

  public static String repeat(int n, String s)
  {
    return String.format(String.format("%%0%dd", n), 0).replace("0", s);
  }

  public static List<String> repeatList(int n, String s)
  {
    List<String> list = new ArrayList<String>();

    for (int i = 0; i < n; i++)
    {
      list.add(s);
    }

    return list;
  }

  public static String joinRepeat(int size, String s, String delim)
  {
    return join(repeatList(size, s), delim);
  }
}
