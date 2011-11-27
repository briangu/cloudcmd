package cloudcmd.common;

import java.util.Collection;
import java.util.Iterator;

public class StringUtil
{
  public static String join(Collection<?> s, String delimiter) {
    StringBuilder builder = new StringBuilder();
    Iterator iter = s.iterator();
    while (iter.hasNext()) {
       builder.append(iter.next().toString());
       if (!iter.hasNext()) {
         break;
       }
       builder.append(delimiter);
    }
    return builder.toString();
  }

  public static String repeat(int n, String s)
  {
    return String.format(String.format("%%0%dd", n), 0).replace("0",s);
  }
}
