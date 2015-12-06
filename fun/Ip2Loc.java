package fun;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;

public class Ip2Loc extends UDF {

    private Map<Long, Map<String,String>> indexInfoMap;
    private ArrayList<Long> ipindex;

    public long binarySearch(long x)
    {
        //只比较偶数
        int low = 0, high = ipindex.size() - 2;
        while (low <= high)
        {
            int mid = (low + high) / 2;
            if (ipindex.get(mid).compareTo(x) < 0)
            {
                low = mid + 2;
            } else if (ipindex.get(mid).compareTo(x) > 0)
            {
                high = mid - 2;
            } else
            {
                return mid/2;
            }
        }
        if (ipindex.get(low)<=x && ipindex.get(low+1)>=x) {
            return low/2;
        }
        return -1;
    }

    public Map<String,String> evaluate(String Ip, String mapFile) throws HiveException {
        if (indexInfoMap == null) {
            indexInfoMap = new HashMap<Long, Map<String,String>>();
            try {
                BufferedReader lineReader = new BufferedReader(new FileReader(mapFile));
                ipindex = new ArrayList<Long>();
                long index = 0;
                String line = null;
                while ((line = lineReader.readLine()) != null) {
                    String[] pair = line.split("|");
                    long startip = Ip2Long.ip2long(pair[0]);
                    long endip = Ip2Long.ip2long(pair[1]);
                    //country|isp|province|city|
                    Map<String,String> info = new HashMap<String,String>();
                    info.put("country",pair[2]);
                    info.put("isp",pair[3]);
                    info.put("province",pair[4]);
                    info.put("city",pair[5]);

                    indexInfoMap.put(index, info);
                    ipindex.add(startip);
                    ipindex.add(endip);

                    index++;
                }
            } catch (FileNotFoundException e) {
                throw new HiveException(mapFile + " doesn't exist");
            } catch (IOException e) {
                throw new HiveException("process file " + mapFile + " failed, please check format");
            }

        }
        long x = Ip2Long.ip2long(Ip);
        if (x == -1) {

            Map<String,String> info = new HashMap<String,String>();
            info.put("country","None");
            info.put("isp","None");
            info.put("province","None");
            info.put("city","None");
            return info;
        }
        else
        {
            Map<String,String> info =indexInfoMap.get(x);
            if (info == null) {

                info = new HashMap<String,String>();
                info.put("country","None");
                info.put("isp","None");
                info.put("province","None");
                info.put("city","None");
                return info;
            }
            return info;
        }
    }
    public static void main(String[] argvs) {
	try{
        Ip2Loc ipl = new Ip2Loc();
        System.out.println(ipl.evaluate("58.35.186.62","./colombo_iplib.txt").toString());
	}
	catch(Exception e)
	{
		System.out.println(e.getMessage());
	}
    }
}
