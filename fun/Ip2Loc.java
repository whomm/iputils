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
import java.text.SimpleDateFormat;  
import java.util.Date;  

public class Ip2Loc extends UDF {

    private Map<Integer, Map<String,String>> indexInfoMap;
    private ArrayList<Long> ipindex;

    public int  binarySearch(long x)
    {
        int maxlow = ipindex.size() - 1;
        int low = 0, high = maxlow;
        while (low <= high)
        {
            int mid = (low + high) / 2;

//System.out.println("x:"+x+"low:"+low+"high:"+high+"mid:"+mid+"midv:"+ipindex.get(mid));
            if (ipindex.get(mid) < x )
            {
                low = mid+1;
            } else if (ipindex.get(mid) > x)
            {
                high = mid-1;
            } else
            {
		low = mid;
                break;
            }
        }
	if(low%2 == 0 && low < maxlow)
	{
		if(ipindex.get(low)<=x && ipindex.get(low+1)>=x)
		{//落在low开始的段
			return low/2;
		}
	}
	if(low%2 >0 && low >0)
	{
        	if (ipindex.get(low)>=x && ipindex.get(low-1)<=x) 
                {//落在low结束的段
            		return (low-1)/2;
		}
        }
//System.out.println("low:"+low);
        return -1;
    }

    public Map<String,String> evaluate(String Ip, String mapFile) throws HiveException {
        if (indexInfoMap == null) {
            indexInfoMap = new HashMap<Integer, Map<String,String>>();
            try {
        	//System.out.println((new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(new Date())+" begin to load"); 
                BufferedReader lineReader = new BufferedReader(new FileReader(mapFile));
                ipindex = new ArrayList<Long>();
                int index = 0;
                String line = null;
                while ((line = lineReader.readLine()) != null) {
		    if(line.substring(0, 1).equals("#"))
		    {
			continue;
		    }

                    String[] pair = line.split("\\|");
                    long startip = Ip2Long.ip2long(pair[0]);
                    long endip = Ip2Long.ip2long(pair[1]);
                    //country|isp|province|city|
                    Map<String,String> info = new HashMap<String,String>();
                    info.put("country",pair[2]);
                    info.put("isp",pair[3]);
                    info.put("province",pair[4]);
                    info.put("city",pair[5]);
                    info.put("zone",pair[6]);

                    indexInfoMap.put(index, info);
                    ipindex.add(startip);
                    ipindex.add(endip);

                    index++;
                }
        	//System.out.println((new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(new Date())+" success load"); 
            } catch (FileNotFoundException e) {
                throw new HiveException(mapFile + " doesn't exist");
            } catch (IOException e) {
                throw new HiveException("process file " + mapFile + " failed, please check format");
            }

        }
        long x = Ip2Long.ip2long(Ip);
        if (x == 0) {

            Map<String,String> info = new HashMap<String,String>();
            info.put("country","None");
            info.put("isp","None");
            info.put("province","None");
            info.put("city","None");
            info.put("zone","None");
            return info;
        }
        else
        {
	    int index = binarySearch(x);
            Map<String,String> info =indexInfoMap.get(index);
            if (info == null) {

                info = new HashMap<String,String>();
                info.put("country","None");
                info.put("isp","None");
                info.put("province","None");
                info.put("city","None");
		info.put("zone","None");
                return info;
            }
            return info;
        }
    }
    public static void main(String[] argvs) {
	try{
        	Ip2Loc ipl = new Ip2Loc();
		long a=System.currentTimeMillis();
		System.out.println("1.0.8.0|1.0.15.255|CN|CHINANET|广东|None|None|83|90|85|0|0");
        	System.out.println(ipl.evaluate("1.0.8.1","./colombo_iplib.txt").toString());
        	System.out.println(ipl.evaluate("1.0.8.0","./colombo_iplib.txt").toString());
        	System.out.println(ipl.evaluate("1.0.8.99","./colombo_iplib.txt").toString());
        	System.out.println(ipl.evaluate("1.0.14.33","./colombo_iplib.txt").toString());
        	System.out.println(ipl.evaluate("1.0.15.255","./colombo_iplib.txt").toString());
		System.out.println(ipl.evaluate("1.0.15.251","./colombo_iplib.txt").toString());
		System.out.println("1.3.0.0|1.3.3.255|CN|CHINANET|广东|广州|None|100|90|90|50|0");
		System.out.println(ipl.evaluate("1.3.0.76","./colombo_iplib.txt").toString());
		System.out.println("1.2.2.0|1.2.2.255|CN|OTHER|北京|北京|海淀|100|90|90|90|30");
		System.out.println(ipl.evaluate("1.2.2.89","./colombo_iplib.txt").toString());
		System.out.println("1.15.97.0|1.15.97.255|CN|FOUNDERBN|北京|北京|朝阳|100|90|90|90|30");
		System.out.println(ipl.evaluate("1.15.97.255","./colombo_iplib.txt").toString());
		System.out.println(ipl.evaluate("1.15.97.0","./colombo_iplib.txt").toString());
		System.out.println(ipl.evaluate("1.15.97.3","./colombo_iplib.txt").toString());
		System.out.println("0.0.0.0|0.255.255.255|ZZ|None|None|None|None|100|0|0|0|0");
		System.out.println(ipl.evaluate("0.0.0.0","./colombo_iplib.txt").toString());
		System.out.println(ipl.evaluate("0.255.255.255","./colombo_iplib.txt").toString());
		System.out.println(ipl.evaluate("0.2.255.255","./colombo_iplib.txt").toString());
		System.out.println("\n执行耗时 : "+(System.currentTimeMillis()-a)/1000f+" 秒 ");
		a=System.currentTimeMillis();
		System.out.println(ipl.evaluate("118.144.130.74","./colombo_iplib.txt").toString());
		System.out.println("\n执行耗时 : "+(System.currentTimeMillis()-a)/1000f+" 秒 ");
	}
	catch(Exception e)
	{
		e.printStackTrace();
		System.out.println("error"+e.getMessage());
	}
    }
}
