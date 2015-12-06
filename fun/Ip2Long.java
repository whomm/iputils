package fun;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;


public class Ip2Long extends UDF {
    public static long ip2long(String ip) {
	if (ip.matches("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}")) {
            try {
		
		String[] ips = ip.split("[.]");
		long ipNum = 0;
		if (ips == null) {
		    return 0;
		}
		for (int i = 0; i < ips.length; i++) {
		    ipNum = ipNum << Byte.SIZE | Long.parseLong(ips[i]);
		}
 
        	return ipNum;
            } catch (Exception e) {
                return 0;
            }
        } else {
            return 0;
        } 
    }
 
    public long evaluate(String ip) {
	return ip2long(ip);
    }
 
    public static void main(String[] argvs) {
        Ip2Long ipl = new Ip2Long();
        System.out.println(ip2long("112.64.106.238"));
        System.out.println(ipl.evaluate("58.35.186.62"));
    }
}

