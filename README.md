# iputils
hive udf for ip2loc  and ip2long

## how to build

use ./build.sh to build a jar

## how to use

hive shell like this:
```
add jar iputils.jar;
create temporary function Ip2Long as 'fun.Ip2Long';
select Ip2Long('192.168.1.1') from tmp.test limit 1;

create temporary function Ip2Loc as 'fun.Ip2Loc';
ADD FILE ./colombo_iplib.txt;
select Ip2Loc(ip, './colombo_iplib.txt') from tmp.testip;
```
Ip2Loc return：hashmap：{isp=FOUNDERBN, province=北京, zone=朝阳, city=北京, country=CN}   运营商 省    区（zone） 市（city）  国。
Ip2Long return 0 if error
