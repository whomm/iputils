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
