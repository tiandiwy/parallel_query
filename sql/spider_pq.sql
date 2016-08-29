CREATE DEFINER=`root`@`192.168.%` PROCEDURE `spider_pq`(IN `Host` varchar(255),IN `Username` varchar(255),IN `Password` varchar(255),IN `Port` int,IN `Databasename` varchar(255),IN `inSQL` text,IN `BeforeEndSQL` varchar(255))
BEGIN
  call spider_pq_all(Host,Username,Password,Port,Databasename,inSQL,"","",BeforeEndSQL,0,"","",1,0);
END