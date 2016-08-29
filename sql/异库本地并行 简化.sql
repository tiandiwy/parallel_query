CREATE DEFINER=`root`@`192.168.%` PROCEDURE `parallel_query`(IN `sDataBaseName` varchar(255),IN `in_sql` text,in `end_before` VARCHAR(255),IN `thread_num` int(10),in `iMin` BIGINT(20),in `iMax` BIGINT(20))
BEGIN
call parallel_query_all('localhost','root','root',3306,sDataBaseName,in_sql,thread_num,end_before,'','','','',iMin,iMax,'','','',1);
END