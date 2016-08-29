CREATE DEFINER=`root`@`192.168.%` PROCEDURE `spider_pq_all`(IN `sHost` varchar(255),IN `Username` varchar(255),IN `sPassword` varchar(255),IN `iPort` int,IN `Databasename` varchar(255),IN `inSQL` text,IN `NewTable` varchar(255),IN `TempTableEngineType` varchar(255),IN `BeforeEndSQL` varchar(255),IN `MaxSQLCount` int,IN `TableWhere` varchar(255),IN `PQFunction` varchar(255),IN `NeedLinkeTable` int,IN `OnlyTable` int)
BEGIN
  DECLARE sTempTable VARCHAR(255);
  set sTempTable := run_spider_pq(sHost,Username,sPassword,iPort,Databasename,inSQL,NewTable,TempTableEngineType,BeforeEndSQL,MaxSQLCount,TableWhere,PQFunction,NeedLinkeTable,OnlyTable);
  SET @MQ_STMT := CONCAT("select * from ",Databasename,'.',sTempTable);
  PREPARE STMT FROM @MQ_STMT; 
  EXECUTE STMT;
  SET @MQ_STMT := CONCAT("DROP TABLE ",Databasename,'.',sTempTable);
  PREPARE STMT FROM @MQ_STMT; 
  EXECUTE STMT;
END