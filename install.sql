use mysql;

DROP FUNCTION IF EXISTS run_parallel_query;
DROP FUNCTION IF EXISTS build_create_table_sql;
DROP FUNCTION IF EXISTS get_main_from;
DROP FUNCTION IF EXISTS is_select_sql;
DROP FUNCTION IF EXISTS my_milliseconds;
DROP FUNCTION IF EXISTS my_microseconds;
DROP FUNCTION IF EXISTS my_nanoseconds;
DROP FUNCTION IF EXISTS parallel_query_pool_size;
DROP FUNCTION IF EXISTS parallel_query_version;
DROP FUNCTION IF EXISTS run_spider_pq;

CREATE FUNCTION run_parallel_query RETURNS string SONAME 'parallel_query.so';
CREATE FUNCTION build_create_table_sql RETURNS string SONAME 'parallel_query.so';
CREATE FUNCTION get_main_from RETURNS string SONAME 'parallel_query.so';
CREATE FUNCTION is_select_sql RETURNS int SONAME 'parallel_query.so';
CREATE FUNCTION my_milliseconds RETURNS int SONAME 'parallel_query.so';
CREATE FUNCTION my_microseconds RETURNS int SONAME 'parallel_query.so';
CREATE FUNCTION my_nanoseconds RETURNS int SONAME 'parallel_query.so';
CREATE FUNCTION parallel_query_pool_size RETURNS int SONAME 'parallel_query.so';
CREATE FUNCTION parallel_query_version RETURNS string SONAME 'parallel_query.so';
CREATE FUNCTION run_spider_pq RETURNS string SONAME 'parallel_query.so';

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for parallel_query_pri_fields
-- ----------------------------
DROP TABLE IF EXISTS `parallel_query_pri_fields`;
CREATE TABLE `parallel_query_pri_fields` (
  `ID` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `TABLE_SCHEMA` varchar(64) CHARACTER SET utf8 NOT NULL DEFAULT '',
  `TABLE_NAME` varchar(64) CHARACTER SET utf8 NOT NULL DEFAULT '',
  `COLUMN_NAME` varchar(64) CHARACTER SET utf8 NOT NULL DEFAULT '',
  `DATA_TYPE` varchar(64) CHARACTER SET utf8 NOT NULL DEFAULT 'int',
  `COLUMN_KEY` varchar(3) CHARACTER SET utf8 NOT NULL DEFAULT 'PRI',
  `MAX_VALUE` int(10) DEFAULT '0' COMMENT 'max value',
  `MIN_VALUE` int(10) DEFAULT '0' COMMENT 'min value',
  PRIMARY KEY (`ID`),
  UNIQUE KEY `TABLE_SCHEMA` (`TABLE_SCHEMA`,`TABLE_NAME`) USING BTREE
) ENGINE=MyISAM AUTO_INCREMENT=0 DEFAULT CHARSET=gbk;

-- ----------------------------
-- Procedure structure for parallel_query
-- ----------------------------
DROP PROCEDURE IF EXISTS `parallel_query`;
DELIMITER ;;
CREATE DEFINER=`root`@`192.168.%` PROCEDURE `parallel_query`(IN `sDataBaseName` varchar(255),IN `in_sql` text,in `end_before` VARCHAR(255),IN `max_sql_count` int(10),in `iMin` BIGINT(20),in `iMax` BIGINT(20))
BEGIN
call parallel_query_all('localhost','root','root',3306,sDataBaseName,in_sql,max_sql_count,end_before,'','','','',iMin,iMax,'','','',1);
END
;;
DELIMITER ;

-- ----------------------------
-- Procedure structure for parallel_query_all
-- ----------------------------
DROP PROCEDURE IF EXISTS `parallel_query_all`;
DELIMITER ;;
CREATE DEFINER=`root`@`192.168.%` PROCEDURE `parallel_query_all`(in `sHost` VARCHAR(255),in `sUserName` VARCHAR(255),in `sPassword` VARCHAR(255),in `iPort` INT(10),in `sDataBaseName` CHAR(255),IN `in_sql` text,IN `max_sql_count` int(10),in `end_before` VARCHAR(255),in `end_sql` VARCHAR(255),in `sTrueTableName` CHAR(255),in `sAsTableName` CHAR(255),in `sPRIField` CHAR(255),in `iMin` BIGINT(20),in `iMax` BIGINT(20),in `sTempTableName` VARCHAR(255),in `sEngineType` VARCHAR(255),in `sEngineConnection` VARCHAR(255),in  `iClearTempTable` TINYINT(1))
label_exit:BEGIN
   DECLARE iPos INT(10) DEFAULT -1;#字符串定位
   DECLARE iNeedTempTable TINYINT(1) DEFAULT 1;
   DECLARE iPRIMax INT(10) DEFAULT 0;#parallel_query_pri_field表中的最大值
   DECLARE iPRIMin INT(10) DEFAULT 0;#parallel_query_pri_field表中的最小值

   IF sHost = '' THEN
      SET sHost := 'localhost';#发起新的MySQL连接时的地址
   END IF;

   IF sUserName = '' THEN
      SET sUserName := 'root';#MySQL用户名
   END IF;

   IF sPassword = '' THEN
      SET sPassword := '';#对应密码
   END IF;

   IF iPort = 0 THEN
      SET iPort := 3306;#MySQL端口号
   END IF;

   IF sDataBaseName = '' THEN
      SET sDataBaseName := database();#当前数据库
   ELSE
      SET sDataBaseName := TRIM(sDataBaseName);
   END IF;
set @DataBaseName := sDataBaseName;

   IF sTempTableName = '' THEN
      SET sTempTableName := 'MQ_TEMP_';#当前数据库
   END IF;

   IF sEngineType = '' THEN
      SET sEngineType := 'InnoDB';#临时表存储引擎
   END IF;

   IF sTrueTableName = '' THEN
      SET sTrueTableName := get_main_from(in_sql);#获取主表名和主表别名以“,”分割
      SET iPos := LOCATE(',',sTrueTableName);
      SET sAsTableName := MID(sTrueTableName,iPos+1,CHAR_LENGTH(sTrueTableName)-iPos);#获取主表别名
      SET sTrueTableName := MID(sTrueTableName,1,CHAR_LENGTH(sTrueTableName)-CHAR_LENGTH(sAsTableName)-1);#获取主表名
   END IF;

   
   #获取主表的整数型主键
   SELECT COLUMN_NAME,MAX_VALUE,MIN_VALUE INTO sPRIField,iPRIMax,iPRIMin FROM mysql.parallel_query_pri_fields WHERE table_schema = sDataBaseName AND table_name = sTrueTableName limit 1;
   IF sPRIField = ''  then
      SELECT column_name INTO sPRIField FROM information_schema.columns WHERE table_schema = sDataBaseName AND table_name = sTrueTableName AND column_key = 'PRI' AND DATA_TYPE LIKE '%int%' limit 1;
      IF sPRIField = ''  then
         #发现没有整数型主键字段就报错退出
         SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT = 'PRI Field Error';
         LEAVE label_exit;#直接退出整个存储过程
      END IF;
   ELSE
     IF iMin = 0 and iMax = 0 THEN
         SET iMin := iPRIMin;
         SET iMax := iPRIMax;
      END IF;
   END IF;
   
   #获取主表主键的最大值和最小值
   IF iMin = 0 and iMax = 0 THEN
      SET @MQ_STMT :=CONCAT("select min(",sPRIField,"),max(",sPRIField,") into @MQ_PRIField_Min,@MQ_PRIField_Max from ",sDataBaseName,".",sTrueTableName);
      PREPARE STMT FROM @MQ_STMT; 
      EXECUTE STMT;
      SELECT @MQ_PRIField_Min,@MQ_PRIField_Max INTO iMin,iMax;
   END IF;
   
   SELECT NULL,NULL,NULL INTO @MQ_STMT,@MQ_PRIField_Min,@MQ_PRIField_Max;#设置空不用的用户变量

   SET @MQ_1sAsTableName := TRIM(sAsTableName);
   SET @MQ_1PRIField := TRIM(sPRIField);
   SET iNeedTempTable := is_select_sql(in_sql);#分析是否是Select语句如果是就需要创建临时表

   IF iNeedTempTable THEN
      #创建临时表
      SET sTempTableName := CONCAT(sTempTableName,my_nanoseconds());#前缀 + 临时表的序号 就是完整的临时表名
      #SET @MQ_STMT :=build_create_table_sql(in_sql,CONCAT(sDataBaseName,'.',sTempTableName),sEngineType,sEngineConnection);#获取创建临时表的SQL语句
      #PREPARE STMT FROM @MQ_STMT;
      #EXECUTE STMT;
      #SET @MQ_STMT := NULL;

      #执行多线程查询
      SET @MQ_STMT :=run_parallel_query(@DataBaseName,in_sql,@MQ_1sAsTableName,@MQ_1PRIField,iMin,iMax,max_sql_count,TRIM(sTempTableName),"innodb",TRIM(end_before),TRIM(end_sql),
                                        TRIM(sHost),TRIM(sUserName),TRIM(sPassword),iPort);
      IF (SUBSTR(@MQ_STMT,1,1)="~") THEN #如果返回的第一个字符是 “~”就表示出错了 
         #删除临时表
         SET @MQ_STMT := CONCAT("DROP TABLE ",sDataBaseName,'.',sTempTableName);
         PREPARE STMT FROM @MQ_STMT;
         EXECUTE STMT;
         SET @MQ_STMT := NULL;

         SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT = @MQ_STMT;
         SET @MQ_STMT := NULL;
         LEAVE label_exit;#直接退出整个存储过程
      ELSE
         #如果成功就执行最后的汇总查询语句
         PREPARE STMT FROM @MQ_STMT;
         EXECUTE STMT;
         SET @MQ_STMT := NULL;
      END IF;
      
      #删除临时表
      SET @MQ_STMT := CONCAT("DROP TABLE ",sDataBaseName,".",sTempTableName);
      PREPARE STMT FROM @MQ_STMT;
      EXECUTE STMT;
      SET @MQ_STMT := NULL;
   ELSE
      SET @MQ_STMT :=run_parallel_query(@DataBaseName,in_sql,@MQ_1sAsTableName,@MQ_1PRIField,iMin,iMax,max_sql_count,TRIM(sTempTableName),"innodb",TRIM(end_before),TRIM(end_sql),
                                        TRIM(sHost),TRIM(sUserName),TRIM(sPassword),iPort);
      IF (SUBSTR(@MQ_STMT,1,1)="~") THEN #如果返回的第一个字符是 “~”就表示出错了 
         SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT = @MQ_STMT;
         SET @MQ_STMT := NULL;
         LEAVE label_exit;#直接退出整个存储过程
      ELSE
         SELECT 'ok' as `return`;
      END IF;
   END IF;
END
;;
DELIMITER ;

-- ----------------------------
-- Procedure structure for spider_pq
-- ----------------------------
DROP PROCEDURE IF EXISTS `spider_pq`;
DELIMITER ;;
CREATE DEFINER=`root`@`192.168.%` PROCEDURE `spider_pq`(IN `Host` varchar(255),IN `Username` varchar(255),IN `Password` varchar(255),IN `Port` int,IN `Databasename` varchar(255),IN `inSQL` text,IN `BeforeEndSQL` varchar(255))
BEGIN
  call spider_pq_all(Host,Username,Password,Port,Databasename,inSQL,"","",BeforeEndSQL,0,"","",1,0);
END
;;
DELIMITER ;

-- ----------------------------
-- Procedure structure for spider_pq_all
-- ----------------------------
DROP PROCEDURE IF EXISTS `spider_pq_all`;
DELIMITER ;;
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
;;
DELIMITER ;
