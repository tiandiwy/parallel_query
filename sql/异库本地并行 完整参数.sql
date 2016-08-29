-- ----------------------------
-- Procedure structure for parallel_query_all
-- ----------------------------
DROP PROCEDURE IF EXISTS `parallel_query_all`;
DELIMITER ;;
CREATE DEFINER=`root`@`192.168.%` PROCEDURE `parallel_query_all`(in `sHost` VARCHAR(255),in `sUserName` VARCHAR(255),in `sPassword` VARCHAR(255),in `iPort` INT(10),in `sDataBaseName` CHAR(255),IN `in_sql` text,IN `thread_num` int(10),in `end_before` VARCHAR(255),in `end_sql` VARCHAR(255),in `sTrueTableName` CHAR(255),in `sAsTableName` CHAR(255),in `sPRIField` CHAR(255),in `iMin` BIGINT(20),in `iMax` BIGINT(20),in `sTempTableName` VARCHAR(255),in `sEngineType` VARCHAR(255),in `sEngineConnection` VARCHAR(255),in  `iClearTempTable` TINYINT(1))
label_exit:BEGIN
   DECLARE iPos INT(10) DEFAULT -1;#�ַ�����λ
   DECLARE iNeedTempTable TINYINT(1) DEFAULT 1;
   DECLARE iPRIMax INT(10) DEFAULT 0;#parallel_query_pri_field���е����ֵ
   DECLARE iPRIMin INT(10) DEFAULT 0;#parallel_query_pri_field���е���Сֵ

   IF sHost = '' THEN
      SET sHost := 'localhost';#�����µ�MySQL����ʱ�ĵ�ַ
   END IF;

   IF sUserName = '' THEN
      SET sUserName := 'root';#MySQL�û���
   END IF;

   IF sPassword = '' THEN
      SET sPassword := '';#��Ӧ����
   END IF;

   IF iPort = 0 THEN
      SET iPort := 3306;#MySQL�˿ں�
   END IF;

   IF sDataBaseName = '' THEN
      SET sDataBaseName := database();#��ǰ���ݿ�
   ELSE
      SET sDataBaseName := TRIM(sDataBaseName);
   END IF;
set @DataBaseName := sDataBaseName;

   IF sTempTableName = '' THEN
      SET sTempTableName := 'MQ_TEMP_';#��ǰ���ݿ�
   END IF;

   IF sEngineType = '' THEN
      SET sEngineType := 'InnoDB';#��ʱ��洢����
   END IF;

   IF sTrueTableName = '' THEN
      SET sTrueTableName := get_main_from(in_sql);#��ȡ����������������ԡ�,���ָ�
      SET iPos := LOCATE(',',sTrueTableName);
      SET sAsTableName := MID(sTrueTableName,iPos+1,CHAR_LENGTH(sTrueTableName)-iPos);#��ȡ�������
      SET sTrueTableName := MID(sTrueTableName,1,CHAR_LENGTH(sTrueTableName)-CHAR_LENGTH(sAsTableName)-1);#��ȡ������
   END IF;

   
   #��ȡ���������������
   SELECT COLUMN_NAME,MAX_VALUE,MIN_VALUE INTO sPRIField,iPRIMax,iPRIMin FROM mysql.parallel_query_pri_fields WHERE table_schema = sDataBaseName AND table_name = sTrueTableName limit 1;
   IF sPRIField = ''  then
      SELECT column_name INTO sPRIField FROM information_schema.columns WHERE table_schema = sDataBaseName AND table_name = sTrueTableName AND column_key = 'PRI' AND DATA_TYPE LIKE '%int%' limit 1;
      IF sPRIField = ''  then
         #����û�������������ֶξͱ����˳�
         SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT = 'PRI Field Error';
         LEAVE label_exit;#ֱ���˳������洢����
      END IF;
   ELSE
     IF iMin = 0 and iMax = 0 THEN
         SET iMin := iPRIMin;
         SET iMax := iPRIMax;
      END IF;
   END IF;
   
   #��ȡ�������������ֵ����Сֵ
   IF iMin = 0 and iMax = 0 THEN
      SET @MQ_STMT :=CONCAT("select min(",sPRIField,"),max(",sPRIField,") into @MQ_PRIField_Min,@MQ_PRIField_Max from ",sDataBaseName,".",sTrueTableName);
      PREPARE STMT FROM @MQ_STMT; 
      EXECUTE STMT;
      SELECT @MQ_PRIField_Min,@MQ_PRIField_Max INTO iMin,iMax;
   END IF;
   
   SELECT NULL,NULL,NULL INTO @MQ_STMT,@MQ_PRIField_Min,@MQ_PRIField_Max;#���ÿղ��õ��û�����

   SET @MQ_1sAsTableName := TRIM(sAsTableName);
   SET @MQ_1PRIField := TRIM(sPRIField);
   SET iNeedTempTable := is_select_sql(in_sql);#�����Ƿ���Select�������Ǿ���Ҫ������ʱ��

   IF iNeedTempTable THEN
      #������ʱ��
      SET sTempTableName := CONCAT(sTempTableName,my_nanoseconds());#ǰ׺ + ��ʱ������ ������������ʱ����
      #SET @MQ_STMT :=build_create_table_sql(in_sql,CONCAT(sDataBaseName,'.',sTempTableName),sEngineType,sEngineConnection);#��ȡ������ʱ���SQL���
      #PREPARE STMT FROM @MQ_STMT;
      #EXECUTE STMT;
      #SET @MQ_STMT := NULL;

      #ִ�ж��̲߳�ѯ
      SET @MQ_STMT :=run_multi_query(@DataBaseName,in_sql,@MQ_1sAsTableName,@MQ_1PRIField,iMin,iMax,thread_num,TRIM(sTempTableName),"innodb",TRIM(end_before),TRIM(end_sql),
                                     TRIM(sHost),TRIM(sUserName),TRIM(sPassword),iPort);
      IF (SUBSTR(@MQ_STMT,1,1)="~") THEN #������صĵ�һ���ַ��� ��~���ͱ�ʾ������ 
         #ɾ����ʱ��
         SET @MQ_STMT := CONCAT("DROP TABLE ",sDataBaseName,'.',sTempTableName);
         PREPARE STMT FROM @MQ_STMT;
         EXECUTE STMT;
         SET @MQ_STMT := NULL;

         SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT = @MQ_STMT;
         SET @MQ_STMT := NULL;
         LEAVE label_exit;#ֱ���˳������洢����
      ELSE
         #����ɹ���ִ�����Ļ��ܲ�ѯ���
         PREPARE STMT FROM @MQ_STMT;
         EXECUTE STMT;
         SET @MQ_STMT := NULL;
      END IF;
      
      #ɾ����ʱ��
      SET @MQ_STMT := CONCAT("DROP TABLE ",sDataBaseName,".",sTempTableName);
      PREPARE STMT FROM @MQ_STMT;
      EXECUTE STMT;
      SET @MQ_STMT := NULL;
   ELSE
      SET @MQ_STMT :=run_multi_query(@DataBaseName,in_sql,@MQ_1sAsTableName,@MQ_1PRIField,iMin,iMax,thread_num,TRIM(sTempTableName),"innodb",TRIM(end_before),TRIM(end_sql),
                                    TRIM(sHost),TRIM(sUserName),TRIM(sPassword),iPort);
      IF (SUBSTR(@MQ_STMT,1,1)="~") THEN #������صĵ�һ���ַ��� ��~���ͱ�ʾ������ 
         SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT = @MQ_STMT;
         SET @MQ_STMT := NULL;
         LEAVE label_exit;#ֱ���˳������洢����
      ELSE
         SELECT 'ok' as `return`;
      END IF;
   END IF;
END
;;
DELIMITER ;