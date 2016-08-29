/*
Navicat MySQL Data Transfer

Source Server         : localhost
Source Server Version : 50540
Source Host           : localhost:3306
Source Database       : mysql

Target Server Type    : MYSQL
Target Server Version : 50540
File Encoding         : 65001

Date: 2016-08-04 14:04:27
*/

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
  PRIMARY KEY (`ID`),
  UNIQUE KEY `TABLE_SCHEMA` (`TABLE_SCHEMA`,`TABLE_NAME`)
) ENGINE=MyISAM AUTO_INCREMENT=2 DEFAULT CHARSET=gbk;
