# hivedriver
hive-jdbc修改，在使用本jdbc时会记录相应的sql语句到mysql中。便于后期的sql统计优化。

原始版本：
    <dependency>
			<groupId>org.apache.hive</groupId>
			<artifactId>hive-jdbc</artifactId>
			<version>0.13.1-cdh5.2.0</version>
		</dependency>

驱动名称：com.udps.hive.jdbc.HiveDriver

mysql中表的建表语句：
		CREATE TABLE `hiveinstance` (
		  `queryid` varchar(64) NOT NULL,
		  `type` varchar(64) DEFAULT NULL,
		  `otid` varchar(64) DEFAULT NULL,
		  `submittime` datetime DEFAULT NULL,
		  `detail` varchar(256) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,
		  `status` varchar(64) DEFAULT NULL,
		  `sqlline` varchar(256) DEFAULT NULL,
		  PRIMARY KEY (`queryid`)
		) ENGINE=MyISAM DEFAULT CHARSET=utf8;
