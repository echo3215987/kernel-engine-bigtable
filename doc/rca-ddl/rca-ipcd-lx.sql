CREATE DATABASE  IF NOT EXISTS `rca-ipcd-lx` /*!40100 DEFAULT CHARACTER SET latin1 */;
USE `rca-ipcd-lx`;
-- MySQL dump 10.13  Distrib 5.7.24, for Linux (x86_64)
--
-- Host: 10.57.232.61    Database: rca-ipcd-lx
-- ------------------------------------------------------
-- Server version	5.7.17

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `RolesPermissions`
--

DROP TABLE IF EXISTS `RolesPermissions`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `RolesPermissions` (
  `id` bigint(20) NOT NULL,
  `permission` varchar(255) NOT NULL,
  `roleName` varchar(255) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `RolesPermissions`
--

LOCK TABLES `RolesPermissions` WRITE;
/*!40000 ALTER TABLE `RolesPermissions` DISABLE KEYS */;
/*!40000 ALTER TABLE `RolesPermissions` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `UserRoles`
--

DROP TABLE IF EXISTS `UserRoles`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `UserRoles` (
  `id` bigint(20) NOT NULL,
  `roleName` varchar(255) NOT NULL,
  `username` varchar(255) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `UK_2hcdotd20fse1tihr13r25mrn` (`roleName`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `UserRoles`
--

LOCK TABLES `UserRoles` WRITE;
/*!40000 ALTER TABLE `UserRoles` DISABLE KEYS */;
/*!40000 ALTER TABLE `UserRoles` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `Users`
--

DROP TABLE IF EXISTS `Users`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `Users` (
  `id` bigint(20) NOT NULL,
  `password` varchar(255) NOT NULL,
  `username` varchar(255) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `UK_23y4gd49ajvbqgl3psjsvhff6` (`username`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `Users`
--

LOCK TABLES `Users` WRITE;
/*!40000 ALTER TABLE `Users` DISABLE KEYS */;
/*!40000 ALTER TABLE `Users` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `fa_case`
--

DROP TABLE IF EXISTS `fa_case`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `fa_case` (
  `sn` varchar(100) NOT NULL,
  `test_starttime` datetime NOT NULL,
  `sympton` varchar(256) NOT NULL,
  `root_cause` varchar(256) DEFAULT NULL,
  `risktype` varchar(100) DEFAULT NULL,
  `riskname` varchar(100) DEFAULT NULL,
  `riskcode` varchar(100) DEFAULT NULL,
  `description` varchar(256) DEFAULT NULL,
  `filenames` json DEFAULT NULL,
  `createdby` bigint(20) unsigned NOT NULL,
  `createtime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updatedby` bigint(20) unsigned NOT NULL,
  `updatetime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`sn`,`test_starttime`,`sympton`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `fa_case`
--

LOCK TABLES `fa_case` WRITE;
/*!40000 ALTER TABLE `fa_case` DISABLE KEYS */;
INSERT INTO `fa_case` VALUES ('CN8BJ7C2G1','2018-01-06 09:00:00','test','test','test','test','test','test','[\"a.txt\", \"b.txt\", \"c.txt\"]',0,'2019-02-21 08:49:02',0,'2019-02-21 08:49:02'),('CN8BJ7C2G1','2018-01-06 09:00:00','test1','test','test','test','test','test','[\"a.txt\", \"b.txt\", \"c.txt\"]',0,'2019-02-21 08:55:34',0,'2019-02-21 08:55:34');
/*!40000 ALTER TABLE `fa_case` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `hibernate_sequence`
--

DROP TABLE IF EXISTS `hibernate_sequence`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `hibernate_sequence` (
  `next_val` bigint(20) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `hibernate_sequence`
--

LOCK TABLES `hibernate_sequence` WRITE;
/*!40000 ALTER TABLE `hibernate_sequence` DISABLE KEYS */;
INSERT INTO `hibernate_sequence` VALUES (1),(1),(1);
/*!40000 ALTER TABLE `hibernate_sequence` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `product_floor_line`
--

DROP TABLE IF EXISTS `product_floor_line`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `product_floor_line` (
  `product` varchar(100) NOT NULL,
  `floor` varchar(45) NOT NULL,
  `line` varchar(100) NOT NULL,
  `update_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`product`,`floor`,`line`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `product_floor_line`
--

LOCK TABLES `product_floor_line` WRITE;
/*!40000 ALTER TABLE `product_floor_line` DISABLE KEYS */;
INSERT INTO `product_floor_line` VALUES ('TaiJi Base','D626','D626','2019-02-20 03:51:58');
/*!40000 ALTER TABLE `product_floor_line` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `risk_assembly_station`
--

DROP TABLE IF EXISTS `risk_assembly_station`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `risk_assembly_station` (
  `test_starttime` datetime NOT NULL,
  `product` varchar(100) NOT NULL,
  `factory` varchar(45) NOT NULL,
  `floor` varchar(45) NOT NULL,
  `failure_sympton` varchar(256) NOT NULL,
  `defectqty` bigint(20) NOT NULL,
  `outputqty` bigint(20) NOT NULL,
  `waitrepaircnt` bigint(20) DEFAULT '0',
  `riskname` varchar(100) NOT NULL,
  `riskcode` varchar(100) NOT NULL,
  `failqty` bigint(20) NOT NULL,
  `throughputqty` bigint(20) NOT NULL,
  `sympton_failureduration_sec` bigint(20) NOT NULL,
  `risk_failureduration_sec` bigint(20) NOT NULL,
  `asm_duration_sec` bigint(20) NOT NULL,
  `teststation` varchar(100) NOT NULL,
  `line` varchar(100) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `risk_assembly_station`
--

LOCK TABLES `risk_assembly_station` WRITE;
/*!40000 ALTER TABLE `risk_assembly_station` DISABLE KEYS */;
INSERT INTO `risk_assembly_station` VALUES ('2018-11-19 13:50:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'CIS','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-19 13:50:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'Control Panel PCB','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-19 13:50:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'Main B','F1034891',2,1,0,0,1,'TLEOL','D626'),('2018-11-19 13:50:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'Main PCB','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-19 16:50:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'CIS','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-19 16:50:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'Control Panel PCB','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-19 16:50:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'Main B','F1034891',2,1,0,0,1,'TLEOL','D626'),('2018-11-19 16:50:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'Main PCB','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-20 09:00:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'CIS','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-20 09:00:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'Control Panel PCB','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-20 09:00:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'Main B','F1034891',2,1,0,0,1,'TLEOL','D626'),('2018-11-20 09:00:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'Main PCB','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-20 10:10:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'CIS','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-20 10:10:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'Control Panel PCB','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-20 10:10:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'Main B','F1034891',2,1,0,0,1,'TLEOL','D626'),('2018-11-20 10:10:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'Main PCB','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-20 17:20:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'CIS','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-20 17:20:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'Control Panel PCB','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-20 17:20:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'Main B','F1034891',2,1,0,0,1,'TLEOL','D626'),('2018-11-20 17:20:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'Main PCB','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-20 18:20:00','TaiJi Base','WH','D626','Scan3StreakRGB',1,1,0,'CIS','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-20 18:20:00','TaiJi Base','WH','D626','Scan3StreakRGB',1,1,0,'Control Panel PCB','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-20 18:20:00','TaiJi Base','WH','D626','Scan3StreakRGB',1,1,0,'Main B','F1034891',2,1,0,0,1,'TLEOL','D626'),('2018-11-20 18:20:00','TaiJi Base','WH','D626','Scan3StreakRGB',1,1,0,'Main PCB','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-21 09:40:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'CIS','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-21 09:40:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'Control Panel PCB','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-21 09:40:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'Main B','F1034891',2,1,0,0,1,'TLEOL','D626'),('2018-11-21 09:40:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'Main PCB','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-21 11:00:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,2,0,'CIS','F1034891',1,2,0,0,1,'TLEOL','D626'),('2018-11-21 11:00:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,2,0,'Control Panel PCB','F1034891',1,2,0,0,1,'TLEOL','D626'),('2018-11-21 11:00:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,2,0,'Main B','F1034891',2,2,0,0,1,'TLEOL','D626'),('2018-11-21 11:00:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,2,0,'Main PCB','F1034891',1,2,0,0,1,'TLEOL','D626'),('2018-11-21 11:10:00','TaiJi Base','WH','D626','Scan3StreakFlare',2,2,0,'CIS','F1034891',2,2,0,0,1,'TLEOL','D626'),('2018-11-21 11:10:00','TaiJi Base','WH','D626','Scan3StreakFlare',2,2,0,'Control Panel PCB','F1034891',2,2,0,0,1,'TLEOL','D626'),('2018-11-21 11:10:00','TaiJi Base','WH','D626','Scan3StreakFlare',2,2,0,'Main B','F1034891',4,2,0,0,1,'TLEOL','D626'),('2018-11-21 11:10:00','TaiJi Base','WH','D626','Scan3StreakFlare',2,2,0,'Main PCB','F1034891',2,2,0,0,1,'TLEOL','D626'),('2018-11-21 11:20:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'CIS','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-21 11:20:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'Control Panel PCB','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-21 11:20:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'Main B','F1034891',2,1,0,0,1,'TLEOL','D626'),('2018-11-21 11:20:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'Main PCB','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-21 12:30:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'CIS','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-21 12:30:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'Control Panel PCB','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-21 12:30:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'Main B','F1034891',2,1,0,0,1,'TLEOL','D626'),('2018-11-21 12:30:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'Main PCB','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-21 17:20:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'CIS','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-21 17:20:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'Control Panel PCB','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-21 17:20:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'Main B','F1034891',2,1,0,0,1,'TLEOL','D626'),('2018-11-21 17:20:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'Main PCB','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-22 12:50:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'CIS','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-22 12:50:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'Control Panel PCB','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-22 12:50:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'Main B','F1034891',2,1,0,0,1,'TLEOL','D626'),('2018-11-22 12:50:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'Main PCB','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-22 18:00:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'CIS','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-22 18:00:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'Control Panel PCB','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-22 18:00:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'Main B','F1034891',2,1,0,0,1,'TLEOL','D626'),('2018-11-22 18:00:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'Main PCB','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-23 12:40:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'CIS','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-23 12:40:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'Control Panel PCB','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-23 12:40:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'Main B','F1034891',2,1,0,0,1,'TLEOL','D626'),('2018-11-23 12:40:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'Main PCB','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-23 14:10:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'Control Panel PCB','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-23 14:10:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'Main B','F1034891',2,1,0,0,1,'TLEOL','D626'),('2018-11-23 14:10:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'Main PCB','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-23 15:10:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'Control Panel PCB','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-23 15:10:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'Main B','F1034891',2,1,0,0,1,'TLEOL','D626'),('2018-11-23 15:10:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'Main PCB','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-23 16:30:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'Control Panel PCB','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-23 16:30:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'Main B','F1034891',2,1,0,0,1,'TLEOL','D626'),('2018-11-23 16:30:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'Main PCB','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-23 17:40:00','TaiJi Base','WH','D626','Scan3GetTargetInfo.2; Scan3StreakFlare',1,1,0,'Control Panel PCB','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-23 17:40:00','TaiJi Base','WH','D626','Scan3GetTargetInfo.2; Scan3StreakFlare',1,1,0,'Main B','F1034891',2,1,0,0,1,'TLEOL','D626'),('2018-11-23 17:40:00','TaiJi Base','WH','D626','Scan3GetTargetInfo.2; Scan3StreakFlare',1,1,0,'Main PCB','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-23 17:50:00','TaiJi Base','WH','D626','Scan3GetTargetInfo.2; Scan3StreakFlare',1,1,0,'Control Panel PCB','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-23 17:50:00','TaiJi Base','WH','D626','Scan3GetTargetInfo.2; Scan3StreakFlare',1,1,0,'Main B','F1034891',2,1,0,0,1,'TLEOL','D626'),('2018-11-23 17:50:00','TaiJi Base','WH','D626','Scan3GetTargetInfo.2; Scan3StreakFlare',1,1,0,'Main PCB','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-24 14:40:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'CIS','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-24 14:40:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'Control Panel PCB','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-24 14:40:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'Main B','F1034891',2,1,0,0,1,'TLEOL','D626'),('2018-11-24 14:40:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'Main PCB','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-24 16:10:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'CIS','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-24 16:10:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'Control Panel PCB','F1034891',1,1,0,0,1,'TLEOL','D626'),('2018-11-24 16:10:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'Main B','F1034891',2,1,0,0,1,'TLEOL','D626'),('2018-11-24 16:10:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'Main PCB','F1034891',1,1,0,0,1,'TLEOL','D626');
/*!40000 ALTER TABLE `risk_assembly_station` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `risk_part`
--

DROP TABLE IF EXISTS `risk_part`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `risk_part` (
  `test_starttime` datetime NOT NULL,
  `product` varchar(100) NOT NULL,
  `factory` varchar(45) NOT NULL,
  `floor` varchar(45) NOT NULL,
  `failure_sympton` varchar(256) NOT NULL,
  `defectqty` bigint(20) NOT NULL,
  `outputqty` bigint(20) NOT NULL,
  `waitrepaircnt` bigint(20) DEFAULT '0',
  `riskname` varchar(100) NOT NULL,
  `riskcode` varchar(100) NOT NULL,
  `failqty` bigint(20) NOT NULL,
  `throughputqty` bigint(20) NOT NULL,
  `sympton_failureduration_sec` bigint(20) NOT NULL,
  `risk_failureduration_sec` bigint(20) NOT NULL,
  `asm_duration_sec` bigint(20) NOT NULL,
  `teststation` varchar(100) NOT NULL,
  `line` varchar(100) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `risk_part`
--

LOCK TABLES `risk_part` WRITE;
/*!40000 ALTER TABLE `risk_part` DISABLE KEYS */;
INSERT INTO `risk_part` VALUES ('2018-11-19 13:50:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'C44','C44_845',1,1,0,0,1,'TLEOL','D626'),('2018-11-19 13:50:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'LK','8P_1892',1,1,0,0,1,'TLEOL','D626'),('2018-11-19 13:50:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'CAL','CAL_845',1,1,0,0,1,'TLEOL','D626'),('2018-11-19 13:50:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'4BH6','4BH6_45011',1,1,0,0,1,'TLEOL','D626'),('2018-11-19 13:50:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'CD','CD_1845',1,1,0,0,1,'TLEOL','D626'),('2018-11-19 16:50:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'C44','C44_845',1,1,0,0,1,'TLEOL','D626'),('2018-11-19 16:50:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'4BH6','4BH6_45410',1,1,0,0,1,'TLEOL','D626'),('2018-11-19 16:50:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'CAL','CAL_845',1,1,0,0,1,'TLEOL','D626'),('2018-11-19 16:50:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'LK','GW_1892',1,1,0,0,1,'TLEOL','D626'),('2018-11-19 16:50:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'CD','CD_1845',1,1,0,0,1,'TLEOL','D626'),('2018-11-20 09:00:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'C44','C44_845',1,1,0,0,1,'TLEOL','D626'),('2018-11-20 09:00:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'4BH6','4BH6_11690',1,1,0,0,1,'TLEOL','D626'),('2018-11-20 09:00:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'CAL','CAL_845',1,1,0,0,1,'TLEOL','D626'),('2018-11-20 09:00:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'LK','U2_1891',1,1,0,0,1,'TLEOL','D626'),('2018-11-20 09:00:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'CD','CD_1845',1,1,0,0,1,'TLEOL','D626'),('2018-11-20 10:10:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'C44','C44_845',1,1,0,0,1,'TLEOL','D626'),('2018-11-20 10:10:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'LK','22_1892',1,1,0,0,1,'TLEOL','D626'),('2018-11-20 10:10:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'CAL','CAL_845',1,1,0,0,1,'TLEOL','D626'),('2018-11-20 10:10:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'4BH6','4BH6_30150',1,1,0,0,1,'TLEOL','D626'),('2018-11-20 10:10:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'CD','CD_1845',1,1,0,0,1,'TLEOL','D626'),('2018-11-20 17:20:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'C44','C44_845',1,1,0,0,1,'TLEOL','D626'),('2018-11-20 17:20:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'CAL','CAL_846',1,1,0,0,1,'TLEOL','D626'),('2018-11-20 17:20:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'LK','RM_18X0',1,1,0,0,1,'TLEOL','D626'),('2018-11-20 17:20:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'4BH6','4BH6_51780',1,1,0,0,1,'TLEOL','D626'),('2018-11-20 17:20:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'CD','CD_1845',1,1,0,0,1,'TLEOL','D626'),('2018-11-20 18:20:00','TaiJi Base','WH','D626','Scan3StreakRGB',1,1,0,'C44','C44_845',1,1,0,0,1,'TLEOL','D626'),('2018-11-20 18:20:00','TaiJi Base','WH','D626','Scan3StreakRGB',1,1,0,'CAL','CAL_845',1,1,0,0,1,'TLEOL','D626'),('2018-11-20 18:20:00','TaiJi Base','WH','D626','Scan3StreakRGB',1,1,0,'LK','87_18X0',1,1,0,0,1,'TLEOL','D626'),('2018-11-20 18:20:00','TaiJi Base','WH','D626','Scan3StreakRGB',1,1,0,'CD','CD_1845',1,1,0,0,1,'TLEOL','D626'),('2018-11-20 18:20:00','TaiJi Base','WH','D626','Scan3StreakRGB',1,1,0,'4BH6','4BH6_42350',1,1,0,0,1,'TLEOL','D626'),('2018-11-21 09:40:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'C44','C44_845',1,1,0,0,1,'TLEOL','D626'),('2018-11-21 09:40:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'4BH6','4BH6_71931',1,1,0,0,1,'TLEOL','D626'),('2018-11-21 09:40:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'CAL','CAL_846',1,1,0,0,1,'TLEOL','D626'),('2018-11-21 09:40:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'LK','RU_1892',1,1,0,0,1,'TLEOL','D626'),('2018-11-21 09:40:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'CD','CD_1845',1,1,0,0,1,'TLEOL','D626'),('2018-11-21 11:00:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,2,0,'C44','C44_845',1,2,0,0,1,'TLEOL','D626'),('2018-11-21 11:00:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,2,0,'LK','G7_1892',0,1,0,0,1,'TLEOL','D626'),('2018-11-21 11:00:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,2,0,'CAL','CAL_846',1,2,0,0,1,'TLEOL','D626'),('2018-11-21 11:00:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,2,0,'4BH6','4BH6_52000',0,1,0,0,1,'TLEOL','D626'),('2018-11-21 11:00:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,2,0,'LK','4B_1892',1,1,0,0,1,'TLEOL','D626'),('2018-11-21 11:00:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,2,0,'CD','CD_1845',1,2,0,0,1,'TLEOL','D626'),('2018-11-21 11:00:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,2,0,'4BH6','4BH6_52510',1,1,0,0,1,'TLEOL','D626'),('2018-11-21 11:10:00','TaiJi Base','WH','D626','Scan3StreakFlare',2,2,0,'C44','C44_845',2,2,0,0,1,'TLEOL','D626'),('2018-11-21 11:10:00','TaiJi Base','WH','D626','Scan3StreakFlare',2,2,0,'LK','G7_1892',1,1,0,0,1,'TLEOL','D626'),('2018-11-21 11:10:00','TaiJi Base','WH','D626','Scan3StreakFlare',2,2,0,'CAL','CAL_846',2,2,0,0,1,'TLEOL','D626'),('2018-11-21 11:10:00','TaiJi Base','WH','D626','Scan3StreakFlare',2,2,0,'4BH6','4BH6_52000',1,1,0,0,1,'TLEOL','D626'),('2018-11-21 11:10:00','TaiJi Base','WH','D626','Scan3StreakFlare',2,2,0,'LK','4B_1892',1,1,0,0,1,'TLEOL','D626'),('2018-11-21 11:10:00','TaiJi Base','WH','D626','Scan3StreakFlare',2,2,0,'CD','CD_1845',2,2,0,0,1,'TLEOL','D626'),('2018-11-21 11:10:00','TaiJi Base','WH','D626','Scan3StreakFlare',2,2,0,'4BH6','4BH6_52510',1,1,0,0,1,'TLEOL','D626'),('2018-11-21 11:20:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'C44','C44_845',1,1,0,0,1,'TLEOL','D626'),('2018-11-21 11:20:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'CAL','CAL_846',1,1,0,0,1,'TLEOL','D626'),('2018-11-21 11:20:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'4BH6','4BH6_72620',1,1,0,0,1,'TLEOL','D626'),('2018-11-21 11:20:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'LK','RX_1892',1,1,0,0,1,'TLEOL','D626'),('2018-11-21 11:20:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'CD','CD_1845',1,1,0,0,1,'TLEOL','D626'),('2018-11-21 12:30:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'C44','C44_845',1,1,0,0,1,'TLEOL','D626'),('2018-11-21 12:30:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'CAL','CAL_846',1,1,0,0,1,'TLEOL','D626'),('2018-11-21 12:30:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'4BH6','4BH6_72620',1,1,0,0,1,'TLEOL','D626'),('2018-11-21 12:30:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'LK','RX_1892',1,1,0,0,1,'TLEOL','D626'),('2018-11-21 12:30:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'CD','CD_1845',1,1,0,0,1,'TLEOL','D626'),('2018-11-21 17:20:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'C44','C44_845',1,1,0,0,1,'TLEOL','D626'),('2018-11-21 17:20:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'CAL','CAL_846',1,1,0,0,1,'TLEOL','D626'),('2018-11-21 17:20:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'CD','CD_1846',1,1,0,0,1,'TLEOL','D626'),('2018-11-21 17:20:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'LK','6V_1892',1,1,0,0,1,'TLEOL','D626'),('2018-11-21 17:20:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'4BH6','4BH6_62280',1,1,0,0,1,'TLEOL','D626'),('2018-11-22 12:50:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'C44','C44_845',1,1,0,0,1,'TLEOL','D626'),('2018-11-22 12:50:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'CAL','CAL_846',1,1,0,0,1,'TLEOL','D626'),('2018-11-22 12:50:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'LK','GL_X939',1,1,0,0,1,'TLEOL','D626'),('2018-11-22 12:50:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'4BH6','4BH6_02300',1,1,0,0,1,'TLEOL','D626'),('2018-11-22 12:50:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'CD','CD_1845',1,1,0,0,1,'TLEOL','D626'),('2018-11-22 18:00:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'C44','C44_845',1,1,0,0,1,'TLEOL','D626'),('2018-11-22 18:00:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'LK','KM_1892',1,1,0,0,1,'TLEOL','D626'),('2018-11-22 18:00:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'CAL','CAL_846',1,1,0,0,1,'TLEOL','D626'),('2018-11-22 18:00:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'4BH6','4BH6_72410',1,1,0,0,1,'TLEOL','D626'),('2018-11-22 18:00:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'CD','CD_1846',1,1,0,0,1,'TLEOL','D626'),('2018-11-23 12:40:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'CAL','CAL_846',1,1,0,0,1,'TLEOL','D626'),('2018-11-23 12:40:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'4BH6','4BH6_30691',1,1,0,0,1,'TLEOL','D626'),('2018-11-23 12:40:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'CD','CD_1846',1,1,0,0,1,'TLEOL','D626'),('2018-11-23 12:40:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'LK','75_1892',1,1,0,0,1,'TLEOL','D626'),('2018-11-23 12:40:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'C44','C44_846',1,1,0,0,1,'TLEOL','D626'),('2018-11-23 14:10:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'CD','CD_1846',1,1,0,0,1,'TLEOL','D626'),('2018-11-23 14:10:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'LK','DX_1892',1,1,0,0,1,'TLEOL','D626'),('2018-11-23 14:10:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'CAL','SB8_809',1,1,0,0,1,'TLEOL','D626'),('2018-11-23 14:10:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'C44','C44_846',1,1,0,0,1,'TLEOL','D626'),('2018-11-23 15:10:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'CAL','SB8_818',1,1,0,0,1,'TLEOL','D626'),('2018-11-23 15:10:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'LK','1M_1892',1,1,0,0,1,'TLEOL','D626'),('2018-11-23 15:10:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'CD','CD_1846',1,1,0,0,1,'TLEOL','D626'),('2018-11-23 15:10:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'C44','C44_846',1,1,0,0,1,'TLEOL','D626'),('2018-11-23 16:30:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'CAL','SB8_818',1,1,0,0,1,'TLEOL','D626'),('2018-11-23 16:30:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'CD','CD_1846',1,1,0,0,1,'TLEOL','D626'),('2018-11-23 16:30:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'LK','BP_1892',1,1,0,0,1,'TLEOL','D626'),('2018-11-23 16:30:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'C44','C44_846',1,1,0,0,1,'TLEOL','D626'),('2018-11-23 17:40:00','TaiJi Base','WH','D626','Scan3GetTargetInfo.2; Scan3StreakFlare',1,1,0,'CAL','SB8_818',1,1,0,0,1,'TLEOL','D626'),('2018-11-23 17:40:00','TaiJi Base','WH','D626','Scan3GetTargetInfo.2; Scan3StreakFlare',1,1,0,'CD','CD_1846',1,1,0,0,1,'TLEOL','D626'),('2018-11-23 17:40:00','TaiJi Base','WH','D626','Scan3GetTargetInfo.2; Scan3StreakFlare',1,1,0,'LK','NG_1892',1,1,0,0,1,'TLEOL','D626'),('2018-11-23 17:40:00','TaiJi Base','WH','D626','Scan3GetTargetInfo.2; Scan3StreakFlare',1,1,0,'C44','C44_846',1,1,0,0,1,'TLEOL','D626'),('2018-11-23 17:50:00','TaiJi Base','WH','D626','Scan3GetTargetInfo.2; Scan3StreakFlare',1,1,0,'CAL','SB8_818',1,1,0,0,1,'TLEOL','D626'),('2018-11-23 17:50:00','TaiJi Base','WH','D626','Scan3GetTargetInfo.2; Scan3StreakFlare',1,1,0,'CD','CD_1846',1,1,0,0,1,'TLEOL','D626'),('2018-11-23 17:50:00','TaiJi Base','WH','D626','Scan3GetTargetInfo.2; Scan3StreakFlare',1,1,0,'LK','NG_1892',1,1,0,0,1,'TLEOL','D626'),('2018-11-23 17:50:00','TaiJi Base','WH','D626','Scan3GetTargetInfo.2; Scan3StreakFlare',1,1,0,'C44','C44_846',1,1,0,0,1,'TLEOL','D626'),('2018-11-24 14:40:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'CAL','CAL_846',1,1,0,0,1,'TLEOL','D626'),('2018-11-24 14:40:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'4BH6','4BH6_81550',1,1,0,0,1,'TLEOL','D626'),('2018-11-24 14:40:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'CD','CD_1846',1,1,0,0,1,'TLEOL','D626'),('2018-11-24 14:40:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'LK','K5_1892',1,1,0,0,1,'TLEOL','D626'),('2018-11-24 14:40:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'C44','C44_846',1,1,0,0,1,'TLEOL','D626'),('2018-11-24 16:10:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'CAL','CAL_846',1,1,0,0,1,'TLEOL','D626'),('2018-11-24 16:10:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'CD','CD_1846',1,1,0,0,1,'TLEOL','D626'),('2018-11-24 16:10:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'LK','MA_1892',1,1,0,0,1,'TLEOL','D626'),('2018-11-24 16:10:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'C44','C44_846',1,1,0,0,1,'TLEOL','D626'),('2018-11-24 16:10:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'4BH6','4BH6_61710',1,1,0,0,1,'TLEOL','D626');
/*!40000 ALTER TABLE `risk_part` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `risk_test_station`
--

DROP TABLE IF EXISTS `risk_test_station`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `risk_test_station` (
  `test_starttime` datetime NOT NULL,
  `product` varchar(100) NOT NULL,
  `factory` varchar(45) NOT NULL,
  `floor` varchar(45) NOT NULL,
  `failure_sympton` varchar(256) NOT NULL,
  `defectqty` bigint(20) NOT NULL,
  `outputqty` bigint(20) NOT NULL,
  `waitrepaircnt` bigint(20) DEFAULT '0',
  `riskname` varchar(100) NOT NULL,
  `riskcode` varchar(100) NOT NULL,
  `failqty` bigint(20) NOT NULL,
  `throughputqty` bigint(20) NOT NULL,
  `sympton_failureduration_sec` bigint(20) NOT NULL,
  `risk_failureduration_sec` bigint(20) NOT NULL,
  `asm_duration_sec` bigint(20) NOT NULL,
  `teststation` varchar(100) NOT NULL,
  `line` varchar(100) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `risk_test_station`
--

LOCK TABLES `risk_test_station` WRITE;
/*!40000 ALTER TABLE `risk_test_station` DISABLE KEYS */;
INSERT INTO `risk_test_station` VALUES ('2018-11-19 13:50:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'TLEOL','LC_TLEOL_45',5,1,0,0,1,'TLEOL','D626'),('2018-11-19 16:50:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'TLEOL','LC_TLEOL_48',5,1,0,0,1,'TLEOL','D626'),('2018-11-20 09:00:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'TLEOL','LC_TLEOL_22',5,1,0,0,1,'TLEOL','D626'),('2018-11-20 10:10:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'TLEOL','LC_TLEOL_41',5,1,0,0,1,'TLEOL','D626'),('2018-11-20 17:20:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'TLEOL','LC_TLEOL_10',5,1,0,0,1,'TLEOL','D626'),('2018-11-20 18:20:00','TaiJi Base','WH','D626','Scan3StreakRGB',1,1,0,'TLEOL','LC_TLEOL_35',5,1,0,0,1,'TLEOL','D626'),('2018-11-21 09:40:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'TLEOL','LC_TLEOL_47',5,1,0,0,1,'TLEOL','D626'),('2018-11-21 11:00:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,2,0,'TLEOL','LC_TLEOL_18',0,1,0,0,1,'TLEOL','D626'),('2018-11-21 11:00:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,2,0,'TLEOL','LC_TLEOL_45',5,1,0,0,1,'TLEOL','D626'),('2018-11-21 11:00:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,2,0,'TLEOL','LC_TLEOL_36',0,1,0,0,1,'TLEOL','D626'),('2018-11-21 11:10:00','TaiJi Base','WH','D626','Scan3StreakFlare',2,2,0,'TLEOL','LC_TLEOL_18',5,1,0,0,1,'TLEOL','D626'),('2018-11-21 11:10:00','TaiJi Base','WH','D626','Scan3StreakFlare',2,2,0,'TLEOL','LC_TLEOL_45',5,1,0,0,1,'TLEOL','D626'),('2018-11-21 11:10:00','TaiJi Base','WH','D626','Scan3StreakFlare',2,2,0,'TLEOL','LC_TLEOL_36',0,1,0,0,1,'TLEOL','D626'),('2018-11-21 11:20:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'TLEOL','LC_TLEOL_45',5,1,0,0,1,'TLEOL','D626'),('2018-11-21 11:20:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'TLEOL','LC_TLEOL_29',0,1,0,0,1,'TLEOL','D626'),('2018-11-21 12:30:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'TLEOL','LC_TLEOL_45',5,1,0,0,1,'TLEOL','D626'),('2018-11-21 12:30:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'TLEOL','LC_TLEOL_29',0,1,0,0,1,'TLEOL','D626'),('2018-11-21 17:20:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'TLEOL','LC_TLEOL_25',5,1,0,0,1,'TLEOL','D626'),('2018-11-22 12:50:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'TLEOL','LC_TLEOL_16',5,1,0,0,1,'TLEOL','D626'),('2018-11-22 18:00:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'TLEOL','LC_TLEOL_21',5,1,0,0,1,'TLEOL','D626'),('2018-11-23 12:40:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'TLEOL','LC_TLEOL_29',5,1,0,0,1,'TLEOL','D626'),('2018-11-23 14:10:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'TLEOL','LC_TLEOL_04',4,1,0,0,1,'TLEOL','D626'),('2018-11-23 15:10:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'TLEOL','LC_TLEOL_28',4,1,0,0,1,'TLEOL','D626'),('2018-11-23 16:30:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'TLEOL','LC_TLEOL_36',4,1,0,0,1,'TLEOL','D626'),('2018-11-23 17:40:00','TaiJi Base','WH','D626','Scan3GetTargetInfo.2; Scan3StreakFlare',1,1,0,'TLEOL','LC_TLEOL_28',4,1,0,0,1,'TLEOL','D626'),('2018-11-23 17:50:00','TaiJi Base','WH','D626','Scan3GetTargetInfo.2; Scan3StreakFlare',1,1,0,'TLEOL','LC_TLEOL_28',4,1,0,0,1,'TLEOL','D626'),('2018-11-24 14:40:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'TLEOL','LC_TLEOL_01',5,1,0,0,1,'TLEOL','D626'),('2018-11-24 16:10:00','TaiJi Base','WH','D626','Scan3StreakFlare',1,1,0,'TLEOL','LC_TLEOL_24',5,1,0,0,1,'TLEOL','D626');
/*!40000 ALTER TABLE `risk_test_station` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `roles_permissions`
--

DROP TABLE IF EXISTS `roles_permissions`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `roles_permissions` (
  `id` bigint(20) NOT NULL,
  `permission` varchar(255) NOT NULL,
  `role_name` varchar(255) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `roles_permissions`
--

LOCK TABLES `roles_permissions` WRITE;
/*!40000 ALTER TABLE `roles_permissions` DISABLE KEYS */;
/*!40000 ALTER TABLE `roles_permissions` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `shift`
--

DROP TABLE IF EXISTS `shift`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `shift` (
  `product` varchar(100) NOT NULL,
  `start_time` time NOT NULL,
  `stop_time` time NOT NULL,
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `description` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`product`,`start_time`,`stop_time`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `shift`
--

LOCK TABLES `shift` WRITE;
/*!40000 ALTER TABLE `shift` DISABLE KEYS */;
INSERT INTO `shift` VALUES ('Maserati','07:00:00','12:00:00','2019-02-19 07:51:16','morning'),('Maserati','13:00:00','18:00:00','2019-02-19 07:53:23','middle'),('Maserati','19:00:00','23:00:00','2019-02-19 07:53:46','night');
/*!40000 ALTER TABLE `shift` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `user_roles`
--

DROP TABLE IF EXISTS `user_roles`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `user_roles` (
  `id` bigint(20) NOT NULL,
  `role_name` varchar(255) NOT NULL,
  `username` varchar(255) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `UK_40fvvy071dnqy9tywk6ei7f5r` (`role_name`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `user_roles`
--

LOCK TABLES `user_roles` WRITE;
/*!40000 ALTER TABLE `user_roles` DISABLE KEYS */;
/*!40000 ALTER TABLE `user_roles` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `users`
--

DROP TABLE IF EXISTS `users`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `users` (
  `id` bigint(20) NOT NULL,
  `password` varchar(255) NOT NULL,
  `username` varchar(255) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `UK_r43af9ap4edm43mmtq01oddj6` (`username`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `users`
--

LOCK TABLES `users` WRITE;
/*!40000 ALTER TABLE `users` DISABLE KEYS */;
/*!40000 ALTER TABLE `users` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Dumping routines for database 'rca-ipcd-lx'
--
/*!50003 DROP FUNCTION IF EXISTS `pzero` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` FUNCTION `pzero`(throughtputqty bigint,
	outputqty bigint) RETURNS decimal(10,9)
BEGIN
	RETURN round(throughtputqty / outputqty, 10);
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP FUNCTION IF EXISTS `riskpoint` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` FUNCTION `riskpoint`(com decimal(10,5),max_com decimal(10,5),sig decimal(10,5),dur decimal(10,5),defectqty bigint) RETURNS int(11)
BEGIN
  DECLARE com_point int DEFAULT 0; 
  DECLARE lead_point int DEFAULT 0; 
  DECLARE sig_point int DEFAULT 0; 
  DECLARE dur_point int DEFAULT 0; 
  if com > 0.5 then set com_point = 1;end if;
  if com > (max_com-(1/defectqty)) then set lead_point = 1;end if;
  if sig > 2 then set sig_point = 1;end if;
  if dur > 0.5 then set dur_point = 1;end if;
RETURN com_point+lead_point+sig_point+dur_point;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP FUNCTION IF EXISTS `riskrank` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` FUNCTION `riskrank`(rkpt int(2),sig decimal(15,5),com decimal(15,5)) RETURNS int(20)
BEGIN
RETURN rkpt*100000+sig*1000+round(com*100,0);
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP FUNCTION IF EXISTS `significant` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` FUNCTION `significant`(failQty bigint,
	sympton_defectqty bigint,
	throughputQty bigint,
	outputQty bigint) RETURNS double
BEGIN
	RETURN IF(throughputQty = outputQty,
	NULL,
	(round(failQty / sympton_defectqty, 10)-round(throughputQty / outputQty, 10))/ sqrt(round(throughputQty / outputQty, 10)*(1-round(throughputQty / outputQty, 10))/ sympton_defectqty));
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `fill_product_floor_line` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'STRICT_TRANS_TABLES,STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `fill_product_floor_line`()
BEGIN
	drop
		TEMPORARY table
			if EXISTS fill_pfl_tmp;

create
	temporary table
		fill_pfl_tmp select
			product,
			floor,
			line
		FROM
			`rca-ipcd-lx`.risk_assembly_station;

insert
	into
		fill_pfl_tmp select
			product,
			floor,
			line
		FROM
			risk_test_station;

insert
	into
		fill_pfl_tmp select
			product,
			floor,
			line
		FROM
			risk_part;

insert
	into
		product_floor_line select
			product,
			floor,
			line,
			now()
		FROM
			fill_pfl_tmp
		group by
			product,
			floor,
			line ;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `level_one` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `level_one`(in pdt varchar(100),in flr varchar(45),in starttime datetime,in stoptime datetime,in top int)
BEGIN
  drop temporary table if exists level_one_tmp ;
  if !isnull(flr) then
  
  create temporary table level_one_tmp
  select *
  FROM `rca-ipcd-lx`.risk_assembly_station where product=pdt and floor=flr and test_starttime >= starttime and test_starttime < stoptime;
  
  insert into level_one_tmp
  select * from risk_test_station where product=pdt and floor=flr and test_starttime >= starttime and test_starttime < stoptime;
  
  insert into level_one_tmp
  select * from risk_part where product=pdt and floor=flr and test_starttime >= starttime and test_starttime < stoptime;
  
  
  else
  
  create temporary table level_one_tmp
  select *
  FROM `rca-ipcd-lx`.risk_assembly_station where product=pdt and test_starttime >= starttime and test_starttime < stoptime;
  
  insert into level_one_tmp
  select * from risk_test_station where product=pdt and test_starttime >= starttime and test_starttime < stoptime;
  
  insert into level_one_tmp
  select * from risk_part where product=pdt and test_starttime >= starttime and test_starttime < stoptime; 
  
  end if;
  
drop temporary table if exists level_one_tmp_all;
create temporary table level_one_tmp_all
SELECT teststation,symptonid,failure_sympton,
round(sum(failQty)/sum(outputQty),5) as failurerate,
sum(defectQty) as defectqty,
sum(outputQty) as outputqty,
round(sum(sympton_failureduration_sec)/3600,2) as failure_conthour,
0 as waitrepaircnt
FROM
    level_one_tmp group by symptonid;

drop temporary table if exists level_two_all;
create temporary table level_two_all
select *
FROM level_two_assembly_with_riskrank;
  
insert into level_two_all
select * FROM level_two_test_with_riskrank;
  
insert into level_two_all
select * from level_two_part_with_riskrank;

drop temporary table if exists level_two_all_toprank;
create temporary table level_two_all_toprank
select symptonid,riskname,max(riskrank) as riskrank from level_two_all group by symptonid;

#select * from level_two_all;
SET @rank:=0;
SELECT 
    @rank:=@rank + 1 AS rank,
    product,
    factory,
    floor,
    teststation,
    symptonid,
    level_one.failure_sympton,
    failurerate,
    level_one.defectqty,
    level_one.outputqty,
    failure_conthour,
    waitrepaircnt,
    riskname,
    riskcode,
    commonnality,
    significant,
    throughput_ratio,
    failureContRation,
    riskpoint
FROM
    level_one_tmp_all level_one
        LEFT JOIN
    (SELECT 
        *
    FROM
        level_two_all two_all
    INNER JOIN level_two_all_toprank top USING (symptonid , riskname , riskrank)) level_two_rank USING (symptonid)
ORDER BY failurerate DESC
LIMIT TOP;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `level_three` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `level_three`(in pdt varchar(100),in flr varchar(45),in starttime datetime,in stoptime datetime)
BEGIN
  drop temporary table if exists level_one_tmp ;
  if !isnull(flr) then
  create temporary table level_one_tmp
  select riskCode,riskname,failQty,defectQty,
  round(sum(failQty)/sum(defectQty),10) as commonnality,outputQty,throughputQty,
  round(sum(throughputQty)/sum(outputQty),10) as throughput_ratio,
  (round(sum(failQty)/sum(defectQty),10)-round(sum(throughputQty)/sum(outputQty),10))/sqrt(round(sum(throughputQty)/sum(outputQty),10)*(1-round(sum(throughputQty)/sum(outputQty),10))/defectqty) as significant,
  sum(failQty),
  sum(throughputQty),
  round((sum(risk_failureduration_sec)/ sum(asm_duration_sec))*100,2) as failureContRation 
  FROM `rca-ipcd-lx`.risk_assembly_station where product=pdt and floor=flr and test_starttime >= starttime and test_starttime < stoptime group by riskcode;
  else
   create temporary table level_one_tmp
  select riskCode,riskname,failQty,defectQty,
  round(sum(failQty)/sum(defectQty),10) as commonnality,outputQty,throughputQty,
  round(sum(throughputQty)/sum(outputQty),10) as throughput_ratio,
  (round(sum(failQty)/sum(defectQty),10)-round(sum(throughputQty)/sum(outputQty),10))/sqrt(round(sum(throughputQty)/sum(outputQty),10)*(1-round(sum(throughputQty)/sum(outputQty),10))/defectqty) as significant,
  sum(failQty),
  sum(throughputQty),
  round((sum(risk_failureduration_sec)/ sum(asm_duration_sec))*100,2) as failureContRation 
  FROM `rca-ipcd-lx`.risk_assembly_station where product=pdt and test_starttime >= starttime and test_starttime < stoptime group by riskcode;
  end if;
SELECT 
    MAX(commonnality)
INTO @max_commonnality FROM
    level_one_tmp;
drop temporary table if exists level_one_with_riskpoint;
create temporary table level_one_with_riskpoint
SELECT 
    *,
    RISKPOINT(commonnality,
            @max_commonnality,
            round(significant,5),
            failureContRation,
            defectqty) AS riskpoint
FROM
    level_one_tmp;
drop temporary table if exists level_one_with_riskrank;
create temporary table level_one_with_riskrank
SELECT 
    *,
    riskrank(riskpoint,
            round(commonnality,1),
            round(significant,1)) AS riskrank
FROM
    level_one_with_riskpoint;
SELECT 
    *
FROM
    level_three_with_riskrank
ORDER BY riskrank DESC;
drop table level_one_tmp;
drop table level_one_with_riskpoint;
drop table level_one_with_riskrank;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `level_two_test` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `level_two_test`(in pdt varchar(100),in flr varchar(45),in starttime datetime,in stoptime datetime)
BEGIN
  drop temporary table if exists level_two_tmp ;
  if !isnull(flr) then
  create temporary table level_two_tmp
  select product,factory,floor,symptonid,failure_sympton,riskname,riskcode,failQty,defectQty,
  round(sum(failQty)/sum(defectQty),10) as commonnality,outputQty,throughputQty,
  round(sum(throughputQty)/sum(outputQty),10) as throughput_ratio,
  (round(sum(failQty)/sum(defectQty),10)-round(sum(throughputQty)/sum(outputQty),10))/sqrt(round(sum(throughputQty)/sum(outputQty),10)*(1-round(sum(throughputQty)/sum(outputQty),10))/defectqty) as significant,
  sum(failQty),
  sum(throughputQty),
  round((sum(risk_failureduration_sec)/ sum(asm_duration_sec))*100,2) as failureContRation 
  FROM `rca-ipcd-lx`.risk_test_station where product=pdt and floor=flr and test_starttime >= starttime and test_starttime < stoptime group by riskname;
  else
   create temporary table level_two_tmp
  select product,factory,floor,symptonid,failure_sympton,riskname,riskcode,failQty,defectQty,
  round(sum(failQty)/sum(defectQty),10) as commonnality,outputQty,throughputQty,
  round(sum(throughputQty)/sum(outputQty),10) as throughput_ratio,
  (round(sum(failQty)/sum(defectQty),10)-round(sum(throughputQty)/sum(outputQty),10))/sqrt(round(sum(throughputQty)/sum(outputQty),10)*(1-round(sum(throughputQty)/sum(outputQty),10))/defectqty) as significant,
  sum(failQty),
  sum(throughputQty),
  round((sum(risk_failureduration_sec)/ sum(asm_duration_sec))*100,2) as failureContRation 
  FROM `rca-ipcd-lx`.risk_test_station where product=pdt and test_starttime >= starttime and test_starttime < stoptime group by riskname;
  end if;
SELECT 
    MAX(commonnality)
INTO @max_commonnality FROM
    level_two_tmp;
drop temporary table if exists level_two_with_riskpoint;
create temporary table level_two_with_riskpoint
SELECT 
    *,
    RISKPOINT(commonnality,
            @max_commonnality,
            round(significant,5),
            failureContRation,
            defectqty) AS riskpoint
FROM
    level_two_tmp;
drop temporary table if exists level_two_test_with_riskrank;
create temporary table level_two_test_with_riskrank
SELECT 
    *,
    riskrank(riskpoint,
            round(commonnality,1),
            round(significant,1)) AS riskrank
FROM
    level_two_with_riskpoint;
SELECT 
    *
FROM
    level_two_test_with_riskrank
ORDER BY riskrank DESC;
#drop table level_three_tmp;
#drop table level_three_with_riskpoint;
#drop table level_three_with_riskrank;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `new_level_one` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `new_level_one`(in pdt varchar(100),
	in flr varchar(45),
	in starttime datetime,
	in stoptime datetime,
	in top int)
BEGIN
	#Create aggreated level_one based on ui conditions
 drop
		temporary table
			if exists level_one_tmp ;

if ! isnull(flr) then create
	temporary table
		level_one_tmp select
			*
		FROM
			`rca-ipcd-lx`.risk_assembly_station
		where
			product = pdt
			and floor = flr
			and test_starttime >= starttime
			and test_starttime < stoptime;

insert
	into
		level_one_tmp select
			*
		from
			risk_test_station
		where
			product = pdt
			and floor = flr
			and test_starttime >= starttime
			and test_starttime < stoptime;

insert
	into
		level_one_tmp select
			*
		from
			risk_part
		where
			product = pdt
			and floor = flr
			and test_starttime >= starttime
			and test_starttime < stoptime;
else create
	temporary table
		level_one_tmp select
			*
		FROM
			`rca-ipcd-lx`.risk_assembly_station
		where
			product = pdt
			and test_starttime >= starttime
			and test_starttime < stoptime;

insert
	into
		level_one_tmp select
			*
		from
			risk_test_station
		where
			product = pdt
			and test_starttime >= starttime
			and test_starttime < stoptime;

insert
	into
		level_one_tmp select
			*
		from
			risk_part
		where
			product = pdt
			and test_starttime >= starttime
			and test_starttime < stoptime;
end if;
#Compute top symptons
 drop
	temporary table
		if exists level_one_tmp_all_top_sympton;

create
	temporary table
		level_one_tmp_all_top_sympton SELECT
			teststation,
			failure_sympton,
			round(sum(failQty)/ sum(outputQty), 5) as failurerate,
			max(defectqty) as defectqty,
			round(sum(sympton_failureduration_sec)/ 3600, 2) as failure_conthour
		FROM
			level_one_tmp
		group by
			teststation,
			failure_sympton
		order by
			failurerate desc
		limit top;
SET
@rank := 0;

SELECT
	@rank := @rank + 1 AS rank,
	teststation,
	failure_sympton,
	failurerate,
	defectqty,
	failure_conthour
FROM
	level_one_tmp_all_top_sympton;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `new_level_three` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'STRICT_TRANS_TABLES,STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `new_level_three`(in pdt varchar(100),
	in flr varchar(45),
	in tstation varchar(100),
	in fsymp varchar(256),
	in rname varchar(100),
	in risktype varchar(50),
	in starttime datetime,
	in stoptime datetime)
BEGIN
	drop
		temporary table
			if exists level_three_tmp ;
-- 	
-- select defectqty for specific sympton
-- 	
 SELECT
	sum(defectqty) INTO
		@sympton_defectqty
	FROM
		(
		SELECT
			max(defectqty) as defectqty
		FROM
			`rca-ipcd-lx`.risk_assembly_station
		where
			product = pdt
			and floor = flr
			and teststation = tstation
			and failure_sympton = fsymp
			and test_starttime >= starttime
			and test_starttime < stoptime
		group by
			teststation,
			failure_sympton,
			test_starttime ) symp;
-- 		
-- 	
 if ! isnull(flr) then
CASE
	risktype
	WHEN 'assembly' THEN create
		temporary table
			level_three_tmp select
				riskCode,
				round(sum(failQty)/ @sympton_defectqty, 10) as commonnality,
				round(sum(throughputQty)/ sum(outputQty), 10) as throughput_ratio,
				(round(sum(failQty)/ @sympton_defectqty, 10)-round(sum(throughputQty)/ sum(outputQty), 10))/ sqrt(round(sum(throughputQty)/ sum(outputQty), 10)*(1-round(sum(throughputQty)/ sum(outputQty), 10))/ @sympton_defectqty) as significant,
			sum(failQty) as failqty,
            @sympton_defectqty as defectqty,
            sum(outputQty) as outputqty,
			sum(throughputQty) as throughputqty,
				round((sum(risk_failureduration_sec)/ sum(asm_duration_sec))* 100, 2) as failureContRation
			FROM
				`rca-ipcd-lx`.risk_assembly_station
			where
				product = pdt
				and floor = flr
				and teststation = tstation
				and failure_sympton = fsymp
				and riskname = rname
				and test_starttime >= starttime
				and test_starttime < stoptime
			group by
				riskcode;
WHEN 'part' THEN create
	temporary table
		level_three_tmp select
			riskCode,
			round(sum(failQty)/ @sympton_defectqty, 10) as commonnality,
			round(sum(throughputQty)/ sum(outputQty), 10) as throughput_ratio,
			(round(sum(failQty)/ @sympton_defectqty, 10)-round(sum(throughputQty)/ sum(outputQty), 10))/ sqrt(round(sum(throughputQty)/ sum(outputQty), 10)*(1-round(sum(throughputQty)/ sum(outputQty), 10))/ @sympton_defectqty) as significant,
			sum(failQty) as failqty,
            @sympton_defectqty as defectqty,
            sum(outputQty) as outputqty,
			sum(throughputQty) as throughputqty,
			round((sum(risk_failureduration_sec)/ sum(asm_duration_sec))* 100, 2) as failureContRation
		FROM
			`rca-ipcd-lx`.risk_part
		where
			product = pdt
			and floor = flr
			and teststation = tstation
			and failure_sympton = fsymp
			and riskname = rname
			and test_starttime >= starttime
			and test_starttime < stoptime
		group by
			riskcode;
WHEN 'test' THEN create
	temporary table
		level_three_tmp select
			riskCode,
			round(sum(failQty)/ @sympton_defectqty, 10) as commonnality,
			round(sum(throughputQty)/ sum(outputQty), 10) as throughput_ratio,
			(round(sum(failQty)/ @sympton_defectqty, 10)-round(sum(throughputQty)/ sum(outputQty), 10))/ sqrt(round(sum(throughputQty)/ sum(outputQty), 10)*(1-round(sum(throughputQty)/ sum(outputQty), 10))/ @sympton_defectqty) as significant,
			sum(failQty) as failqty,
            @sympton_defectqty as defectqty,
            sum(outputQty) as outputqty,
			sum(throughputQty) as throughputqty,
			round((sum(risk_failureduration_sec)/ sum(asm_duration_sec))* 100, 2) as failureContRation
		FROM
			`rca-ipcd-lx`.risk_test_station
		where
			product = pdt
			and floor = flr
			and teststation = tstation
			and failure_sympton = fsymp
			and riskname = rname
			and test_starttime >= starttime
			and test_starttime < stoptime
		group by
			riskcode;
END
CASE;
else
CASE
	risktype
	WHEN 'assembly' THEN create
		temporary table
			level_three_tmp select
				riskCode,
				round(sum(failQty)/ @sympton_defectqty, 10) as commonnality,
				round(sum(throughputQty)/ sum(outputQty), 10) as throughput_ratio,
				(round(sum(failQty)/ @sympton_defectqty, 10)-round(sum(throughputQty)/ sum(outputQty), 10))/ sqrt(round(sum(throughputQty)/ sum(outputQty), 10)*(1-round(sum(throughputQty)/ sum(outputQty), 10))/ @sympton_defectqty) as significant,
			sum(failQty) as failqty,
            @sympton_defectqty as defectqty,
            sum(outputQty) as outputqty,
			sum(throughputQty) as throughputqty,
				round((sum(risk_failureduration_sec)/ sum(asm_duration_sec))* 100, 2) as failureContRation
			FROM
				`rca-ipcd-lx`.risk_assembly_station
			where
				product = pdt
				and teststation = tstation
				and failure_sympton = fsymp
				and riskname = rname
				and test_starttime >= starttime
				and test_starttime < stoptime
			group by
				riskcode;
WHEN 'part' THEN create
	temporary table
		level_three_tmp select
			riskCode,
			round(sum(failQty)/ @sympton_defectqty, 10) as commonnality,
			round(sum(throughputQty)/ sum(outputQty), 10) as throughput_ratio,
			(round(sum(failQty)/ @sympton_defectqty, 10)-round(sum(throughputQty)/ sum(outputQty), 10))/ sqrt(round(sum(throughputQty)/ sum(outputQty), 10)*(1-round(sum(throughputQty)/ sum(outputQty), 10))/ @sympton_defectqty) as significant,
			sum(failQty) as failqty,
            @sympton_defectqty as defectqty,
            sum(outputQty) as outputqty,
			sum(throughputQty) as throughputqty,
			round((sum(risk_failureduration_sec)/ sum(asm_duration_sec))* 100, 2) as failureContRation
		FROM
			`rca-ipcd-lx`.risk_part
		where
			product = pdt
			and teststation = tstation
			and failure_sympton = fsymp
			and riskname = rname
			and test_starttime >= starttime
			and test_starttime < stoptime
		group by
			riskcode;
WHEN 'test' THEN create
	temporary table
		level_three_tmp select
			riskCode,
			round(sum(failQty)/ @sympton_defectqty, 10) as commonnality,
			round(sum(throughputQty)/ sum(outputQty), 10) as throughput_ratio,
			(round(sum(failQty)/ @sympton_defectqty, 10)-round(sum(throughputQty)/ sum(outputQty), 10))/ sqrt(round(sum(throughputQty)/ sum(outputQty), 10)*(1-round(sum(throughputQty)/ sum(outputQty), 10))/ @sympton_defectqty) as significant,
			sum(failQty) as failqty,
            @sympton_defectqty as defectqty,
            sum(outputQty) as outputqty,
			sum(throughputQty) as throughputqty,
			round((sum(risk_failureduration_sec)/ sum(asm_duration_sec))* 100, 2) as failureContRation
		FROM
			`rca-ipcd-lx`.risk_test_station
		where
			product = pdt
			and teststation = tstation
			and failure_sympton = fsymp
			and riskname = rname
			and test_starttime >= starttime
			and test_starttime < stoptime
		group by
			riskcode;
END
CASE;
end if;

SELECT
	MAX(commonnality) INTO
		@max_commonnality
	FROM
		level_three_tmp;

drop
	temporary table
		if exists level_three_with_riskpoint;

create
	temporary table
		level_three_with_riskpoint SELECT
			*,
			RISKPOINT(commonnality,
			@max_commonnality,
			round(significant, 5),
			failureContRation,
			@sympton_defectqty) AS riskpoint
		FROM
			level_three_tmp;

drop
	temporary table
		if exists level_three_with_riskrank;

create
	temporary table
		level_three_with_riskrank SELECT
			*,
			riskrank(riskpoint,
			round(commonnality, 1),
			round(significant, 1)) AS riskrank
		FROM
			level_three_with_riskpoint;

SELECT
	*
FROM
	level_three_with_riskrank
ORDER BY
	riskrank DESC;
-- drop
-- 	table
-- 		level_one_tmp;
-- 
-- drop
-- 	table
-- 		level_one_with_riskpoint;
-- 
-- drop
-- 	table
-- 		level_one_with_riskrank;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `new_level_three_max_rank` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `new_level_three_max_rank`(in pdt varchar(100),
	in flr varchar(45),
	in tstation varchar(100),
	in fsymp varchar(256),
	in rname varchar(100),
	in risktype varchar(50),
	in starttime datetime,
	in stoptime datetime)
BEGIN
	drop
		temporary table
			if exists level_three_tmp ;
-- 	
-- select defectqty for specific sympton
-- 	
 SELECT
	sum(defectqty) INTO
		@sympton_defectqty
	FROM
		(
		SELECT
			max(defectqty) as defectqty
		FROM
			`rca-ipcd-lx`.risk_assembly_station
		where
			product = pdt
			and floor = flr
			and teststation = tstation
			and failure_sympton = fsymp
			and test_starttime >= starttime
			and test_starttime < stoptime
		group by
			teststation,
			failure_sympton,
			test_starttime ) symp;
-- 		
-- 	
 if ! isnull(flr) then
CASE
	risktype
	WHEN 'assembly' THEN create
		temporary table
			level_three_tmp select
				riskCode,
				round(sum(failQty)/ @sympton_defectqty, 10) as commonnality,
				round(sum(throughputQty)/ sum(outputQty), 10) as throughput_ratio,
				significant(sum(failQty),
				@sympton_defectqty,
				sum(throughputQty),
				sum(outputQty)) as significant,
				@sympton_defectqty as defectqty,
				sum(failQty) as failqty,
				sum(throughputqty) as throughputqty,
				sum(outputqty) as outputqty,
				round((sum(risk_failureduration_sec)/ sum(asm_duration_sec))* 100, 2) as failureContRation
			FROM
				`rca-ipcd-lx`.risk_assembly_station
			where
				product = pdt
				and floor = flr
				and teststation = tstation
				and failure_sympton = fsymp
				and riskname = rname
				and test_starttime >= starttime
				and test_starttime < stoptime
			group by
				riskcode;
WHEN 'part' THEN create
	temporary table
		level_three_tmp select
			riskCode,
			round(sum(failQty)/ @sympton_defectqty, 10) as commonnality,
			round(sum(throughputQty)/ sum(outputQty), 10) as throughput_ratio,
			significant(sum(failQty),
			@sympton_defectqty,
			sum(throughputQty),
			sum(outputQty)) as significant,
			@sympton_defectqty as defectqty,
			sum(failQty) as failqty,
			sum(throughputqty) as throughputqty,
			sum(outputqty) as outputqty,
			round((sum(risk_failureduration_sec)/ sum(asm_duration_sec))* 100, 2) as failureContRation
		FROM
			`rca-ipcd-lx`.risk_part
		where
			product = pdt
			and floor = flr
			and teststation = tstation
			and failure_sympton = fsymp
			and riskname = rname
			and test_starttime >= starttime
			and test_starttime < stoptime
		group by
			riskcode;
WHEN 'test' THEN create
	temporary table
		level_three_tmp select
			riskCode,
			round(sum(failQty)/ @sympton_defectqty, 10) as commonnality,
			round(sum(throughputQty)/ sum(outputQty), 10) as throughput_ratio,
			significant(sum(failQty),
			@sympton_defectqty,
			sum(throughputQty),
			sum(outputQty)) as significant,
			@sympton_defectqty as defectqty,
			sum(failQty) as failqty,
			sum(throughputqty) as throughputqty,
			sum(outputqty) as outputqty,
			round((sum(risk_failureduration_sec)/ sum(asm_duration_sec))* 100, 2) as failureContRation
		FROM
			`rca-ipcd-lx`.risk_test_station
		where
			product = pdt
			and floor = flr
			and teststation = tstation
			and failure_sympton = fsymp
			and riskname = rname
			and test_starttime >= starttime
			and test_starttime < stoptime
		group by
			riskcode;
END
CASE;
else
CASE
	risktype
	WHEN 'assembly' THEN create
		temporary table
			level_three_tmp select
				riskCode,
				round(sum(failQty)/ @sympton_defectqty, 10) as commonnality,
				round(sum(throughputQty)/ sum(outputQty), 10) as throughput_ratio,
				significant(sum(failQty),
				@sympton_defectqty,
				sum(throughputQty),
				sum(outputQty)) as significant,
				sum(failQty) as failqty,
				@sympton_defectqty as defectqty,
				sum(throughputqty) as throughputqty,
				sum(outputqty) as outputqty,
				round((sum(risk_failureduration_sec)/ sum(asm_duration_sec))* 100, 2) as failureContRation
			FROM
				`rca-ipcd-lx`.risk_assembly_station
			where
				product = pdt
				and teststation = tstation
				and failure_sympton = fsymp
				and riskname = rname
				and test_starttime >= starttime
				and test_starttime < stoptime
			group by
				riskcode;
WHEN 'part' THEN create
	temporary table
		level_three_tmp select
			riskCode,
			round(sum(failQty)/ @sympton_defectqty, 10) as commonnality,
			round(sum(throughputQty)/ sum(outputQty), 10) as throughput_ratio,
			significant(sum(failQty),
			@sympton_defectqty,
			sum(throughputQty),
			sum(outputQty)) as significant,
			sum(failQty) as failqty,
			@sympton_defectqty as defectqty,
			sum(throughputqty) as throughputqty,
			sum(outputqty) as outputqty,
			round((sum(risk_failureduration_sec)/ sum(asm_duration_sec))* 100, 2) as failureContRation
		FROM
			`rca-ipcd-lx`.risk_part
		where
			product = pdt
			and teststation = tstation
			and failure_sympton = fsymp
			and riskname = rname
			and test_starttime >= starttime
			and test_starttime < stoptime
		group by
			riskcode;
WHEN 'test' THEN create
	temporary table
		level_three_tmp select
			riskCode,
			round(sum(failQty)/ @sympton_defectqty, 10) as commonnality,
			round(sum(throughputQty)/ sum(outputQty), 10) as throughput_ratio,
			significant(sum(failQty),
			@sympton_defectqty,
			sum(throughputQty),
			sum(outputQty)) as significant,
			sum(failQty) as failqty,
			@sympton_defectqty as defectqty,
			sum(throughputqty) as throughputqty,
			sum(outputqty) as outputqty,
			round((sum(risk_failureduration_sec)/ sum(asm_duration_sec))* 100, 2) as failureContRation
		FROM
			`rca-ipcd-lx`.risk_test_station
		where
			product = pdt
			and teststation = tstation
			and failure_sympton = fsymp
			and riskname = rname
			and test_starttime >= starttime
			and test_starttime < stoptime
		group by
			riskcode;
END
CASE;
end if;

SELECT
	MAX(commonnality) INTO
		@max_commonnality
	FROM
		level_three_tmp;

drop
	temporary table
		if exists level_three_with_riskpoint;

create
	temporary table
		level_three_with_riskpoint SELECT
			*,
			RISKPOINT(commonnality,
			@max_commonnality,
			round(significant, 5),
			failureContRation,
			@sympton_defectqty) AS riskpoint
		FROM
			level_three_tmp;

drop
	temporary table
		if exists level_three_with_riskrank;

create
	temporary table
		level_three_with_riskrank SELECT
			*,
			riskrank(riskpoint,
			round(commonnality, 1),
			round(significant, 1)) AS riskrank
		FROM
			level_three_with_riskpoint;

SELECT
	*
FROM
	level_three_with_riskrank
ORDER BY
	riskrank DESC
LIMIT 1;
-- drop
-- 	table
-- 		level_one_tmp;
-- 
-- drop
-- 	table
-- 		level_one_with_riskpoint;
-- 
-- drop
-- 	table
-- 		level_one_with_riskrank;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `new_level_two` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `new_level_two`(in pdt varchar(100),
	in flr varchar(45),
	in tstation varchar(100),
	in fsymp varchar(256),
	in starttime datetime,
	in stoptime datetime)
BEGIN
	#Create aggreated level_two based on ui conditions
 drop
		temporary table
			if exists level_two_tmp ;
-- 		
-- compute risk_part values
-- 
-- 	
-- select defectqty for specific sympton
-- 	
 SELECT
	sum(defectqty) INTO
		@sympton_defectqty
	FROM
		(
		SELECT
			max(defectqty) as defectqty
		FROM
			`rca-ipcd-lx`.risk_part
		where
			product = pdt
			and floor = flr
			and teststation = tstation
			and failure_sympton = fsymp
			and test_starttime >= starttime
			and test_starttime < stoptime
		group by
			teststation,
			failure_sympton,
			test_starttime ) symp;
-- 		
-- 		
 if ! isnull(flr) then create
	temporary table
		level_two_tmp select
			riskname,
			round(sum(failQty)/ @sympton_defectqty, 10) as commonnality,
			round(sum(throughputQty)/ sum(outputQty), 10) as throughput_ratio,
			significant(sum(failQty),
			@sympton_defectqty,
			sum(throughputQty),
			sum(outputQty)) as significant,
			sum(failQty),
			sum(throughputQty),
			round((sum(risk_failureduration_sec)/ sum(asm_duration_sec))* 100, 2) as failureContRation
		FROM
			`rca-ipcd-lx`.risk_part
		where
			product = pdt
			and floor = flr
			and teststation = teststation
			and failure_sympton = fsymp
			and test_starttime >= starttime
			and test_starttime < stoptime
		group by
			riskname;
else create
	temporary table
		level_two_tmp select
			riskname,
			round(sum(failQty)/ @sympton_defectqty, 10) as commonnality,
			round(sum(throughputQty)/ sum(outputQty), 10) as throughput_ratio,
			significant(sum(failQty),
			@sympton_defectqty,
			sum(throughputQty),
			sum(outputQty)) as significant,
			sum(failQty),
			sum(throughputQty),
			round((sum(risk_failureduration_sec)/ sum(asm_duration_sec))* 100, 2) as failureContRation
		FROM
			`rca-ipcd-lx`.risk_part
		where
			product = pdt
			and teststation = teststation
			and failure_sympton = fsymp
			and test_starttime >= starttime
			and test_starttime < stoptime
		group by
			riskname;
end if;

SELECT
	MAX(commonnality) INTO
		@max_commonnality
	FROM
		level_two_tmp;

drop
	temporary table
		if exists level_two_with_riskpoint;

create
	temporary table
		level_two_with_riskpoint SELECT
			*,
			RISKPOINT(commonnality,
			@max_commonnality,
			round(significant, 5),
			failureContRation,
			@sympton_defectqty) AS riskpoint,
			'part' AS risktype
		FROM
			level_two_tmp;

alter table
	level_two_with_riskpoint modify risktype varchar(50);
-- 		
-- compute risk_assembly values
-- 
-- 	
-- select defectqty for specific sympton
-- 	
 SELECT
	sum(defectqty) INTO
		@sympton_defectqty
	FROM
		(
		SELECT
			max(defectqty) as defectqty
		FROM
			`rca-ipcd-lx`.risk_assembly_station
		where
			product = pdt
			and floor = flr
			and teststation = teststation
			and failure_sympton = fsymp
			and test_starttime >= starttime
			and test_starttime < stoptime
		group by
			teststation,
			failure_sympton,
			test_starttime ) symp;
-- 		
-- 	
 drop
	temporary table
		if exists level_two_tmp ;

if ! isnull(flr) then create
	temporary table
		level_two_tmp select
			riskname,
			round(sum(failQty)/ @sympton_defectqty, 10) as commonnality,
			round(sum(throughputQty)/ sum(outputQty), 10) as throughput_ratio,
			significant(sum(failQty),
			@sympton_defectqty,
			sum(throughputQty),
			sum(outputQty)) as significant,
			sum(failQty),
			sum(throughputQty),
			round((sum(risk_failureduration_sec)/ sum(asm_duration_sec))* 100, 2) as failureContRation
		FROM
			`rca-ipcd-lx`.risk_assembly_station
		where
			product = pdt
			and floor = flr
			and teststation = teststation
			and failure_sympton = fsymp
			and test_starttime >= starttime
			and test_starttime < stoptime
		group by
			riskname;
else create
	temporary table
		level_two_tmp select
			riskname,
			round(sum(failQty)/ @sympton_defectqty, 10) as commonnality,
			round(sum(throughputQty)/ sum(outputQty), 10) as throughput_ratio,
			significant(sum(failQty),
			@sympton_defectqty,
			sum(throughputQty),
			sum(outputQty)) as significant,
			sum(failQty),
			sum(throughputQty),
			round((sum(risk_failureduration_sec)/ sum(asm_duration_sec))* 100, 2) as failureContRation
		FROM
			`rca-ipcd-lx`.risk_assembly_station
		where
			product = pdt
			and teststation = teststation
			and failure_sympton = fsymp
			and test_starttime >= starttime
			and test_starttime < stoptime
		group by
			riskname;
end if;

SELECT
	MAX(commonnality) INTO
		@max_commonnality
	FROM
		level_two_tmp;

INSERT
	INTO
		level_two_with_riskpoint SELECT
			*,
			RISKPOINT(commonnality,
			@max_commonnality,
			round(significant, 5),
			failureContRation,
			@sympton_defectqty) AS riskpoint,
			'assembly' AS risktype
		FROM
			level_two_tmp;
-- 		
-- compute risk_test values
-- 
-- select defectqty for specific sympton
-- 	
 SELECT
	sum(defectqty) INTO
		@sympton_defectqty
	FROM
		(
		SELECT
			max(defectqty) as defectqty
		FROM
			`rca-ipcd-lx`.risk_test_station
		where
			product = pdt
			and floor = flr
			and teststation = teststation
			and failure_sympton = fsymp
			and test_starttime >= starttime
			and test_starttime < stoptime
		group by
			teststation,
			failure_sympton,
			test_starttime ) symp;
-- 		
-- 	
 drop
	temporary table
		if exists level_two_tmp ;

if ! isnull(flr) then create
	temporary table
		level_two_tmp select
			riskname,
			round(sum(failQty)/ @sympton_defectqty, 10) as commonnality,
			round(sum(throughputQty)/ sum(outputQty), 10) as throughput_ratio,
			significant(sum(failQty),
			@sympton_defectqty,
			sum(throughputQty),
			sum(outputQty)) as significant,
			sum(failQty),
			sum(throughputQty),
			round((sum(risk_failureduration_sec)/ sum(asm_duration_sec))* 100, 2) as failureContRation
		FROM
			`rca-ipcd-lx`.risk_test_station
		where
			product = pdt
			and floor = flr
			and teststation = teststation
			and failure_sympton = fsymp
			and test_starttime >= starttime
			and test_starttime < stoptime
		group by
			riskname;
else create
	temporary table
		level_two_tmp select
			riskname,
			round(sum(failQty)/ @sympton_defectqty, 10) as commonnality,
			round(sum(throughputQty)/ sum(outputQty), 10) as throughput_ratio,
			significant(sum(failQty),
			@sympton_defectqty,
			sum(throughputQty),
			sum(outputQty)) as significant,
			sum(failQty),
			sum(throughputQty),
			round((sum(risk_failureduration_sec)/ sum(asm_duration_sec))* 100, 2) as failureContRation
		FROM
			`rca-ipcd-lx`.risk_test_station
		where
			product = pdt
			and teststation = teststation
			and failure_sympton = fsymp
			and test_starttime >= starttime
			and test_starttime < stoptime
		group by
			riskname;
end if;

SELECT
	MAX(commonnality) INTO
		@max_commonnality
	FROM
		level_two_tmp;

INSERT
	INTO
		level_two_with_riskpoint SELECT
			*,
			RISKPOINT(commonnality,
			@max_commonnality,
			round(significant, 5),
			failureContRation,
			@sympton_defectqty) AS riskpoint,
			'test' AS risktype
		FROM
			level_two_tmp;
-- 
-- SELECT
-- 	*
-- FROM
-- 	level_two_with_riskpoint;
-- Compute risk rank 
 drop
	temporary table
		if exists level_two_part_with_riskrank;

create
	temporary table
		level_two_part_with_riskrank SELECT
			*,
			riskrank(riskpoint,
			round(commonnality, 1),
			round(significant, 1)) AS riskrank
		FROM
			level_two_with_riskpoint;

SELECT
	*
FROM
	level_two_part_with_riskrank
ORDER BY
	riskrank DESC;
#drop table level_three_tmp;
#drop table level_three_with_riskpoint;
#drop table level_three_with_riskrank;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `new_level_two_max_rank` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `new_level_two_max_rank`(in pdt varchar(100),
	in flr varchar(45),
	in tstation varchar(100),
	in fsymp varchar(256),
	in starttime datetime,
	in stoptime datetime)
BEGIN
	#Create aggreated level_two based on ui conditions
 drop
		temporary table
			if exists level_two_tmp ;
-- 		
-- compute risk_part values
-- 
-- 	
-- select defectqty for specific sympton
-- 	
 SELECT
	sum(defectqty) INTO
		@sympton_defectqty
	FROM
		(
		SELECT
			max(defectqty) as defectqty
		FROM
			`rca-ipcd-lx`.risk_part
		where
			product = pdt
			and floor = flr
			and teststation = tstation
			and failure_sympton = fsymp
			and test_starttime >= starttime
			and test_starttime < stoptime
		group by
			teststation,
			failure_sympton,
			test_starttime ) symp;
-- 		
-- 		
 if ! isnull(flr) then create
	temporary table
		level_two_tmp select
			riskname,
			round(sum(failQty)/ @sympton_defectqty, 10) as commonnality,
			round(sum(throughputQty)/ sum(outputQty), 10) as throughput_ratio,
			significant(sum(failQty),
			@sympton_defectqty,
			sum(throughputQty),
			sum(outputQty)) as significant,
			sum(failQty),
			sum(throughputQty),
			round((sum(risk_failureduration_sec)/ sum(asm_duration_sec))* 100, 2) as failureContRation
		FROM
			`rca-ipcd-lx`.risk_part
		where
			product = pdt
			and floor = flr
			and teststation = teststation
			and failure_sympton = fsymp
			and test_starttime >= starttime
			and test_starttime < stoptime
		group by
			riskname;
else create
	temporary table
		level_two_tmp select
			riskname,
			round(sum(failQty)/ @sympton_defectqty, 10) as commonnality,
			round(sum(throughputQty)/ sum(outputQty), 10) as throughput_ratio,
			significant(sum(failQty),
			@sympton_defectqty,
			sum(throughputQty),
			sum(outputQty)) as significant,
			sum(failQty),
			sum(throughputQty),
			round((sum(risk_failureduration_sec)/ sum(asm_duration_sec))* 100, 2) as failureContRation
		FROM
			`rca-ipcd-lx`.risk_part
		where
			product = pdt
			and teststation = teststation
			and failure_sympton = fsymp
			and test_starttime >= starttime
			and test_starttime < stoptime
		group by
			riskname;
end if;

SELECT
	MAX(commonnality) INTO
		@max_commonnality
	FROM
		level_two_tmp;

drop
	temporary table
		if exists level_two_with_riskpoint;

create
	temporary table
		level_two_with_riskpoint SELECT
			*,
			RISKPOINT(commonnality,
			@max_commonnality,
			round(significant, 5),
			failureContRation,
			@sympton_defectqty) AS riskpoint,
			'part' AS risktype
		FROM
			level_two_tmp;

alter table
	level_two_with_riskpoint modify risktype varchar(50);
-- 		
-- compute risk_assembly values
-- 
-- 	
-- select defectqty for specific sympton
-- 	
 SELECT
	sum(defectqty) INTO
		@sympton_defectqty
	FROM
		(
		SELECT
			max(defectqty) as defectqty
		FROM
			`rca-ipcd-lx`.risk_assembly_station
		where
			product = pdt
			and floor = flr
			and teststation = teststation
			and failure_sympton = fsymp
			and test_starttime >= starttime
			and test_starttime < stoptime
		group by
			teststation,
			failure_sympton,
			test_starttime ) symp;
-- 		
-- 	
 drop
	temporary table
		if exists level_two_tmp ;

if ! isnull(flr) then create
	temporary table
		level_two_tmp select
			riskname,
			round(sum(failQty)/ @sympton_defectqty, 10) as commonnality,
			round(sum(throughputQty)/ sum(outputQty), 10) as throughput_ratio,
			significant(sum(failQty),
			@sympton_defectqty,
			sum(throughputQty),
			sum(outputQty)) as significant,
			sum(failQty),
			sum(throughputQty),
			round((sum(risk_failureduration_sec)/ sum(asm_duration_sec))* 100, 2) as failureContRation
		FROM
			`rca-ipcd-lx`.risk_assembly_station
		where
			product = pdt
			and floor = flr
			and teststation = teststation
			and failure_sympton = fsymp
			and test_starttime >= starttime
			and test_starttime < stoptime
		group by
			riskname;
else create
	temporary table
		level_two_tmp select
			riskname,
			round(sum(failQty)/ @sympton_defectqty, 10) as commonnality,
			round(sum(throughputQty)/ sum(outputQty), 10) as throughput_ratio,
			significant(sum(failQty),
			@sympton_defectqty,
			sum(throughputQty),
			sum(outputQty)) as significant,
			sum(failQty),
			sum(throughputQty),
			round((sum(risk_failureduration_sec)/ sum(asm_duration_sec))* 100, 2) as failureContRation
		FROM
			`rca-ipcd-lx`.risk_assembly_station
		where
			product = pdt
			and teststation = teststation
			and failure_sympton = fsymp
			and test_starttime >= starttime
			and test_starttime < stoptime
		group by
			riskname;
end if;

SELECT
	MAX(commonnality) INTO
		@max_commonnality
	FROM
		level_two_tmp;

INSERT
	INTO
		level_two_with_riskpoint SELECT
			*,
			RISKPOINT(commonnality,
			@max_commonnality,
			round(significant, 5),
			failureContRation,
			@sympton_defectqty) AS riskpoint,
			'assembly' AS risktype
		FROM
			level_two_tmp;
-- 		
-- compute risk_test values
-- 
-- select defectqty for specific sympton
-- 	
 SELECT
	sum(defectqty) INTO
		@sympton_defectqty
	FROM
		(
		SELECT
			max(defectqty) as defectqty
		FROM
			`rca-ipcd-lx`.risk_test_station
		where
			product = pdt
			and floor = flr
			and teststation = teststation
			and failure_sympton = fsymp
			and test_starttime >= starttime
			and test_starttime < stoptime
		group by
			teststation,
			failure_sympton,
			test_starttime ) symp;
-- 		
-- 	
 drop
	temporary table
		if exists level_two_tmp ;

if ! isnull(flr) then create
	temporary table
		level_two_tmp select
			riskname,
			round(sum(failQty)/ @sympton_defectqty, 10) as commonnality,
			round(sum(throughputQty)/ sum(outputQty), 10) as throughput_ratio,
			significant(sum(failQty),
			@sympton_defectqty,
			sum(throughputQty),
			sum(outputQty)) as significant,
			sum(failQty),
			sum(throughputQty),
			round((sum(risk_failureduration_sec)/ sum(asm_duration_sec))* 100, 2) as failureContRation
		FROM
			`rca-ipcd-lx`.risk_test_station
		where
			product = pdt
			and floor = flr
			and teststation = teststation
			and failure_sympton = fsymp
			and test_starttime >= starttime
			and test_starttime < stoptime
		group by
			riskname;
else create
	temporary table
		level_two_tmp select
			riskname,
			round(sum(failQty)/ @sympton_defectqty, 10) as commonnality,
			round(sum(throughputQty)/ sum(outputQty), 10) as throughput_ratio,
			significant(sum(failQty),
			@sympton_defectqty,
			sum(throughputQty),
			sum(outputQty)) as significant,
			sum(failQty),
			sum(throughputQty),
			round((sum(risk_failureduration_sec)/ sum(asm_duration_sec))* 100, 2) as failureContRation
		FROM
			`rca-ipcd-lx`.risk_test_station
		where
			product = pdt
			and teststation = teststation
			and failure_sympton = fsymp
			and test_starttime >= starttime
			and test_starttime < stoptime
		group by
			riskname;
end if;

SELECT
	MAX(commonnality) INTO
		@max_commonnality
	FROM
		level_two_tmp;

INSERT
	INTO
		level_two_with_riskpoint SELECT
			*,
			RISKPOINT(commonnality,
			@max_commonnality,
			round(significant, 5),
			failureContRation,
			@sympton_defectqty) AS riskpoint,
			'test' AS risktype
		FROM
			level_two_tmp;
-- 
-- SELECT
-- 	*
-- FROM
-- 	level_two_with_riskpoint;
-- Compute risk rank 
 drop
	temporary table
		if exists level_two_part_with_riskrank;

create
	temporary table
		level_two_part_with_riskrank SELECT
			*,
			riskrank(riskpoint,
			round(commonnality, 1),
			round(significant, 1)) AS riskrank
		FROM
			level_two_with_riskpoint;

SELECT
	*
FROM
	level_two_part_with_riskrank
ORDER BY
	riskrank DESC LIMIT 1;
#drop table level_three_tmp;
#drop table level_three_with_riskpoint;
#drop table level_three_with_riskrank;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `wyj_level_one` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'STRICT_TRANS_TABLES,STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `wyj_level_one`(in pdt varchar(100),
	in flr varchar(45),
	in lne varchar(100),
	in starttime datetime,
	in stoptime datetime,
	in top int)
BEGIN
	#Create aggreated level_one based on ui conditions
 drop
		temporary table
			if exists level_one_tmp ;

if ! isnull(flr) then create
	temporary table
		level_one_tmp select
			*
		FROM
			`rca-ipcd-lx`.risk_assembly_station
		where
			product = pdt
			and floor = flr
			and line = lne
			and test_starttime >= starttime
			and test_starttime < stoptime;

insert
	into
		level_one_tmp select
			*
		from
			risk_test_station
		where
			product = pdt
			and floor = flr
			and line = lne
			and test_starttime >= starttime
			and test_starttime < stoptime;

insert
	into
		level_one_tmp select
			*
		from
			risk_part
		where
			product = pdt
			and floor = flr
			and line = lne
			and test_starttime >= starttime
			and test_starttime < stoptime;
else create
	temporary table
		level_one_tmp select
			*
		FROM
			`rca-ipcd-lx`.risk_assembly_station
		where
			product = pdt
			and line = lne
			and test_starttime >= starttime
			and test_starttime < stoptime;

insert
	into
		level_one_tmp select
			*
		from
			risk_test_station
		where
			product = pdt
			and line = lne
			and test_starttime >= starttime
			and test_starttime < stoptime;

insert
	into
		level_one_tmp select
			*
		from
			risk_part
		where
			product = pdt
			and line = lne
			and test_starttime >= starttime
			and test_starttime < stoptime;
end if;
#Compute top symptons
 drop
	temporary table
		if exists level_one_tmp_all_top_sympton;

create
	temporary table
		level_one_tmp_all_top_sympton SELECT
			teststation,
			failure_sympton,
			round(sum(failQty)/ sum(outputQty), 5) as failurerate,
			sum(defectQty) as defectqty,
			round(sum(sympton_failureduration_sec)/ 3600, 2) as failure_conthour
		FROM
			level_one_tmp
		group by
			teststation,
			failure_sympton
		order by
			failurerate desc
		limit top;

SET
@rank := 0;

SELECT
	@rank := @rank + 1 AS rank,
	teststation,
	failure_sympton,
	failurerate,
	defectqty,
	failure_conthour
FROM
	level_one_tmp_all_top_sympton;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `wyj_level_three_max_rank` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'STRICT_TRANS_TABLES,STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `wyj_level_three_max_rank`(in pdt varchar(100),
	in flr varchar(45),
	in lne varchar(100),
	in tstation varchar(100),
	in fsymp varchar(256),
	in rname varchar(100),
	in risktype varchar(50),
	in starttime datetime,
	in stoptime datetime)
BEGIN
	drop
		temporary table
			if exists level_three_tmp ;
-- 	
-- select defectqty for specific sympton
-- 	
 SELECT
	cast(sum(defectqty) as signed) INTO
		@sympton_defectqty
	FROM
		(
		SELECT
			max(defectqty) as defectqty
		FROM
			`rca-ipcd-lx`.risk_assembly_station
		where
			product = pdt
			and floor = flr
			and line = lne
			and teststation = tstation
			and failure_sympton = fsymp
			and test_starttime >= starttime
			and test_starttime < stoptime
		group by
			teststation,
			failure_sympton,
			test_starttime ) symp;
-- 		
-- 	
 if ! isnull(flr) then
CASE
	risktype
	WHEN 'assembly' THEN create
		temporary table
			level_three_tmp select
				riskCode,
				round(sum(failQty)/ @sympton_defectqty, 10) as commonnality,
				round(sum(throughputQty)/ sum(outputQty), 10) as throughput_ratio,
				(round(sum(failQty)/ @sympton_defectqty, 10)-round(sum(throughputQty)/ sum(outputQty), 10))/ sqrt(round(sum(throughputQty)/ sum(outputQty), 10)*(1-round(sum(throughputQty)/ sum(outputQty), 10))/ @sympton_defectqty) as significant,
				sum(failQty) as failqty,
				@sympton_defectqty as defectqty,
				sum(throughputqty) as throughputqty,
				sum(outputqty) as outputqty,
				round((sum(risk_failureduration_sec)/ sum(asm_duration_sec))* 100, 2) as failureContRation
			FROM
				`rca-ipcd-lx`.risk_assembly_station
			where
				product = pdt
				and floor = flr
				and line = lne
				and teststation = tstation
				and failure_sympton = fsymp
				and riskname = rname
				and test_starttime >= starttime
				and test_starttime < stoptime
			group by
				riskcode;
WHEN 'part' THEN create
	temporary table
		level_three_tmp select
			riskCode,
			round(sum(failQty)/ @sympton_defectqty, 10) as commonnality,
			round(sum(throughputQty)/ sum(outputQty), 10) as throughput_ratio,
			(round(sum(failQty)/ @sympton_defectqty, 10)-round(sum(throughputQty)/ sum(outputQty), 10))/ sqrt(round(sum(throughputQty)/ sum(outputQty), 10)*(1-round(sum(throughputQty)/ sum(outputQty), 10))/ @sympton_defectqty) as significant,
			sum(failQty) as failqty,
			@sympton_defectqty as defectqty,
			sum(throughputqty) as throughputqty,
			sum(outputqty) as outputqty,
			round((sum(risk_failureduration_sec)/ sum(asm_duration_sec))* 100, 2) as failureContRation
		FROM
			`rca-ipcd-lx`.risk_part
		where
			product = pdt
			and floor = flr
			and line = lne
			and teststation = tstation
			and failure_sympton = fsymp
			and riskname = rname
			and test_starttime >= starttime
			and test_starttime < stoptime
		group by
			riskcode;
WHEN 'test' THEN create
	temporary table
		level_three_tmp select
			riskCode,
			round(sum(failQty)/ @sympton_defectqty, 10) as commonnality,
			round(sum(throughputQty)/ sum(outputQty), 10) as throughput_ratio,
			(round(sum(failQty)/ @sympton_defectqty, 10)-round(sum(throughputQty)/ sum(outputQty), 10))/ sqrt(round(sum(throughputQty)/ sum(outputQty), 10)*(1-round(sum(throughputQty)/ sum(outputQty), 10))/ @sympton_defectqty) as significant,
			sum(failQty) as failqty,
			@sympton_defectqty as defectqty,
			sum(throughputqty) as throughputqty,
			sum(outputqty) as outputqty,
			round((sum(risk_failureduration_sec)/ sum(asm_duration_sec))* 100, 2) as failureContRation
		FROM
			`rca-ipcd-lx`.risk_test_station
		where
			product = pdt
			and floor = flr
			and line = lne
			and teststation = tstation
			and failure_sympton = fsymp
			and riskname = rname
			and test_starttime >= starttime
			and test_starttime < stoptime
		group by
			riskcode;
END
CASE;
else
CASE
	risktype
	WHEN 'assembly' THEN create
		temporary table
			level_three_tmp select
				riskCode,
				round(sum(failQty)/ @sympton_defectqty, 10) as commonnality,
				round(sum(throughputQty)/ sum(outputQty), 10) as throughput_ratio,
				(round(sum(failQty)/ @sympton_defectqty, 10)-round(sum(throughputQty)/ sum(outputQty), 10))/ sqrt(round(sum(throughputQty)/ sum(outputQty), 10)*(1-round(sum(throughputQty)/ sum(outputQty), 10))/ @sympton_defectqty) as significant,
				sum(failQty) as failqty,
				@sympton_defectqty as defectqty,
				sum(throughputqty) as throughputqty,
				sum(outputqty) as outputqty,
				round((sum(risk_failureduration_sec)/ sum(asm_duration_sec))* 100, 2) as failureContRation
			FROM
				`rca-ipcd-lx`.risk_assembly_station
			where
				product = pdt
				and line = lne
				and teststation = tstation
				and failure_sympton = fsymp
				and riskname = rname
				and test_starttime >= starttime
				and test_starttime < stoptime
			group by
				riskcode;
WHEN 'part' THEN create
	temporary table
		level_three_tmp select
			riskCode,
			round(sum(failQty)/ @sympton_defectqty, 10) as commonnality,
			round(sum(throughputQty)/ sum(outputQty), 10) as throughput_ratio,
			(round(sum(failQty)/ @sympton_defectqty, 10)-round(sum(throughputQty)/ sum(outputQty), 10))/ sqrt(round(sum(throughputQty)/ sum(outputQty), 10)*(1-round(sum(throughputQty)/ sum(outputQty), 10))/ @sympton_defectqty) as significant,
			sum(failQty) as failqty,
			@sympton_defectqty as defectqty,
			sum(throughputqty) as throughputqty,
			sum(outputqty) as outputqty,
			round((sum(risk_failureduration_sec)/ sum(asm_duration_sec))* 100, 2) as failureContRation
		FROM
			`rca-ipcd-lx`.risk_part
		where
			product = pdt
			and line = lne
			and teststation = tstation
			and failure_sympton = fsymp
			and riskname = rname
			and test_starttime >= starttime
			and test_starttime < stoptime
		group by
			riskcode;
WHEN 'test' THEN create
	temporary table
		level_three_tmp select
			riskCode,
			round(sum(failQty)/ @sympton_defectqty, 10) as commonnality,
			round(sum(throughputQty)/ sum(outputQty), 10) as throughput_ratio,
			(round(sum(failQty)/ @sympton_defectqty, 10)-round(sum(throughputQty)/ sum(outputQty), 10))/ sqrt(round(sum(throughputQty)/ sum(outputQty), 10)*(1-round(sum(throughputQty)/ sum(outputQty), 10))/ @sympton_defectqty) as significant,
			sum(failQty) as failqty,
			@sympton_defectqty as defectqty,
			sum(throughputqty) as throughputqty,
			sum(outputqty) as outputqty,
			round((sum(risk_failureduration_sec)/ sum(asm_duration_sec))* 100, 2) as failureContRation
		FROM
			`rca-ipcd-lx`.risk_test_station
		where
			product = pdt
			and line = lne
			and teststation = tstation
			and failure_sympton = fsymp
			and riskname = rname
			and test_starttime >= starttime
			and test_starttime < stoptime
		group by
			riskcode;
END
CASE;
end if;

SELECT
	MAX(commonnality) INTO
		@max_commonnality
	FROM
		level_three_tmp;

drop
	temporary table
		if exists level_three_with_riskpoint;

create
	temporary table
		level_three_with_riskpoint SELECT
			*,
			RISKPOINT(commonnality,
			@max_commonnality,
			round(significant, 5),
			failureContRation,
			@sympton_defectqty) AS riskpoint
		FROM
			level_three_tmp;

drop
	temporary table
		if exists level_three_with_riskrank;

create
	temporary table
		level_three_with_riskrank SELECT
			*,
			riskrank(riskpoint,
			round(commonnality, 1),
			round(significant, 1)) AS riskrank
		FROM
			level_three_with_riskpoint;

SELECT
	*
FROM
	level_three_with_riskrank
ORDER BY
	riskrank DESC
LIMIT 1;
-- drop
-- 	table
-- 		level_one_tmp;
-- 
-- drop
-- 	table
-- 		level_one_with_riskpoint;
-- 
-- drop
-- 	table
-- 		level_one_with_riskrank;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `wyj_level_two_max_rank` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `wyj_level_two_max_rank`(in pdt varchar(100),
	in flr varchar(45),
	in lne varchar(100),
	in tstation varchar(100),
	in fsymp varchar(256),
	in starttime datetime,
	in stoptime datetime)
BEGIN
	#Create aggreated level_two based on ui conditions
 drop
		temporary table
			if exists level_two_tmp ;
-- 		
-- compute risk_part values
-- 
-- 	
-- select defectqty for specific sympton
-- 	
 SELECT
	cast(sum(defectqty) as signed) INTO
		@sympton_defectqty
	FROM
		(
		SELECT
			max(defectqty) as defectqty
		FROM
			`rca-ipcd-lx`.risk_part
		where
			product = pdt
			and floor = flr
			and line = lne
			and teststation = tstation
			and failure_sympton = fsymp
			and test_starttime >= starttime
			and test_starttime < stoptime
		group by
			teststation,
			failure_sympton,
			test_starttime ) symp;
-- 		
-- 		
 if ! isnull(flr) then create
	temporary table
		level_two_tmp select
			riskname,
			round(sum(failQty)/ @sympton_defectqty, 10) as commonnality,
			round(sum(throughputQty)/ sum(outputQty), 10) as throughput_ratio,
			significant(sum(failQty),
			@sympton_defectqty,
			sum(throughputQty),
			sum(outputQty)) as significant,
			sum(failQty),
			sum(throughputQty),
			round((sum(risk_failureduration_sec)/ sum(asm_duration_sec))* 100, 2) as failureContRation
		FROM
			`rca-ipcd-lx`.risk_part
		where
			product = pdt
			and floor = flr
			and line = lne
			and teststation = teststation
			and failure_sympton = fsymp
			and test_starttime >= starttime
			and test_starttime < stoptime
		group by
			riskname;
else create
	temporary table
		level_two_tmp select
			riskname,
			round(sum(failQty)/ @sympton_defectqty, 10) as commonnality,
			round(sum(throughputQty)/ sum(outputQty), 10) as throughput_ratio,
			significant(sum(failQty),
			@sympton_defectqty,
			sum(throughputQty),
			sum(outputQty)) as significant,
			sum(failQty),
			sum(throughputQty),
			round((sum(risk_failureduration_sec)/ sum(asm_duration_sec))* 100, 2) as failureContRation
		FROM
			`rca-ipcd-lx`.risk_part
		where
			product = pdt
			and line = lne
			and teststation = teststation
			and failure_sympton = fsymp
			and test_starttime >= starttime
			and test_starttime < stoptime
		group by
			riskname;
end if;

SELECT
	MAX(commonnality) INTO
		@max_commonnality
	FROM
		level_two_tmp;

drop
	temporary table
		if exists part_level_two_with_riskpoint;

create
	temporary table
		part_level_two_with_riskpoint SELECT
			*,
			RISKPOINT(commonnality,
			@max_commonnality,
			round(significant, 5),
			failureContRation,
			@sympton_defectqty) AS riskpoint,
			'part' AS risktype
		FROM
			level_two_tmp;
-- 		
-- compute risk_assembly values
-- 
-- 	
-- select defectqty for specific sympton
-- 	
 SELECT
	sum(defectqty) INTO
		@sympton_defectqty
	FROM
		(
		SELECT
			max(defectqty) as defectqty
		FROM
			`rca-ipcd-lx`.risk_assembly_station
		where
			product = pdt
			and floor = flr
			and line = lne
			and teststation = teststation
			and failure_sympton = fsymp
			and test_starttime >= starttime
			and test_starttime < stoptime
		group by
			teststation,
			failure_sympton,
			test_starttime ) symp;
-- 		
-- 	
 drop
	temporary table
		if exists level_two_tmp ;

if ! isnull(flr) then create
	temporary table
		level_two_tmp select
			riskname,
			round(sum(failQty)/ @sympton_defectqty, 10) as commonnality,
			round(sum(throughputQty)/ sum(outputQty), 10) as throughput_ratio,
			significant(sum(failQty),
			@sympton_defectqty,
			sum(throughputQty),
			sum(outputQty)) as significant,
			sum(failQty),
			sum(throughputQty),
			round((sum(risk_failureduration_sec)/ sum(asm_duration_sec))* 100, 2) as failureContRation
		FROM
			`rca-ipcd-lx`.risk_assembly_station
		where
			product = pdt
			and floor = flr
			and line = lne
			and teststation = teststation
			and failure_sympton = fsymp
			and test_starttime >= starttime
			and test_starttime < stoptime
		group by
			riskname;
else create
	temporary table
		level_two_tmp select
			riskname,
			round(sum(failQty)/ @sympton_defectqty, 10) as commonnality,
			round(sum(throughputQty)/ sum(outputQty), 10) as throughput_ratio,
			significant(sum(failQty),
			@sympton_defectqty,
			sum(throughputQty),
			sum(outputQty)) as significant,
			sum(failQty),
			sum(throughputQty),
			round((sum(risk_failureduration_sec)/ sum(asm_duration_sec))* 100, 2) as failureContRation
		FROM
			`rca-ipcd-lx`.risk_assembly_station
		where
			product = pdt
			and line = lne
			and teststation = teststation
			and failure_sympton = fsymp
			and test_starttime >= starttime
			and test_starttime < stoptime
		group by
			riskname;
end if;

SELECT
	MAX(commonnality) INTO
		@max_commonnality
	FROM
		level_two_tmp;

drop
	temporary table
		if exists assembly_level_two_with_riskpoint;

create
	temporary table
		assembly_level_two_with_riskpoint SELECT
			*,
			RISKPOINT(commonnality,
			@max_commonnality,
			round(significant, 5),
			failureContRation,
			@sympton_defectqty) AS riskpoint,
			'assembly' AS risktype
		FROM
			level_two_tmp;
-- 		
-- compute risk_test values
-- 
-- select defectqty for specific sympton
-- 	
 SELECT
	sum(defectqty) INTO
		@sympton_defectqty
	FROM
		(
		SELECT
			max(defectqty) as defectqty
		FROM
			`rca-ipcd-lx`.risk_test_station
		where
			product = pdt
			and floor = flr
			and line = lne
			and teststation = teststation
			and failure_sympton = fsymp
			and test_starttime >= starttime
			and test_starttime < stoptime
		group by
			teststation,
			failure_sympton,
			test_starttime ) symp;
-- 		
-- 	
 drop
	temporary table
		if exists level_two_tmp ;

if ! isnull(flr) then create
	temporary table
		level_two_tmp select
			riskname,
			round(sum(failQty)/ @sympton_defectqty, 10) as commonnality,
			round(sum(throughputQty)/ sum(outputQty), 10) as throughput_ratio,
			significant(sum(failQty),
			@sympton_defectqty,
			sum(throughputQty),
			sum(outputQty)) as significant,
			sum(failQty),
			sum(throughputQty),
			round((sum(risk_failureduration_sec)/ sum(asm_duration_sec))* 100, 2) as failureContRation
		FROM
			`rca-ipcd-lx`.risk_test_station
		where
			product = pdt
			and floor = flr
			and line = lne
			and teststation = teststation
			and failure_sympton = fsymp
			and test_starttime >= starttime
			and test_starttime < stoptime
		group by
			riskname;
else create
	temporary table
		level_two_tmp select
			riskname,
			round(sum(failQty)/ @sympton_defectqty, 10) as commonnality,
			round(sum(throughputQty)/ sum(outputQty), 10) as throughput_ratio,
			significant(sum(failQty),
			@sympton_defectqty,
			sum(throughputQty),
			sum(outputQty)) as significant,
			sum(failQty),
			sum(throughputQty),
			round((sum(risk_failureduration_sec)/ sum(asm_duration_sec))* 100, 2) as failureContRation
		FROM
			`rca-ipcd-lx`.risk_test_station
		where
			product = pdt
			and line = lne
			and teststation = teststation
			and failure_sympton = fsymp
			and test_starttime >= starttime
			and test_starttime < stoptime
		group by
			riskname;
end if;

SELECT
	MAX(commonnality) INTO
		@max_commonnality
	FROM
		level_two_tmp;

drop
	temporary table
		if exists test_level_two_with_riskpoint;

create
	temporary table
		test_level_two_with_riskpoint SELECT
			*,
			RISKPOINT(commonnality,
			@max_commonnality,
			round(significant, 5),
			failureContRation,
			@sympton_defectqty) AS riskpoint,
			'test' AS risktype
		FROM
			level_two_tmp;
-- 
-- SELECT
-- 	*
-- FROM
-- 	level_two_with_riskpoint;
-- Compute risk rank 
 drop
	temporary table
		if exists level_two_part_with_riskrank;

create
	temporary table
		level_two_part_with_riskrank SELECT
			*,
			riskrank(riskpoint,
			round(commonnality, 1),
			round(significant, 1)) AS riskrank
		FROM
			part_level_two_with_riskpoint
		ORDER BY
			riskrank DESC
		LIMIT 1;

alter table
	level_two_part_with_riskrank modify risktype varchar(50);

INSERT
	INTO
		level_two_part_with_riskrank SELECT
			*,
			riskrank(riskpoint,
			round(commonnality, 1),
			round(significant, 1)) AS riskrank
		FROM
			assembly_level_two_with_riskpoint
		ORDER BY
			riskrank DESC
		LIMIT 1;

INSERT
	INTO
		level_two_part_with_riskrank SELECT
			*,
			riskrank(riskpoint,
			round(commonnality, 1),
			round(significant, 1)) AS riskrank
		FROM
			test_level_two_with_riskpoint
		ORDER BY
			riskrank DESC
		LIMIT 1;

SELECT
	*
FROM
	level_two_part_with_riskrank;
#drop table level_three_tmp;
#drop table level_three_with_riskpoint;
#drop table level_three_with_riskrank;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2019-02-26 16:46:51
