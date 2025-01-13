-- MySQL Workbench Forward Engineering

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

-- -----------------------------------------------------
-- Schema air_quality
-- -----------------------------------------------------
DROP SCHEMA IF EXISTS `air_quality` ;

-- -----------------------------------------------------
-- Schema air_quality
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `air_quality` DEFAULT CHARACTER SET utf8 ;
USE `air_quality` ;

-- -----------------------------------------------------
-- Table `air_quality`.`constituencies`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `air_quality`.`constituencies` ;

CREATE TABLE IF NOT EXISTS `air_quality`.`constituencies` (
  `constituency_id` INT NOT NULL,
  `constituency_name` VARCHAR(45) NULL,
  PRIMARY KEY (`constituency_id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `air_quality`.`stations`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `air_quality`.`stations` ;

CREATE TABLE IF NOT EXISTS `air_quality`.`stations` (
  `site_id` INT NOT NULL,
  `constituency_id` INT NOT NULL,
  `station_name` VARCHAR(45) NULL,
  `longitude` FLOAT NULL,
  `latitude` FLOAT NULL,
  PRIMARY KEY (`site_id`),
  INDEX `fk_stations_constituencies_idx` (`constituency_id` ASC))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `air_quality`.`readings`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `air_quality`.`readings` ;

CREATE TABLE IF NOT EXISTS `air_quality`.`readings` (
  `reading_id` INT NOT NULL,
  `site_id` INT NOT NULL,
  `date_time` DATETIME NULL,
  `NOx` FLOAT NULL,
  `NO2` FLOAT NULL,
  `NO` FLOAT NULL,
  `PM10` FLOAT NULL,
  `O3` FLOAT NULL,
  `temperature` FLOAT NULL,
  `NVPM10` FLOAT NULL,
  `VPM10` FLOAT NULL,
  `NVPM2_5` FLOAT NULL,
  `VPM2_5` FLOAT NULL,
  `PM2_5` FLOAT NULL,
  `CO` FLOAT NULL,
  `RH` FLOAT NULL,
  `pressure` FLOAT NULL,
  `SO2` FLOAT NULL,
  PRIMARY KEY (`reading_id`),
  INDEX `fk_readings_stations1_idx` (`site_id` ASC))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `air_quality`.`measures`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `air_quality`.`measures` ;

CREATE TABLE IF NOT EXISTS `air_quality`.`measures` (
  `measure` VARCHAR(45) NOT NULL,
  `description` VARCHAR(256) NULL,
  `unit` VARCHAR(45) NULL,
  PRIMARY KEY (`measure`))
ENGINE = InnoDB;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
