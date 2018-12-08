/* Table creation statements for PostgreSQL */

CREATE TABLE LOCATION (
  ID varchar(255),
  X float,
  Y float,
  Z float,
  PRIMARY KEY (id)
) ;

CREATE TABLE INFRASTRUCTURE_TYPE (
  ID varchar(255),
  DESCRIPTION varchar(255) ,
  NAME varchar(255),
  PRIMARY KEY (id)
) ;

CREATE TABLE INFRASTRUCTURE (
  NAME varchar(255) ,
  INFRASTRUCTURE_TYPE_ID varchar(255),
  ID varchar(255),
  FLOOR integer,
  PRIMARY KEY (id)
) ;

CREATE TABLE INFRASTRUCTURE_LOCATION (
  LOCATION_ID varchar(255),
  INFRASTRUCTURE_ID varchar(255)
) ;

CREATE TABLE PLATFORM_TYPE (
  ID varchar(255),
  DESCRIPTION varchar(255),
  NAME varchar(255),
  PRIMARY KEY (id)
) ;

CREATE TABLE USERS (
  EMAIL varchar(255)  UNIQUE,
  GOOGLE_AUTH_TOKEN varchar(255) ,
  NAME varchar(255) ,
  ID varchar(255),
  PRIMARY KEY (id)
 ) ;

CREATE TABLE USER_GROUP (
  ID varchar(255),
  DESCRIPTION varchar(255) ,
  NAME varchar(255),
  PRIMARY KEY (id)
) ;

CREATE TABLE USER_GROUP_MEMBERSHIP (
  USER_ID varchar(255),
  USER_GROUP_ID varchar(255)
) ;

CREATE TABLE PLATFORM (
  ID varchar(255),
  NAME varchar(255) ,
  USER_ID varchar(255) ,
  PLATFORM_TYPE_ID varchar(255) ,
  HASHED_MAC varchar(255),
  PRIMARY KEY (id)
) ;

CREATE TABLE SENSOR_TYPE (
  ID varchar(255),
  DESCRIPTION varchar(255) ,
  MOBILITY varchar(255) ,
  NAME varchar(255) ,
  CAPTURE_FUNCTIONALITY varchar(255) ,
  PAYLOAD_SCHEMA varchar(255),
  PRIMARY KEY (id)
) ;

CREATE TABLE SENSOR (
  ID varchar(255),
  NAME varchar(255) ,
  INFRASTRUCTURE_ID varchar(255) ,
  USER_ID varchar(255) ,
  SENSOR_TYPE_ID varchar(255) ,
  SENSOR_CONFIG varchar(255),
  PRIMARY KEY (id)
) ;

CREATE TABLE COVERAGE_INFRASTRUCTURE (
  SENSOR_ID varchar(255),
  INFRASTRUCTURE_ID varchar(255)
) ;

CREATE TABLE OBSERVATION (
  id varchar(255),
  payload json ,
  timeStamp timestamp,
  sensor_id varchar(255),
  PRIMARY KEY (id)
) ;

CREATE TABLE SEMANTIC_OBSERVATION_TYPE (
  ID varchar(255),
  DESCRIPTION varchar(255) ,
  NAME varchar(255),
  PRIMARY KEY (id)
) ;

CREATE TABLE VIRTUAL_SENSOR_TYPE (
  ID varchar(255),
  NAME varchar(255) ,
  DESCRIPTION varchar(255) ,
  INPUT_TYPE_ID varchar(255) ,
  SEMANTIC_OBSERVATION_TYPE_ID varchar(255),
  PRIMARY KEY (id)
) ;

CREATE TABLE VIRTUAL_SENSOR (
  ID varchar(255),
  NAME varchar(255) ,
  DESCRIPTION varchar(255) ,
  LANGUAGE varchar(255) ,
  PROJECT_NAME varchar(255) ,
  TYPE_ID varchar(255),
  PRIMARY KEY (id)
) ;

CREATE TABLE SEMANTIC_OBSERVATION (
  id varchar(255),
  semantic_entity_id varchar(255),
  payload json ,
  timeStamp timestamp,
  virtual_sensor_id varchar(255) ,
  type_id varchar(255),
  PRIMARY KEY (id)
) ;

CREATE INDEX obs_timestamp_idx ON OBSERVATION(timeStamp);
CREATE INDEX sobs_timestamp_idx ON SEMANTIC_OBSERVATION(timeStamp);
