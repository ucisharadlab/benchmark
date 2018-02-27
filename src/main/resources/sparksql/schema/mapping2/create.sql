
CREATE TABLE LOCATION (
  ID varchar(255) ,
  X float ,
  Y float ,
  Z float 
) ;

CREATE TABLE INFRASTRUCTURE_TYPE (
  ID varchar(255) ,
  DESCRIPTION varchar(255) ,
  NAME varchar(255) 
) ;

CREATE TABLE INFRASTRUCTURE (
  NAME varchar(255) ,
  INFRASTRUCTURE_TYPE_ID varchar(255) ,
  ID varchar(255) ,
  FLOOR integer 
) ;

CREATE TABLE INFRASTRUCTURE_LOCATION (
  LOCATION_ID varchar(255) ,
  INFRASTRUCTURE_ID varchar(255) 
) ;

CREATE TABLE PLATFORM_TYPE (
  ID varchar(255) ,
  DESCRIPTION varchar(255) ,
  NAME varchar(255)
) ;

CREATE TABLE USERS (
  EMAIL varchar(255),
  GOOGLE_AUTH_TOKEN varchar(255) ,
  NAME varchar(255) ,
  ID varchar(255)
 ) ;

CREATE TABLE USER_GROUP (
  ID varchar(255) ,
  DESCRIPTION varchar(255) ,
  NAME varchar(255)
) ;

CREATE TABLE USER_GROUP_MEMBERSHIP (
  USER_ID varchar(255) ,
  USER_GROUP_ID varchar(255)
) ;

CREATE TABLE PLATFORM (
  ID varchar(255) ,
  NAME varchar(255) ,
  USER_ID varchar(255) ,
  PLATFORM_TYPE_ID varchar(255) ,
  HASHED_MAC varchar(255)
) ;

CREATE TABLE SENSOR_TYPE (
  ID varchar(255) ,
  DESCRIPTION varchar(255) ,
  MOBILITY varchar(255) ,
  NAME varchar(255) ,
  CAPTURE_FUNCTIONALITY varchar(255) ,
  PAYLOAD_SCHEMA varchar(255)
) ;

CREATE TABLE SENSOR (
  ID varchar(255) ,
  NAME varchar(255) ,
  INFRASTRUCTURE_ID varchar(255) ,
  USER_ID varchar(255) ,
  SENSOR_TYPE_ID varchar(255) ,
  SENSOR_CONFIG varchar(255)
) ;

CREATE TABLE COVERAGE_INFRASTRUCTURE (
  SENSOR_ID varchar(255) ,
  INFRASTRUCTURE_ID varchar(255) 
) ;

CREATE TABLE WeMoObservation (
  id varchar(255) ,
  currentMilliWatts integer ,
  onTodaySeconds integer ,
  timeStamp timestamp ,
  sensor_id varchar(255)
) ;

CREATE TABLE WiFiAPObservation (
  id varchar(255) ,
  clientId varchar(255) ,
  timeStamp timestamp ,
  sensor_id varchar(255) 
) ;

CREATE TABLE ThermometerObservation (
  id varchar(255) ,
  temperature integer ,
  timeStamp timestamp ,
  sensor_id varchar(255) 
) ;

CREATE TABLE SEMANTIC_OBSERVATION_TYPE (
  ID varchar(255) ,
  DESCRIPTION varchar(255) ,
  NAME varchar(255) 
) ;

CREATE TABLE VIRTUAL_SENSOR_TYPE (
  ID varchar(255) ,
  NAME varchar(255) ,
  DESCRIPTION varchar(255) ,
  INPUT_TYPE_ID varchar(255) ,
  SEMANTIC_OBSERVATION_TYPE_ID varchar(255)
) ;

CREATE TABLE VIRTUAL_SENSOR (
  ID varchar(255) ,
  NAME varchar(255) ,
  DESCRIPTION varchar(255) ,
  LANGUAGE varchar(255) ,
  PROJECT_NAME varchar(255) ,
  TYPE_ID varchar(255)
) ;

CREATE TABLE OCCUPANCY (
  id varchar(255) ,
  semantic_entity_id varchar(255) ,
  occupancy integer ,
  timeStamp timestamp ,
  virtual_sensor_id varchar(255)
) ;

CREATE TABLE PRESENCE (
  id varchar(255) ,
  semantic_entity_id varchar(255) ,
  location varchar(255) ,
  timeStamp timestamp ,
  virtual_sensor_id varchar(255)
) ;

