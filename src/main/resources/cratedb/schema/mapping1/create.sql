


CREATE TABLE IF NOT EXISTS LOCATION (
  ID string NOT NULL,
  X float NOT NULL,
  Y float NOT NULL,
  Z float NOT NULL,
  PRIMARY KEY (ID)
) CLUSTERED INTO 1 shards WITH ( number_of_replicas = 0 );

CREATE TABLE INFRASTRUCTURE_LOCATION (
  LOCATION_ID string NOT NULL,
  INFRASTRUCTURE_ID string NOT NULL,
   PRIMARY KEY(LOCATION_ID, INFRASTRUCTURE_ID)
) CLUSTERED INTO 1 shards WITH ( number_of_replicas = 0 );

CREATE TABLE IF NOT EXISTS INFRASTRUCTURE_TYPE (
  ID string NOT NULL,
  DESCRIPTION string ,
  NAME string,
  PRIMARY KEY (ID)
) CLUSTERED INTO 1 shards WITH ( number_of_replicas = 0 );

CREATE TABLE IF NOT EXISTS INFRASTRUCTURE (
  NAME string ,
  INFRASTRUCTURE_TYPE_ID string ,
  ID string NOT NULL,
  FLOOR integer ,
  PRIMARY KEY (ID)
) CLUSTERED INTO 1 shards WITH ( number_of_replicas = 0 );

CREATE TABLE IF NOT EXISTS PLATFORM_TYPE (
  ID string NOT NULL,
  DESCRIPTION string ,
  NAME string,
  PRIMARY KEY (ID)
) CLUSTERED INTO 1 shards WITH ( number_of_replicas = 0 );

CREATE TABLE IF NOT EXISTS USERS (
  EMAIL string,
  GOOGLE_AUTH_TOKEN string ,
  NAME string ,
  ID string NOT NULL,
  PRIMARY KEY (ID)
 ) CLUSTERED INTO 1 shards WITH ( number_of_replicas = 0);

CREATE TABLE IF NOT EXISTS USER_GROUP (
  ID string NOT NULL,
  DESCRIPTION string ,
  NAME string ,
  PRIMARY KEY (ID)
) CLUSTERED INTO 1 shards WITH ( number_of_replicas = 0 );

CREATE TABLE IF NOT EXISTS USER_GROUP_MEMBERSHIP (
  USER_ID string NOT NULL,
  USER_GROUP_ID string NOT NULL,
  PRIMARY KEY (USER_GROUP_ID, USER_ID)
) CLUSTERED INTO 1 shards WITH ( number_of_replicas = 0 );

CREATE TABLE IF NOT EXISTS PLATFORM (
  ID string NOT NULL,
  NAME string ,
  USER_ID string ,
  PLATFORM_TYPE_ID string ,
  PRIMARY KEY (ID)
) CLUSTERED INTO 1 shards WITH ( number_of_replicas = 0 );

CREATE TABLE IF NOT EXISTS SENSOR_TYPE (
  ID string NOT NULL,
  DESCRIPTION string ,
  MOBILITY string ,
  NAME string ,
  CAPTURE_FUNCTIONALITY string ,
  PAYLOAD_SCHEMA string,
  PRIMARY KEY (ID)
) CLUSTERED INTO 1 shards WITH ( number_of_replicas = 0 );

CREATE TABLE IF NOT EXISTS SENSOR (
  ID string NOT NULL,
  NAME string ,
  INFRASTRUCTURE_ID string ,
  USER_ID string ,
  SENSOR_TYPE_ID string ,
  SENSOR_CONFIG string,
  PRIMARY KEY (ID)
) CLUSTERED INTO 1 shards WITH ( number_of_replicas = 0 );

CREATE TABLE IF NOT EXISTS COVERAGE_INFRASTRUCTURE (
  SENSOR_ID string NOT NULL,
  INFRASTRUCTURE_ID string NOT NULL,
  PRIMARY KEY (INFRASTRUCTURE_ID, SENSOR_ID)
) CLUSTERED INTO 1 shards WITH ( number_of_replicas = 0 );

CREATE TABLE IF NOT EXISTS OBSERVATION (
  id string NOT NULL,
  payload string ,
  timeStamp timestamp NOT NULL,
  sensor_id string ,
  PRIMARY KEY (id)
) CLUSTERED INTO 1 shards WITH ( number_of_replicas = 0 );

CREATE TABLE IF NOT EXISTS SEMANTIC_OBSERVATION_TYPE (
  ID string NOT NULL,
  DESCRIPTION string ,
  NAME string,
  PRIMARY KEY (ID)
) ;

CREATE TABLE IF NOT EXISTS VIRTUAL_SENSOR_TYPE (
  ID string NOT NULL,
  NAME string,
  DESCRIPTION string,
  INPUT_TYPE_ID string,
  SEMANTIC_OBSERVATION_TYPE_ID string,
  PRIMARY KEY (ID)
) ;

CREATE TABLE IF NOT EXISTS VIRTUAL_SENSOR (
  ID string NOT NULL,
  NAME string,
  DESCRIPTION string,
  LANGUAGE string,
  PROJECT_NAME string,
  TYPE_ID string,
  PRIMARY KEY (ID)
) ;

CREATE TABLE IF NOT EXISTS SEMANTIC_OBSERVATION (
  id string NOT NULL,
  semantic_entity_id string NOT NULL,
  payload string,
  timeStamp timestamp NOT NULL,
  virtual_sensor_id string,
  type_id string,
  PRIMARY KEY (id)
) ;

