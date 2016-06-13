DROP  TABLE IF EXISTS interests;

CREATE TABLE `interests` (
  `id`                 INT(11)   NOT NULL AUTO_INCREMENT,
  `name`               VARCHAR(255)       DEFAULT NULL,
  `category`           VARCHAR(255)       DEFAULT NULL,
  `status`             VARCHAR(10)       DEFAULT NULL,
  `groupname`         VARCHAR(10)       DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE = InnoDB;

