CREATE TABLE `files` (
      `id` bigint(20) NOT NULL AUTO_INCREMENT,
      `original_filename` varchar(255) DEFAULT NULL,
      `is_in_repo` tinyint(1) DEFAULT '0',
      `type` varchar(255) DEFAULT NULL,
      `sha1` varchar(255) DEFAULT NULL,
      `file_size` bigint(20) DEFAULT NULL,
      `original_mtime` datetime DEFAULT NULL,
      `original_ctime` datetime DEFAULT NULL,
      `inserted_at` datetime DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (`id`),
      UNIQUE KEY `sha1` (`sha1`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `metadata` (
      `id` bigint(20) NOT NULL AUTO_INCREMENT,
      `file_id` bigint(20) NOT NULL,
      `scanner` varchar(255) DEFAULT NULL,
      `tagname` varchar(255) DEFAULT NULL,
      `tagvalue` varchar(255) DEFAULT NULL,
      PRIMARY KEY (`id`),
      KEY `file_id` (`file_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


