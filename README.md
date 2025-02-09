# parquetFilesAutomation

# Table must be explicitly created in mysql db.

CREATE TABLE `insights_comparison` (
  `id` int NOT NULL AUTO_INCREMENT,
  `folder_name` varchar(255) NOT NULL,
  `file1` varchar(255) NOT NULL,
  `file2` varchar(255) NOT NULL,
  `edited_date` varchar(50) NOT NULL,
  `data` json NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=14419 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
