CREATE TABLE `ticker` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `exchange` varchar(32) NOT NULL,
  `market` varchar(32) NOT NULL,
  `price` decimal(64,8) NOT NULL,
  `timestamp` timestamp(3) NOT NULL,
  `created_at` timestamp(3) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `trade` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `exchange` varchar(32) NOT NULL,
  `market` varchar(32) NOT NULL,
  `trade_id` bigint unsigned NULL,
  `side` varchar(8) NULL,
  `size` decimal(64,8) NOT NULL,
  `price` decimal(64,8) NOT NULL,
  `timestamp` timestamp(3) NOT NULL,
  `created_at` timestamp(3) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;