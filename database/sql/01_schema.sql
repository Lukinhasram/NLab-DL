-- =============================================================================
-- nLab Core Relational Schema (Base DDL for ETL Pipeline)
-- =============================================================================
-- This file creates the 5 core wiki tables derived from the upstream nLab
-- (Instiki/Rails) MariaDB schema, excluding legacy nForum/session tables.
--
-- After running the ETL scripts (01..04), run 05_enrich_model.py to:
--   - Create and populate `users` and `categories`
--   - Add `wiki_files.file_type`
--   - Add denormalized revision metrics on `pages`
--   - Resolve polymorphic references into FKs on `wiki_references`
--   - Migrate `revisions.author` -> `revisions.author_id`
--   - Enforce all 8 FOREIGN KEY constraints
-- =============================================================================

CREATE TABLE IF NOT EXISTS `webs` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `created_at` DATETIME NOT NULL,
  `updated_at` DATETIME NOT NULL,
  `name` VARCHAR(60) NOT NULL DEFAULT '',
  `address` VARCHAR(60) NOT NULL DEFAULT '',
  `password` VARCHAR(60) DEFAULT NULL,
  `additional_style` TEXT DEFAULT NULL,
  `allow_uploads` INT DEFAULT 1,
  `published` INT DEFAULT 0,
  `count_pages` INT DEFAULT 0,
  `markup` VARCHAR(50) DEFAULT 'markdownMML',
  `color` VARCHAR(6) DEFAULT '008B26',
  `max_upload_size` INT DEFAULT 100,
  `safe_mode` INT DEFAULT 0,
  `brackets_only` INT DEFAULT 0,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `pages` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `created_at` DATETIME NOT NULL,
  `updated_at` DATETIME DEFAULT NULL,
  `web_id` INT NOT NULL DEFAULT 0,
  `locked_by` VARCHAR(60) DEFAULT NULL,
  `name` VARCHAR(255) DEFAULT NULL,
  `locked_at` DATETIME DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_pages_web_id` (`web_id`),
  KEY `idx_pages_name` (`name`),
  KEY `idx_pages_web_name` (`web_id`, `name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `revisions` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `created_at` DATETIME NOT NULL,
  `updated_at` DATETIME NOT NULL,
  `revised_at` DATETIME NOT NULL,
  `page_id` INT NOT NULL DEFAULT 0,
  `content` LONGTEXT DEFAULT NULL,
  `author` VARCHAR(60) DEFAULT NULL,
  `ip` VARCHAR(60) DEFAULT NULL,
  `web_id` INT NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`),
  KEY `idx_revisions_page_id` (`page_id`),
  KEY `idx_revisions_created_at` (`created_at`),
  KEY `idx_revisions_revised_at` (`revised_at`),
  KEY `idx_revisions_author` (`author`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `wiki_files` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `created_at` DATETIME NOT NULL,
  `updated_at` DATETIME NOT NULL,
  `web_id` INT NOT NULL DEFAULT 0,
  `file_name` VARCHAR(255) DEFAULT NULL,
  `description` VARCHAR(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_wiki_files_web_id` (`web_id`),
  KEY `idx_wiki_files_file_name` (`file_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `wiki_references` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `created_at` DATETIME NOT NULL,
  `updated_at` DATETIME NOT NULL,
  `page_id` INT NOT NULL DEFAULT 0,
  `referenced_name` VARCHAR(255) NOT NULL DEFAULT '',
  `link_type` VARCHAR(1) NOT NULL DEFAULT '',
  PRIMARY KEY (`id`),
  KEY `idx_wr_page_id` (`page_id`),
  KEY `idx_wr_referenced_name` (`referenced_name`),
  KEY `idx_wr_link_type` (`link_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
