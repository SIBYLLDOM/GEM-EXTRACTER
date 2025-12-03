-- ===========================================
--   DATABASE INITIALIZATION FOR TENDER BOT
--   gem_extractor
-- ===========================================

CREATE DATABASE IF NOT EXISTS gem_extractor
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE gem_extractor;


-- =====================================================
--  TABLE 1: bids
--  (Source-of-truth populated by the Scraper)
-- =====================================================

CREATE TABLE IF NOT EXISTS bids (
    id INT AUTO_INCREMENT PRIMARY KEY,

    -- scraped fields
    page INT NULL,
    bid_number VARCHAR(128) NOT NULL,
    detail_url TEXT NOT NULL,
    items TEXT NULL,
    quantity VARCHAR(64) NULL,
    department VARCHAR(255) NULL,
    start_date DATETIME NULL,
    end_date DATETIME NULL,

    -- processing metadata
    todayscan TINYINT(1) NOT NULL DEFAULT 0,  
      -- 0 → Not processed yet
      -- 1 → Fully processed (PDF → JSON complete)

    status ENUM('new','queued','processing','done','failed') 
        NOT NULL DEFAULT 'new',

    locked_by VARCHAR(128) NULL,  -- worker id that claimed this row
    processing_started_ts DATETIME NULL,
    attempts INT NOT NULL DEFAULT 0,
    last_error TEXT NULL,

    -- artifact locations (local or S3/MinIO or absolute paths)
    pdf_uri VARCHAR(1024) NULL,
    json_uri VARCHAR(1024) NULL,
    parse_confidence FLOAT NULL,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
              ON UPDATE CURRENT_TIMESTAMP,

    UNIQUE KEY uniq_bid_number (bid_number),
    INDEX idx_todayscan_status (todayscan, status, attempts)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci;



-- =====================================================
--  TABLE 2: final_bids
--  (Structured canonical extracted fields)
-- =====================================================

CREATE TABLE IF NOT EXISTS final_bids (
    id INT AUTO_INCREMENT PRIMARY KEY,

    bid_id INT NOT NULL,
    bid_number VARCHAR(128) NOT NULL,

    -- extracted canonical fields
    buyer VARCHAR(255) NULL,
    item_description TEXT NULL,
    total_quantity VARCHAR(64) NULL,
    unit VARCHAR(64) NULL,
    emd_amount VARCHAR(64) NULL,
    epbg_required TINYINT(1) NULL,
    technical_specs JSON NULL,

    raw_json_uri VARCHAR(1024) NULL,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
              ON UPDATE CURRENT_TIMESTAMP,

    FOREIGN KEY (bid_id) REFERENCES bids(id)
        ON DELETE CASCADE,

    UNIQUE KEY uniq_final_bid_number (bid_number)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci;
