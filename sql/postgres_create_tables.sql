CREATE TABLE tbl_reconcile_summary (
  reconcile_id text PRIMARY KEY,
  reconcile_type text NOT NULL,
  release_version text,
  src_project_id text,
  src_instance_name text,
  src_database text NOT NULL,
  src_schema text,
  src_table text NOT NULL,
  target_dataset text NOT NULL,
  target_table text NOT NULL,
  schema_match text,
  total_target_record INTEGER,
  total_source_record INTEGER,
  match_record INTEGER DEFAULT 0,
  mismatch_record INTEGER DEFAULT 0,
  missing_record INTEGER DEFAULT 0,
  duplicate_record INTEGER DEFAULT 0,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  primary_key text,
  condition_key text,
  condition_start text,
  condition_end text,
  record_start TIMESTAMP,
  record_end TIMESTAMP,
  reconcile_status text,
  created_at TIMESTAMP,
  created_by text DEFAULT 'system',
  updated_at TIMESTAMP,
  updated_by text DEFAULT 'system'
);

CREATE TABLE tbl_mismatch_record
(
  id text PRIMARY KEY,
  reconcile_id text,
  FOREIGN KEY (reconcile_id) REFERENCES tbl_reconcile_summary (reconcile_id),
  reason text,
  record_id text,
  fix_status text,
  created_at TIMESTAMP ,
  created_by text default 'system',
  updated_at TIMESTAMP,
  updated_by text default 'system'
);

CREATE TABLE tbl_missing_record
(
  id text PRIMARY KEY,
  reconcile_id text,
  FOREIGN KEY (reconcile_id) REFERENCES tbl_reconcile_summary (reconcile_id),
  record_id text,
  reason text,
  fix_status text,
  created_at TIMESTAMP ,
  created_by text default 'system',
  updated_at TIMESTAMP,
  updated_by text default 'system'
);

CREATE TABLE tbl_duplicate_record (
  id text PRIMARY KEY,
  reconcile_id text,
  FOREIGN KEY (reconcile_id) REFERENCES tbl_reconcile_summary (reconcile_id),
  record_id text,
  duplicate_num INTEGER,
  fix_status text,
  created_at TIMESTAMP,
  created_by text DEFAULT 'system',
  updated_at TIMESTAMP,
  updated_by text DEFAULT 'system'
);