PRAGMA encoding = 'UTF-16le';
PRAGMA foreign_keys = ON;


CREATE TABLE annotation_key (      -- A table to list annotation keys
  id INTEGER PRIMARY KEY,          -- Primary key for foreign keys
  name TEXT NOT NULL,              -- annotation key name
  parent_id INTEGER,               -- parent annotation key
  FOREIGN KEY (parent_id) REFERENCES annotation_key(id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE location (            -- A table to list location
  id INTEGER PRIMARY KEY,          -- Primary key for foreign keys
  UNIQUE(id)
);

CREATE TABLE location_annotation ( -- A table to list annotation for location
  id INTEGER PRIMARY KEY,          -- Primary key for foreign keys
  location_id INTEGER NOT NULL,    -- location this refers to
  key_id INTEGER NOT NULL,         -- annotation key this refers to
  value TEXT NOT NULL,             -- annotation value
  UNIQUE(location_id, key_id),
  FOREIGN KEY (location_id) REFERENCES location(id) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY (key_id) REFERENCES annotation_key(id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE analysis (            -- A table to list analysis runs
  id INTEGER PRIMARY KEY,          -- Primary key for foreign keys
  name TEXT NOT NULL,              -- Identifier of the analysis
  doc_uri TEXT NOT NULL            -- doc file URI in storage
);

CREATE TABLE storage_type (        -- A table to list data types
  id INTEGER PRIMARY KEY,          -- Primary key for foreign keys
  name TEXT NOT NULL,              -- name of the type
  format TEXT NOT NULL             -- file format
);
INSERT INTO storage_type (name, format) VALUES ('Array', 'Array');
INSERT INTO storage_type (name, format) VALUES ('Table', 'Table');
INSERT INTO storage_type (name, format) VALUES ('Value', 'Value');
INSERT INTO storage_type (name, format) VALUES ('Label', 'Label');

CREATE TABLE data (                -- A table to list data
  id INTEGER PRIMARY KEY,          -- Primary key for foreign keys
  location_id INTEGER NOT NULL,    -- location this refers to
  type_id  INTEGER NOT NULL,       -- data type this refers to
  analysis_id INTEGER,             -- data analysis this refers to
  metadata_uri TEXT,               -- metadata doc file
  uri TEXT NOT NULL,               -- file uri in the storage
  FOREIGN KEY (location_id) REFERENCES location(id) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY (type_id) REFERENCES storage_type(id) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY (analysis_id) REFERENCES analysis(id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE data_annotation (     -- A table to list annotation for data
  id INTEGER PRIMARY KEY,          -- Primary key for foreign keys
  data_id INTEGER NOT NULL,        -- data this refers to
  key_id INTEGER NOT NULL,         -- annotation key this refers to
  value TEXT NOT NULL,             -- annotation value
  FOREIGN KEY (data_id) REFERENCES data(id) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY (key_id) REFERENCES annotation_key(id) ON DELETE CASCADE ON UPDATE CASCADE
);
