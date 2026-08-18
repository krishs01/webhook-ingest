-- Replace the plain index with a UNIQUE index to enforce deduplication at the
-- database level. This closes the TOCTOU race window in check-then-insert.
DROP INDEX IF EXISTS idx_events_event_id;
CREATE UNIQUE INDEX IF NOT EXISTS idx_events_event_id ON events (event_id);
