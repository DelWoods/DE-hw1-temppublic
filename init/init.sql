-- target schema for hw-1: messages loaded by the notebook etl
CREATE SCHEMA IF NOT EXISTS toxicity;

CREATE TABLE IF NOT EXISTS toxicity.messages (
    raw_text_id      BIGINT PRIMARY KEY,
    dataset_id       SMALLINT     NOT NULL,
    source_platform  TEXT,
    is_verified      BOOLEAN,
    text_raw         TEXT         NOT NULL,
    is_toxic         SMALLINT     CHECK (is_toxic IN (0, 1)),   -- NULL = unlabelled
    toxicity_type    TEXT,
    text_length      INTEGER,
    count_spp        INTEGER,
    count_rpp        INTEGER,
    count_profanity  INTEGER,
    has_emoji        BOOLEAN,
    has_emoticon     BOOLEAN,
    has_capslock     BOOLEAN,
    is_all_lower     BOOLEAN,
    is_bad_word_incl BOOLEAN,
    has_url          BOOLEAN,
    has_number       BOOLEAN,
    has_mention      BOOLEAN,
    has_hashtag      BOOLEAN,
    loaded_at        TIMESTAMPTZ  NOT NULL DEFAULT now()
);
