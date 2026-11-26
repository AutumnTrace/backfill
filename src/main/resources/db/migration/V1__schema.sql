-- One row per import of one source file. The checkpoint and every counter move only inside the
-- chunk transaction (ChunkTransaction.java), together with the effects they describe.
create table imports (
    id                bigint generated always as identity primary key,
    source_path       text        not null,
    source_sha256     char(64)    not null,
    source_bytes      bigint      not null,
    chunk_size        int         not null check (chunk_size > 0),
    state             text        not null check (state in ('running', 'completed')),
    checkpoint_offset bigint      not null default 0, -- first byte not yet processed
    checkpoint_line   bigint      not null default 0, -- lines fully processed
    chunks_committed  int         not null default 0,
    accepted          bigint      not null default 0,
    duplicates        bigint      not null default 0,
    conflicts         bigint      not null default 0,
    rejected          bigint      not null default 0,
    started_at        timestamptz not null default now(),
    updated_at        timestamptz not null default now(),
    completed_at      timestamptz,
    last_worker       text
);

-- Every scan that changed stock, once. The primary key is what makes a replay idempotent.
create table applied_events (
    event_id       uuid     primary key,
    payload_sha256 char(64) not null,
    warehouse      text     not null,
    sku            text     not null,
    delta          int      not null,
    source_device  text     not null,
    source_seq     bigint   not null,
    import_id      bigint   not null references imports,
    line_no        bigint   not null,
    byte_offset    bigint   not null
);

-- The projection.
create table stock (
    warehouse text   not null,
    sku       text   not null,
    quantity  bigint not null,
    primary key (warehouse, sku)
);

-- Lines that are not a valid scan. Keyed by position, so a replayed chunk cannot add a row twice.
create table rejects (
    import_id   bigint not null references imports,
    line_no     bigint not null,
    byte_offset bigint not null,
    reason      text   not null,
    raw         text   not null,
    primary key (import_id, line_no)
);

-- An event id seen again with different content. Both payloads stay reachable: the first by its
-- import, line and offset (it is in applied_events), the second verbatim.
create table quarantine (
    import_id            bigint   not null references imports,
    line_no              bigint   not null,
    byte_offset          bigint   not null,
    event_id             uuid     not null,
    payload_sha256       char(64) not null,
    payload              text     not null,
    first_import_id      bigint   not null references imports,
    first_line_no        bigint   not null,
    first_byte_offset    bigint   not null,
    first_payload_sha256 char(64) not null,
    primary key (import_id, line_no)
);
