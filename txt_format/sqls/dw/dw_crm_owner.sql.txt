CREATE TABLE dw_crm_owner (
    owner_territory_id  INTEGER,
    owner_name          VARCHAR(255),
    owner_type          VARCHAR(100),
    owner_team          VARCHAR(100),
    owner_manager       VARCHAR(255),
    partition_date      DATE,
    primary key (owner_territory_id, partition_date)
);
