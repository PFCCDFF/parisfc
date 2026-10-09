-- 2026-10-09_interface_performance_v2.sql
-- Interface Performance v2 (branche beta) : bien-être, RPE de séance,
-- contenu des séances d'entraînement et fichiers joints.
-- Additif uniquement : aucune table existante modifiée ou supprimée.
-- À exécuter une fois dans l'éditeur SQL Supabase (même projet que la prod).

-- ── Bien-être quotidien (questionnaire type Hooper / McLean, 1 = mauvais … 5 = très bon)
create table if not exists wellness_quotidien (
    id              bigserial primary key,
    joueuse         text        not null,
    date            date        not null,
    sommeil_qualite smallint    check (sommeil_qualite between 1 and 5),
    sommeil_heures  numeric(4,1) check (sommeil_heures between 0 and 24),
    fatigue         smallint    check (fatigue between 1 and 5),
    courbatures     smallint    check (courbatures between 1 and 5),
    stress          smallint    check (stress between 1 and 5),
    humeur          smallint    check (humeur between 1 and 5),
    douleur_zone    text,
    commentaire     text,
    saisi_par       text,
    created_at      timestamptz not null default now(),
    updated_at      timestamptz not null default now(),
    unique (joueuse, date)
);
create index if not exists wellness_quotidien_date_idx on wellness_quotidien (date);

-- ── RPE de séance (échelle CR-10 de Foster, charge interne = RPE × durée en UA)
create table if not exists rpe_seance (
    id          bigserial primary key,
    joueuse     text        not null,
    date        date        not null,
    creneau     text        not null default 'Unique',   -- Unique / Matin / Après-midi / Match
    type_seance text        not null default 'Entraînement', -- Entraînement / Match / Réathlé / Autre
    rpe         numeric(3,1) not null check (rpe between 0 and 10),
    duree_min   numeric(5,1) not null check (duree_min >= 0),
    charge_ua   numeric(7,1) generated always as (rpe * duree_min) stored,
    commentaire text,
    saisi_par   text,
    created_at  timestamptz not null default now(),
    updated_at  timestamptz not null default now(),
    unique (joueuse, date, creneau)
);
create index if not exists rpe_seance_date_idx on rpe_seance (date);

-- ── Contenu pratique des séances (thème, objectifs, exercices, notes, fichiers)
create table if not exists seances_contenu (
    id          bigserial primary key,
    date        date        not null,
    creneau     text        not null default 'Unique',
    equipe      text        not null default '',
    microcycle  text,                 -- MD-4, MD-3, MD-2, MD-1, MD+1, Récupération…
    theme       text,
    objectifs   text,
    duree_min   numeric(5,1),
    exercices   jsonb       not null default '[]'::jsonb,
    -- [{"nom": "...", "duree_min": 12, "format": "8v8 1/2 terrain", "intensite": "Haute", "consignes": "..."}]
    notes       text,
    fichiers    jsonb       not null default '[]'::jsonb,
    -- [{"nom": "schema.pdf", "chemin": "2026-10-09/Unique/schema.pdf", "type": "application/pdf", "taille": 12345, "stockage": "supabase"|"local"}]
    saisi_par   text,
    created_at  timestamptz not null default now(),
    updated_at  timestamptz not null default now(),
    unique (date, creneau, equipe)
);
create index if not exists seances_contenu_date_idx on seances_contenu (date);

-- ── Bucket de stockage privé pour les fichiers joints aux séances
insert into storage.buckets (id, name, public)
values ('seances', 'seances', false)
on conflict (id) do nothing;

-- L'app utilise la clé service_role (bypass RLS) : on active RLS sans policy
-- pour fermer l'accès aux clés anon/authenticated, comme pour les autres tables.
alter table wellness_quotidien enable row level security;
alter table rpe_seance         enable row level security;
alter table seances_contenu    enable row level security;
