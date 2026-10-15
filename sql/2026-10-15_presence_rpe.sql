-- 2026-10-15_presence_rpe.sql
-- Saisie RPE + présence à l'entraînement (Performance → Entraînement → RPE).
-- La table presence_entrainement était lue/écrite par l'app mais n'existait pas dans ce
-- projet Supabase : rien n'était enregistré. Création, avec la RPE (échelle de Borg 1-10,
-- questionnaire sRPE) : une ligne par joueuse et par jour.
--   statut = 'Présente' (RPE renseignée) ou motif d'absence :
--            'Absente', 'Sélection', 'Réathlé', 'Blessée (Kiné)', 'Pro'
-- Additif uniquement : aucune table existante modifiée.

create table if not exists presence_entrainement (
    joueuse     text        not null,
    date        date        not null,
    statut      text        not null,
    rpe         smallint    check (rpe between 1 and 10),
    saisi_par   text,
    updated_at  timestamptz not null default now(),
    primary key (joueuse, date)
);

-- Si la table existait déjà sans RPE (autre environnement) :
alter table presence_entrainement add column if not exists rpe smallint check (rpe between 1 and 10);
alter table presence_entrainement add column if not exists saisi_par text;
alter table presence_entrainement add column if not exists updated_at timestamptz not null default now();

create index if not exists presence_entrainement_date_idx on presence_entrainement (date);

-- L'app écrit avec la clé service_role (RLS contourné) : RLS activé sans policy publique.
alter table presence_entrainement enable row level security;
