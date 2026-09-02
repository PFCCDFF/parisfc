-- 2026-08-28_evenements_match_identite.sql
-- Additif uniquement : aucune colonne/table existante modifiée ou supprimée.
-- evenement_tags continue d'exister et d'être écrite comme avant.

alter table evenements_match
    add column if not exists joueuse_id uuid references joueuses(id),
    add column if not exists row_brut text,
    add column if not exists tags jsonb;

create index if not exists evenements_match_joueuse_id_idx
    on evenements_match (joueuse_id);
