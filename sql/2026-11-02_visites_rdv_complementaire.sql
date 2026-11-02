-- 2026-11-02_visites_rdv_complementaire.sql
-- Médical → Calendrier : case « Rdv complémentaire » après le commentaire d'une visite.
-- Cochée, elle permet de noter le rendez-vous pris chez un spécialiste :
--   rdv_specialiste (type de spécialiste), rdv_medecin (nom, facultatif),
--   rdv_adresse (facultatif), rdv_date (facultatif).
-- Additif uniquement : colonnes ajoutées à visites_medicales, rien de supprimé.
-- À exécuter dans Supabase → SQL Editor AVANT d'utiliser la case dans l'app.

alter table visites_medicales add column if not exists rdv_complementaire boolean not null default false;
alter table visites_medicales add column if not exists rdv_specialiste    text;
alter table visites_medicales add column if not exists rdv_medecin        text;
alter table visites_medicales add column if not exists rdv_adresse        text;
alter table visites_medicales add column if not exists rdv_date           date;
