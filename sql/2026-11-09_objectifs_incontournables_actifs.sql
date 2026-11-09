-- Onglet Objectifs : les indicateurs affichés pour une joueuse sont désormais les lignes
-- `actif` de objectifs_joueuse, pour toutes les catégories (le staff les coche dans l'app).
-- Avant, les incontournables s'affichaient tous, actifs ou non : on les marque actifs pour
-- qu'ils restent visibles.
update objectifs_joueuse set actif = true where categorie = 'incontournable' and actif is not true;
