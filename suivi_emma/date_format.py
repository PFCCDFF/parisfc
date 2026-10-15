# Porté depuis l'app d'Emma Debaud (paris-fc-charge, utils/date_format.py, version du 14/10/2026).
# Seuls les imports ont été adaptés. Garder la logique alignée sur l'original.
MOIS = {
    1: "janvier", 2: "février", 3: "mars", 4: "avril",
    5: "mai", 6: "juin", 7: "juillet", 8: "août",
    9: "septembre", 10: "octobre", 11: "novembre", 12: "décembre"
}
JOURS = {
    0: "lundi", 1: "mardi", 2: "mercredi", 3: "jeudi",
    4: "vendredi", 5: "samedi", 6: "dimanche"
}

def fmt_date(dt):
    return f"{JOURS[dt.weekday()]} {dt.day} {MOIS[dt.month]} {dt.year}"

def fmt_date_short(dt):
    """Format concis : 23/12/25"""
    return dt.strftime("%d/%m/%y")


def fmt_matchday(index):
    """Affiche une journée Impect, dont l'index commence à zéro."""
    return f"J{int(index) + 1}"
