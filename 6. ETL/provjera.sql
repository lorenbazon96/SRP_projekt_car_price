/*******************************************************************************
  SQL SKRIPTA ZA PROVJERU I ANALIZU DIMENZIJSKOG MODELA (STAR SCHEMA)
  Pokrenuti nakon main.py (cars_price_dw baza).
*******************************************************************************/

USE cars_price_dw;

-- =============================================================================
-- DIO 1: VERIFIKACIJA - broj redova po tablicama
-- =============================================================================
SELECT 'dim_vozilo'         AS Tablica, COUNT(*) AS Broj_Redova FROM dim_vozilo
UNION ALL
SELECT 'dim_datum',          COUNT(*) FROM dim_datum
UNION ALL
SELECT 'dim_lokacija',       COUNT(*) FROM dim_lokacija
UNION ALL
SELECT 'dim_prodavac',       COUNT(*) FROM dim_prodavac
UNION ALL
SELECT 'dim_stanje',         COUNT(*) FROM dim_stanje
UNION ALL
SELECT 'fact_vehicle_sales', COUNT(*) FROM fact_vehicle_sales;


-- =============================================================================
-- DIO 2: BUSINESS INTELLIGENCE upiti
-- =============================================================================

-- IZVJEŠTAJ 1: Top 10 marki po ukupnoj vrijednosti prodaje
SELECT
    v.make                              AS Marka,
    COUNT(*)                            AS Broj_Prodaja,
    ROUND(SUM(f.selling_price), 2)      AS Ukupni_Prihod,
    ROUND(AVG(f.selling_price), 2)      AS Prosjecna_Cijena
FROM fact_vehicle_sales f
JOIN dim_vozilo v ON f.vehicle_tk = v.vehicle_tk
GROUP BY v.make
ORDER BY Ukupni_Prihod DESC
LIMIT 10;


-- IZVJEŠTAJ 2: Razlika prodajne cijene i MMR procjene po marki
SELECT
    v.make                                        AS Marka,
    ROUND(AVG(f.selling_price - f.mmr), 2)        AS Prosjecna_Razlika_Od_MMR,
    ROUND(AVG(f.selling_price), 2)                AS Prosjecna_Cijena,
    ROUND(AVG(f.mmr), 2)                          AS Prosjecni_MMR
FROM fact_vehicle_sales f
JOIN dim_vozilo v ON f.vehicle_tk = v.vehicle_tk
WHERE f.mmr IS NOT NULL AND f.selling_price IS NOT NULL
GROUP BY v.make
ORDER BY Prosjecna_Razlika_Od_MMR DESC
LIMIT 15;


-- IZVJEŠTAJ 3: Prodaja kroz vrijeme (godina/kvartal)
SELECT
    d.year                              AS Godina,
    d.quarter                           AS Kvartal,
    COUNT(*)                            AS Broj_Prodaja,
    ROUND(SUM(f.selling_price), 2)      AS Prihod
FROM fact_vehicle_sales f
JOIN dim_datum d ON f.date_tk = d.date_tk
GROUP BY d.year, d.quarter
ORDER BY d.year DESC, d.quarter DESC;


-- IZVJEŠTAJ 4: Top 10 saveznih država po prihodu
SELECT
    l.state                             AS Drzava,
    COUNT(*)                            AS Broj_Prodaja,
    ROUND(SUM(f.selling_price), 2)      AS Prihod
FROM fact_vehicle_sales f
JOIN dim_lokacija l ON f.location_tk = l.location_tk
GROUP BY l.state
ORDER BY Prihod DESC
LIMIT 10;


-- IZVJEŠTAJ 5: Utjecaj stanja vozila (condition) na prosječnu cijenu
SELECT
    s.condition_val                     AS Stanje,
    COUNT(*)                            AS Broj_Prodaja,
    ROUND(AVG(f.selling_price), 2)      AS Prosjecna_Cijena,
    ROUND(AVG(f.odometer), 0)           AS Prosjecna_Kilometraza
FROM fact_vehicle_sales f
JOIN dim_stanje s ON f.condition_tk = s.condition_tk
GROUP BY s.condition_val
ORDER BY s.condition_val;


-- IZVJEŠTAJ 6: Top 10 prodavača po broju transakcija
SELECT
    p.seller                            AS Prodavac,
    COUNT(*)                            AS Broj_Prodaja,
    ROUND(SUM(f.selling_price), 2)      AS Prihod
FROM fact_vehicle_sales f
JOIN dim_prodavac p ON f.seller_tk = p.seller_tk
GROUP BY p.seller
ORDER BY Broj_Prodaja DESC
LIMIT 10;
