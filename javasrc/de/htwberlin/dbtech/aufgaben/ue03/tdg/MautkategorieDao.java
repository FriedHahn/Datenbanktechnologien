package de.htwberlin.dbtech.aufgaben.ue03.tdg;

import java.math.BigDecimal;
import java.util.Map;

public interface MautkategorieDao {

    /**
     * Berechnet für eine Schadstoffklasse (SSKL_ID), Achszahl und Streckenlänge
     * die passende Kategorie und den Preis.
     *
     * Rückgabe:
     *   key   = KATEGORIE_ID
     *   value = Preis (BigDecimal)
     */
    Map.Entry<Integer, BigDecimal> berechneMautpreisFuerFahrzeug(
            int ssklId, int achszahl, long laengeMeter);
}
