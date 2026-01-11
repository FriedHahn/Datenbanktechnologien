package de.htwberlin.dbtech.aufgaben.ue03.tdg;

public interface BuchungDao {
    boolean istManuellRegistriert(int mautAbschnitt, String kennzeichen);
    boolean istBuchungAbgeschlossen(int mautAbschnitt, String kennzeichen);
    void setzeBuchungAbgeschlossen(String kennzeichen, int mautAbschnitt);
    String ermittleAchszahlRegel(int mautAbschnitt, String kennzeichen);
}
