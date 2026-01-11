package de.htwberlin.dbtech.aufgaben.ue03.tdg;

import java.math.BigDecimal;

public interface FahrzeugDao {
    boolean istAutoRegistriert(String kennzeichen);
    int ermittleSchadstoffklasseId(String kennzeichen, int achszahl);
    long ermittleFzgId(String kennzeichen, int achszahl, int ssklId);
}
