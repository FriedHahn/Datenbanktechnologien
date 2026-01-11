package de.htwberlin.dbtech.aufgaben.ue03.tdg;

import java.math.BigDecimal;
import java.time.LocalDate;

public interface MauterhebungDao {
    int ermittleNaechsteMautId();
    void fuegeMauterhebungEin(int mautId,
                              int mautAbschnitt,
                              long fzgId,
                              int kategorieId,
                              LocalDate datum,
                              BigDecimal kosten);
}
