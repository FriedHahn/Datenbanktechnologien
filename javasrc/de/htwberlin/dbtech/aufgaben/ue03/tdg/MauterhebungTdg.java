package de.htwberlin.dbtech.aufgaben.ue03.tdg;

import de.htwberlin.dbtech.aufgaben.ue03.MautServiceImplDao;
import de.htwberlin.dbtech.exceptions.DataException;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDate;

public class MauterhebungTdg implements MauterhebungDao {

    private final Connection connection;

    public MauterhebungTdg(Connection connection) {
        this.connection = connection;
    }

    @Override
    public int ermittleNaechsteMautId() {
        try (PreparedStatement s = connection.prepareStatement(
                "SELECT COALESCE(MAX(MAUT_ID), 0) AS MAX_ID FROM MAUTERHEBUNG")) {
            ResultSet r = s.executeQuery();
            return r.next() ? (int) (r.getLong("MAX_ID") + 1) : 1;
        } catch (SQLException e) {
            throw new DataException(e);
        }
    }

    @Override
    public void fuegeMauterhebungEin(int mautId,
                                     int mautAbschnitt,
                                     long fzgId,
                                     int kategorieId,
                                     LocalDate datum,
                                     BigDecimal kosten) {
        try (PreparedStatement s = connection.prepareStatement(
                "INSERT INTO MAUTERHEBUNG " +
                        "(MAUT_ID, ABSCHNITTS_ID, FZG_ID, KATEGORIE_ID, BEFAHRUNGSDATUM, KOSTEN) " +
                        "VALUES (?, ?, ?, ?, ?, ?)")) {
            s.setInt(1, mautId);
            s.setInt(2, mautAbschnitt);
            s.setLong(3, fzgId);
            s.setInt(4, kategorieId);
            s.setDate(5, Date.valueOf(datum));
            s.setBigDecimal(6, kosten);
            s.executeUpdate();
        } catch (SQLException e) {
            throw new DataException(e);
        }
    }

}
