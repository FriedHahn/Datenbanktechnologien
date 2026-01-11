package de.htwberlin.dbtech.aufgaben.ue03.tdg;

import de.htwberlin.dbtech.exceptions.DataException;

import java.sql.*;

public class FahrzeugTdg implements FahrzeugDao {

    private final Connection connection;

    public FahrzeugTdg(Connection connection) {
        this.connection = connection;
    }

    @Override
    public boolean istAutoRegistriert(String kennzeichen) {
        try (PreparedStatement s = connection.prepareStatement(
                "SELECT 1 FROM FAHRZEUG WHERE KENNZEICHEN = ? AND ABMELDEDATUM IS NULL")) {
            s.setString(1, kennzeichen);
            return s.executeQuery().next();
        } catch (SQLException e) {
            throw new DataException(e);
        }
    }

    @Override
    public int ermittleSchadstoffklasseId(String kennzeichen, int achszahl) {
        try (PreparedStatement s = connection.prepareStatement(
                "SELECT SSKL_ID FROM FAHRZEUG WHERE ACHSEN = ? AND KENNZEICHEN = ?")) {
            s.setInt(1, achszahl);
            s.setString(2, kennzeichen);
            ResultSet r = s.executeQuery();
            return r.next() ? r.getInt("SSKL_ID") : 0;
        } catch (SQLException e) {
            throw new DataException(e);
        }
    }

    @Override
    public long ermittleFzgId(String kennzeichen, int achszahl, int ssklId) {
        try (PreparedStatement s = connection.prepareStatement(
                "SELECT fg.FZG_ID " +
                        "FROM FAHRZEUGGERAT fg " +
                        "JOIN FAHRZEUG f ON fg.FZ_ID = f.FZ_ID " +
                        "WHERE f.SSKL_ID = ? AND f.KENNZEICHEN = ? AND f.ACHSEN = ?")) {
            s.setInt(1, ssklId);
            s.setString(2, kennzeichen);
            s.setInt(3, achszahl);
            ResultSet r = s.executeQuery();
            return r.next() ? r.getLong("FZG_ID") : 0L;
        } catch (SQLException e) {
            throw new DataException(e);
        }
    }
}
