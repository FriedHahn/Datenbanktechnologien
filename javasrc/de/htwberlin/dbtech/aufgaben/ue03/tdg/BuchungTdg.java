package de.htwberlin.dbtech.aufgaben.ue03.tdg;

import de.htwberlin.dbtech.exceptions.DataException;

import java.sql.*;
import java.time.LocalDate;

public class BuchungTdg implements BuchungDao {

    private final Connection connection;

    public BuchungTdg(Connection connection) {
        this.connection = connection;
    }

    @Override
    public boolean istManuellRegistriert(int mautAbschnitt, String kennzeichen) {
        try (PreparedStatement s = connection.prepareStatement(
                "SELECT 1 FROM BUCHUNG WHERE KENNZEICHEN = ? AND ABSCHNITTS_ID = ?")) {
            s.setString(1, kennzeichen);
            s.setInt(2, mautAbschnitt);
            return s.executeQuery().next();
        } catch (SQLException e) {
            throw new DataException(e);
        }
    }

    @Override
    public boolean istBuchungAbgeschlossen(int mautAbschnitt, String kennzeichen) {
        try (PreparedStatement s = connection.prepareStatement(
                "SELECT B_ID FROM BUCHUNG WHERE ABSCHNITTS_ID = ? AND KENNZEICHEN = ?")) {
            s.setInt(1, mautAbschnitt);
            s.setString(2, kennzeichen);
            ResultSet r = s.executeQuery();
            if (r.next()) {
                return r.getInt("B_ID") == 3;
            }
            return false;
        } catch (SQLException e) {
            throw new DataException(e);
        }
    }

    @Override
    public void setzeBuchungAbgeschlossen(String kennzeichen, int mautAbschnitt) {
        try (PreparedStatement s = connection.prepareStatement(
                "UPDATE BUCHUNG SET B_ID = 3, BEFAHRUNGSDATUM = ? " +
                        "WHERE ABSCHNITTS_ID = ? AND KENNZEICHEN = ?")) {
            s.setDate(1, Date.valueOf(LocalDate.now()));
            s.setInt(2, mautAbschnitt);
            s.setString(3, kennzeichen);
            s.executeUpdate();
        } catch (SQLException e) {
            throw new DataException(e);
        }
    }

    @Override
    public String ermittleAchszahlRegel(int mautAbschnitt, String kennzeichen) {
        try (PreparedStatement s = connection.prepareStatement(
                "SELECT MK.ACHSZAHL " +
                        "FROM BUCHUNG B " +
                        "JOIN MAUTKATEGORIE MK ON B.KATEGORIE_ID = MK.KATEGORIE_ID " +
                        "WHERE B.ABSCHNITTS_ID = ? AND B.KENNZEICHEN = ?")) {
            s.setInt(1, mautAbschnitt);
            s.setString(2, kennzeichen);
            ResultSet r = s.executeQuery();
            return r.next() ? r.getString("ACHSZAHL") : null;
        } catch (SQLException e) {
            throw new DataException(e);
        }
    }
}
