package de.htwberlin.dbtech.aufgaben.ue03.tdg;

import de.htwberlin.dbtech.exceptions.DataException;

import java.sql.*;

public class MautabschnittTdg implements MautabschnittDao {

    private final Connection connection;

    public MautabschnittTdg(Connection connection) {
        this.connection = connection;
    }

    @Override
    public long ermittleLaengeInMetern(int mautAbschnitt) {
        try (PreparedStatement s = connection.prepareStatement(
                "SELECT LAENGE FROM MAUTABSCHNITT WHERE ABSCHNITTS_ID = ?")) {
            s.setInt(1, mautAbschnitt);
            ResultSet r = s.executeQuery();
            return r.next() ? r.getLong("LAENGE") : 0L;
        } catch (SQLException e) {
            throw new DataException(e);
        }
    }
}
