package de.htwberlin.dbtech.aufgaben.ue03.tdg;

import de.htwberlin.dbtech.aufgaben.ue03.MautServiceImplDao;
import de.htwberlin.dbtech.exceptions.DataException;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.*;
import java.util.Map;

        public class MautkategorieTdg implements MautkategorieDao {

    private final Connection connection;

    public MautkategorieTdg(Connection connection) {
        this.connection = connection;
    }

    @Override
    public Map.Entry<Integer, BigDecimal> berechneMautpreisFuerFahrzeug(
            int ssklId, int achszahl, long laengeMeter) {

        try (PreparedStatement s = connection.prepareStatement(
                "SELECT KATEGORIE_ID, ACHSZAHL, MAUTSATZ_JE_KM " +
                        "FROM MAUTKATEGORIE WHERE SSKL_ID = ?")) {

            s.setInt(1, ssklId);
            ResultSet r = s.executeQuery();

            while (r.next()) {
                String regel = r.getString("ACHSZAHL");
                if (MautServiceImplDao.passtZurAchszahl(regel, achszahl)) {

                    BigDecimal mautsatzJeKm = r.getBigDecimal("MAUTSATZ_JE_KM");
                    int kategorieId = r.getInt("KATEGORIE_ID");

                    BigDecimal lengthKm = BigDecimal.valueOf(laengeMeter)
                            .divide(BigDecimal.valueOf(1000), 3, RoundingMode.HALF_UP);

                    BigDecimal preis = mautsatzJeKm
                            .divide(BigDecimal.valueOf(100), 4, RoundingMode.HALF_UP) // ct -> €
                            .multiply(lengthKm)
                            .setScale(2, RoundingMode.HALF_UP);

                    // key   = Kategorie-ID
                    // value = Preis
                    return Map.entry(kategorieId, preis);
                }
            }

            // nichts gefunden -> 0 / 0 €
            return Map.entry(0, BigDecimal.ZERO);

        } catch (SQLException e) {
            throw new DataException(e);
        }
    }
}
