package de.htwberlin.dbtech.aufgaben.ue02;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import de.htwberlin.dbtech.exceptions.DataException;

/**
 * Die Klasse realisiert die Mautverwaltung.
 * 
 * @author Patrick Dohmeier
 */
public class MautVerwaltungImpl implements IMautVerwaltung {

	private static final Logger L = LoggerFactory.getLogger(MautVerwaltungImpl.class);
	private Connection connection;

	@Override
	public void setConnection(Connection connection) {
		this.connection = connection;
	}

	private Connection getConnection() {
		if (connection == null) {
			throw new DataException("Connection not set");
		}
		return connection;
	}

	@Override
	public String getStatusForOnBoardUnit(long fzg_id) {

		PreparedStatement ps = null;
		ResultSet rs = null;
		String query = "SELECT status FROM Fahrzeuggerat f WHERE f.FZG_ID = ?";
		String result = "";

		try {
			ps = getConnection().prepareStatement(query);
			ps.setLong(1, fzg_id);
			rs = ps.executeQuery();

			if(rs.next()) {

				result = rs.getString("status");
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (NullPointerException e) {
			throw new RuntimeException(e);
		}
		return result;
	}

	@Override
	public int getUsernumber(int maut_id) {
		int result = 0;
		final String sql =
				"SELECT f.NUTZER_ID FROM MAUTERHEBUNG m JOIN FAHRZEUGGERAT g ON m.FZG_ID = g.FZG_ID JOIN FAHRZEUG f ON g.FZ_ID = f.FZ_ID WHERE m.MAUT_ID = ?";

		try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
			ps.setInt(1, maut_id);
			try (ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					result = rs.getInt("NUTZER_ID");
				}
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		return result;
	}


	@Override
	public void registerVehicle(long fz_id, int sskl_id, int nutzer_id, String kennzeichen, String fin, int achsen,
			int gewicht, String zulassungsland) {
		final String sql = "INSERT INTO FAHRZEUG (FZ_ID, SSKL_ID, NUTZER_ID, KENNZEICHEN, FIN, ACHSEN, GEWICHT, ZULASSUNGSLAND, ANMELDEDATUM) "
				+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_DATE)";

		final String insertDevice = "INSERT INTO FAHRZEUGGERAT (FZG_ID, FZ_ID, STATUS, EINBAUDATUM) " +
		"VALUES ((SELECT COALESCE(MAX(FZG_ID),0)+1 FROM FAHRZEUGGERAT), ?, 'aktiv', CURRENT_DATE)";

		try (PreparedStatement ps1 = getConnection().prepareStatement(sql);
			 PreparedStatement ps2 = getConnection().prepareStatement(insertDevice)) {
			ps1.setLong(1, fz_id);
			ps1.setInt(2, sskl_id);
			ps1.setInt(3, nutzer_id);
			ps1.setString(4, kennzeichen);
			ps1.setString(5, fin);
			ps1.setInt(6, achsen);
			ps1.setInt(7, gewicht);
			ps1.setString(8, zulassungsland);

			int rows = ps1.executeUpdate();
			if (rows == 0) {
				throw new DataException("Kein Datensatz eingefügt für FZ_ID=" + fz_id);
			}

			ps2.setLong(1, fz_id);
			ps2.executeUpdate();

			L.info("Fahrzeug {} erfolgreich registriert.", fz_id);
		} catch (SQLException e) {
			L.error("Fehler beim Registrieren des Fahrzeugs mit FZ_ID={}", fz_id, e);
			throw new DataException("Fehler beim Registrieren des Fahrzeugs: " + e.getMessage(), e);
		}
	}


	@Override
	public void updateStatusForOnBoardUnit(long fzg_id, String status) {

		String sql = "UPDATE FAHRZEUGGERAT SET STATUS = ? WHERE FZG_ID = ?";

		try (PreparedStatement ps = getConnection().prepareStatement(sql)) {

			ps.setString(1, status);
			ps.setLong(2, fzg_id);

			int affected = ps.executeUpdate();
			if (affected == 0) {
				L.warn("Kein Gerät mit FZG_ID {} gefunden.", fzg_id);
			}

		} catch (SQLException e) {
			L.error("Fehler beim Aktualisieren des Status für FZG_ID {}", fzg_id, e);
			throw new DataException("Fehler beim Aktualisieren des Status: " + e.getMessage(), e);
		}
	}

	@Override
	public void deleteVehicle(long fz_id) {
		String sql = "DELETE FROM FAHRZEUG WHERE FZ_ID = ?";

		try (var preparedStatement = getConnection().prepareStatement(sql)) {
			preparedStatement.setLong(1, fz_id);
			int rowsAffected = preparedStatement.executeUpdate();

			if (rowsAffected == 0) {
				L.warn("Kein Fahrzeug mit FZ_ID = {} gefunden.", fz_id);
			} else {
				L.info("Fahrzeug mit FZ_ID = {} erfolgreich gelöscht.", fz_id);
			}

		} catch (SQLException e) {
			L.error("Fehler beim Löschen des Fahrzeugs mit FZ_ID = {}", fz_id, e);
			throw new DataException("Fehler beim Löschen des Fahrzeugs: " + e.getMessage(), e);
		}

	}

	@Override
	public List<Mautabschnitt> getTrackInformations(String abschnittstyp) {
		String sql = """
        SELECT ABSCHNITTS_ID, LAENGE, START_KOORDINATE, ZIEL_KOORDINATE, NAME, ABSCHNITTSTYP
        FROM MAUTABSCHNITT
        WHERE ABSCHNITTSTYP = ?
        ORDER BY ABSCHNITTS_ID
        """;

		List<Mautabschnitt> result = new ArrayList<>();

		try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
			ps.setString(1, abschnittstyp);

			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					Mautabschnitt abschnitt = new Mautabschnitt(
							rs.getInt("ABSCHNITTS_ID"),
							rs.getInt("LAENGE"),
							rs.getString("START_KOORDINATE"),
							rs.getString("ZIEL_KOORDINATE"),
							rs.getString("NAME"),
							rs.getString("ABSCHNITTSTYP")
					);
					result.add(abschnitt);
				}
			}
		} catch (SQLException e) {
			L.error("Fehler beim Abfragen der Mautabschnitte für Typ = {}", abschnittstyp, e);
			throw new DataException("Fehler beim Abfragen der Mautabschnitte: " + e.getMessage(), e);
		}

		return result;
	}


}
