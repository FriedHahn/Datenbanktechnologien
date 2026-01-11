package de.htwberlin.dbtech.aufgaben.ue03;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.*;
import java.time.LocalDate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import de.htwberlin.dbtech.exceptions.DataException;
import de.htwberlin.dbtech.exceptions.AlreadyCruisedException;
import de.htwberlin.dbtech.exceptions.InvalidVehicleDataException;
import de.htwberlin.dbtech.exceptions.UnkownVehicleException;

/**
 * Die Klasse realisiert den AusleiheService.
 *
 * @author Patrick Dohmeier
 */
public class MautServiceImpl implements IMautService {

	private static final Logger L = LoggerFactory.getLogger(MautServiceImpl.class);
	private Connection connection;

	@Override
	public void setConnection(Connection connection) {this.connection = connection;
	}

	private Connection getConnection() {
		if (connection == null) {
			throw new DataException("Connection not set");
		}
		return connection;
	}

	@Override
	public void berechneMaut(int mautAbschnitt, int achszahl, String kennzeichen)
			throws UnkownVehicleException, InvalidVehicleDataException, AlreadyCruisedException {

		boolean istAutomatisch = istAutoRegistriert(kennzeichen);
		boolean istManuell = istManuellRegistriert(mautAbschnitt, kennzeichen);

		if (!istAutomatisch && !istManuell) {
			throw new UnkownVehicleException("Fahrzeug nicht registriert");
		}

		boolean istAchszahlRichtig = istAchszahlRichtig(mautAbschnitt, achszahl, kennzeichen,
				istAutomatisch, istManuell);
		boolean istBuchungAbgeschlossen = istBuchungAbgeschlossen(mautAbschnitt, kennzeichen);

		if (!istAchszahlRichtig) {
			throw new InvalidVehicleDataException("Fahrzeugdaten (Achszahl) sind nicht korrekt");
		}

		if (istManuell) {
			if (istBuchungAbgeschlossen) {
				throw new AlreadyCruisedException("Strecke wurde bereits befahren");
			}
			buchungsStatusAbgeschlossen(kennzeichen, mautAbschnitt);
		}

		if (istAutomatisch) {
			autoMautberechnung(achszahl, kennzeichen, mautAbschnitt);
		}
	}

	private boolean istManuellRegistriert(int mautAbschnitt, String kennzeichen) {
		try (PreparedStatement s = connection.prepareStatement(
				"SELECT * FROM BUCHUNG b WHERE KENNZEICHEN = ? AND ABSCHNITTS_ID = ?")) {

			s.setString(1, kennzeichen);
			s.setInt(2, mautAbschnitt);
			ResultSet r = s.executeQuery();
			return r.next();
		} catch (SQLException e) {
			throw new DataException(e);
		}
	}

	private boolean istAutoRegistriert(String kennzeichen) {
		try (PreparedStatement s = connection.prepareStatement(
				"SELECT * FROM FAHRZEUG WHERE KENNZEICHEN = ? AND ABMELDEDATUM IS NULL")) {

			s.setString(1, kennzeichen);
			ResultSet r = s.executeQuery();
			return r.next();
		} catch (SQLException e) {
			throw new DataException(e);
		}
	}

	private boolean istAchszahlRichtig(int mautAbschnitt, int achszahl, String kennzeichen,
									   boolean istAutomatisch, boolean istManuell) {

		if (istAutomatisch) {
			try (PreparedStatement s = connection.prepareStatement(
					"SELECT 1 FROM FAHRZEUG " +
							"WHERE KENNZEICHEN = ? AND ACHSEN = ? AND ABMELDEDATUM IS NULL")) {

				s.setString(1, kennzeichen);
				s.setInt(2, achszahl);
				return s.executeQuery().next();

			} catch (SQLException e) {
				throw new DataException(e);
			}
		}

		if (istManuell) {
			try (PreparedStatement s = connection.prepareStatement(
					"SELECT MK.ACHSZAHL " +
							"FROM BUCHUNG B " +
							"JOIN MAUTKATEGORIE MK ON B.KATEGORIE_ID = MK.KATEGORIE_ID " +
							"WHERE B.ABSCHNITTS_ID = ? AND B.KENNZEICHEN = ?")) {

				s.setInt(1, mautAbschnitt);
				s.setString(2, kennzeichen);
				ResultSet r = s.executeQuery();

				if (r.next()) {
					String regel = r.getString("ACHSZAHL");
					return passtZurAchszahl(regel, achszahl);
				}
				return false;

			} catch (SQLException e) {
				throw new DataException(e);
			}
		}
		return false;
	}

	private boolean istBuchungAbgeschlossen(int mautAbschnitt, String kennzeichen) {
		try (PreparedStatement s = connection.prepareStatement(
				"SELECT B_ID FROM BUCHUNG WHERE ABSCHNITTS_ID = ? AND KENNZEICHEN = ?")) {

			s.setInt(1, mautAbschnitt);
			s.setString(2, kennzeichen);
			ResultSet r = s.executeQuery();

			if (r.next()) {
				int status = r.getInt("B_ID");
				return status == 3;
			}
			return false;

		} catch (SQLException e) {
			throw new DataException(e);
		}
	}

	private void buchungsStatusAbgeschlossen(String kennzeichen, int mautAbschnitt){
		try (PreparedStatement s = connection.prepareStatement("Update buchung set b_id = 3, befahrungsdatum = ? where abschnitts_id = ? AND kennzeichen = ?")) {
			s.setDate(1, Date.valueOf(LocalDate.now()));
			s.setInt(2, mautAbschnitt);
			s.setString(3, kennzeichen);

			s.executeUpdate();

		} catch (SQLException e) {
			throw new DataException(e);
		}
	}

	private void autoMautberechnung(int achszahl, String kennzeichen, int mautAbschnitt) {
		long laengeMeter = 0L;
		int SSKL_ID = 0;
		BigDecimal preis = BigDecimal.ZERO;
		int kategorie = 0;
		long fzg = 0;
		int maut = 0;

		try (PreparedStatement s = connection.prepareStatement(
				"SELECT LAENGE FROM MAUTABSCHNITT WHERE ABSCHNITTS_ID = ?")) {

			s.setInt(1, mautAbschnitt);
			ResultSet r = s.executeQuery();
			if (r.next()) {
				laengeMeter = r.getLong("LAENGE");
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}

		try (PreparedStatement s = connection.prepareStatement(
				"SELECT SSKL_ID FROM FAHRZEUG WHERE ACHSEN = ? AND KENNZEICHEN = ?")) {

			s.setInt(1, achszahl);
			s.setString(2, kennzeichen);
			ResultSet r = s.executeQuery();
			if (r.next()) {
				SSKL_ID = r.getInt("SSKL_ID");
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}

		try (PreparedStatement s = connection.prepareStatement(
				"SELECT KATEGORIE_ID, ACHSZAHL, MAUTSATZ_JE_KM " +
						"FROM MAUTKATEGORIE WHERE SSKL_ID = ?")) {

			s.setInt(1, SSKL_ID);
			ResultSet r = s.executeQuery();

			while (r.next()) {
				String regel = r.getString("ACHSZAHL");

				if (passtZurAchszahl(regel, achszahl)) {
					BigDecimal mautsatzJeKm = r.getBigDecimal("MAUTSATZ_JE_KM");
					kategorie = r.getInt("KATEGORIE_ID");

					BigDecimal lengthKm = BigDecimal.valueOf(laengeMeter)
							.divide(BigDecimal.valueOf(1000), 3, RoundingMode.HALF_UP);

					preis = mautsatzJeKm
							.divide(BigDecimal.valueOf(100), 4, RoundingMode.HALF_UP)
							.multiply(lengthKm);

					preis = preis.setScale(2, RoundingMode.HALF_UP);

					break;
				}
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}

		try (PreparedStatement s = connection.prepareStatement(
				"SELECT fg.FZG_ID " +
						"FROM FAHRZEUGGERAT fg " +
						"JOIN FAHRZEUG f ON fg.FZ_ID = f.FZ_ID " +
						"WHERE f.SSKL_ID = ? " +
						"  AND f.KENNZEICHEN = ? " +
						"  AND f.ACHSEN = ?")) {

			s.setInt(1, SSKL_ID);
			s.setString(2, kennzeichen);
			s.setInt(3, achszahl);
			ResultSet r = s.executeQuery();
			if (r.next()) {
				fzg = r.getLong("FZG_ID");
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}

		try (PreparedStatement s = connection.prepareStatement(
				"SELECT COALESCE(MAX(MAUT_ID), 0) AS MAX_ID FROM MAUTERHEBUNG")) {

			ResultSet r = s.executeQuery();
			if (r.next()) {
				maut = (int) (r.getLong("MAX_ID") + 1);
			} else {
				maut = 1;
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}

		try (PreparedStatement s = connection.prepareStatement(
				"INSERT INTO MAUTERHEBUNG " +
						"(MAUT_ID, ABSCHNITTS_ID, FZG_ID, KATEGORIE_ID, BEFAHRUNGSDATUM, KOSTEN) " +
						"VALUES (?, ?, ?, ?, ?, ?)")) {

			s.setInt(1, maut);
			s.setInt(2, mautAbschnitt);
			s.setLong(3, fzg);
			s.setInt(4, kategorie);
			s.setDate(5, Date.valueOf(LocalDate.now()));
			s.setBigDecimal(6, preis);

			s.executeUpdate();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private boolean passtZurAchszahl(String regel, int achsen) {
		regel = regel.trim();

		if (regel.startsWith("=")) {
			int val = Integer.parseInt(regel.substring(1).trim());
			return achsen == val;
		}

		if (regel.startsWith(">=")) {
			int val = Integer.parseInt(regel.substring(2).trim());
			return achsen >= val;
		}

		throw new IllegalArgumentException("Unbekannte Regel in ACHSZAHL: " + regel);
	}
}
