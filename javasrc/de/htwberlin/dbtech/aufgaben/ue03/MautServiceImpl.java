package de.htwberlin.dbtech.aufgaben.ue03;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.htwberlin.dbtech.exceptions.AlreadyCruisedException;
import de.htwberlin.dbtech.exceptions.DataException;
import de.htwberlin.dbtech.exceptions.InvalidVehicleDataException;
import de.htwberlin.dbtech.exceptions.UnkownVehicleException;

/**
 * Die Klasse realisiert den Mautservice.
 *
 * @author Patrick Vollert
 */
public class MautServiceImpl implements IMautService {

	private static final Logger L = LoggerFactory.getLogger(MautServiceImpl.class);
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
	public void berechneMaut(int mautAbschnitt, int achszahl, String kennzeichen)
			throws UnkownVehicleException, InvalidVehicleDataException, AlreadyCruisedException {

		if (!isVehicleRegistered(kennzeichen)) {
			throw new UnkownVehicleException("Das Fahrzeug ist nicht bekannt!-> Mautpreller");
		}

		Connection con = getConnection();

		try {
			Vehicle vehicle = loadVehicle(con, kennzeichen);

			BookingInfos bookings = loadBookings(con, kennzeichen, mautAbschnitt);

			boolean hasVehicle = vehicle != null;
			boolean hasAnyBooking = bookings.anyBooking != null;
			boolean hasOpenBooking = bookings.openBooking != null;
			boolean hasActiveDevice = hasVehicle && vehicle.hasActiveDevice;

			if (!hasActiveDevice && !hasAnyBooking) {
				throw new UnkownVehicleException(
						"Fahrzeug ist weder im automatischen Verfahren aktiv noch liegt eine Buchung vor");
			}

			if (hasActiveDevice) {

				if (achszahl > vehicle.achsen) {
					throw new InvalidVehicleDataException(
							"Im automatischen Verfahren ist die angegebene Achszahl groesser als registriert");
				}

				double kosten = calculateToll(con, mautAbschnitt, achszahl, vehicle.ssklId);
				insertMauterhebung(con, mautAbschnitt, vehicle.activeDeviceId, vehicle.ssklId, achszahl, kosten);

			} else {

				if (!hasAnyBooking) {
					throw new UnkownVehicleException("Keine Buchung fuer das Fahrzeug vorhanden");
				}

				if (!hasOpenBooking) {
					throw new AlreadyCruisedException(
							"Der Streckenabschnitt wurde mit dieser Buchung bereits befahren");
				}

				Booking booking = bookings.openBooking;

				int bookedAxles = getAchszahlForKategorie(con, booking.kategorieId);

				if (achszahl != bookedAxles) {
					throw new InvalidVehicleDataException(
							"Die gebuchte Mautkategorie passt nicht zur angegebenen Achszahl");
				}

				closeBooking(con, booking.buchungId);
			}

		} catch (SQLException e) {
			L.error("SQL-Fehler bei der Mauterhebung", e);
			throw new DataException("Fehler bei der Mauterhebung", e);
		}
	}

	/**
	 * prüft, ob das Fahrzeug bereits registriert und aktiv ist oder eine
	 * manuelle offene Buchung für das Fahrzeug vorliegt.
	 *
	 * @param kennzeichen das Kennzeichen des Fahrzeugs
	 * @return true wenn das Fahrzeug registiert ist, false wenn nicht
	 **/
	public boolean isVehicleRegistered(String kennzeichen) {
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;

		try {
			String queryString =
					"SELECT SUM( ANZAHL ) AS ANZAHL FROM (" +
							"  SELECT COUNT(F.KENNZEICHEN) AS ANZAHL " +
							"  FROM FAHRZEUG F " +
							"  JOIN FAHRZEUGGERAT FZG ON F.FZ_ID = FZG.FZ_ID " +
							"  AND F.ABMELDEDATUM IS NULL " +
							"  AND FZG.STATUS = 'active' " +
							"  AND F.KENNZEICHEN = ? " +
							"  UNION ALL " +
							"  SELECT COUNT(KENNZEICHEN) AS ANZAHL " +
							"  FROM BUCHUNG " +
							"  WHERE KENNZEICHEN = ? AND B_ID = 1" +
							")";

			preparedStatement = getConnection().prepareStatement(queryString);
			preparedStatement.setString(1, kennzeichen);
			preparedStatement.setString(2, kennzeichen);
			resultSet = preparedStatement.executeQuery();
			if (resultSet.next()) {
				return resultSet.getLong("ANZAHL") > 0;
			} else {
				return false;
			}
		} catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	/**
	 * Fahrzeug und aktives Fahrzeuggeraet laden.
	 *
	 * Tabellen: FAHRZEUG, FAHRZEUGGERAT
	 */
	private Vehicle loadVehicle(Connection con, String kennzeichen) throws SQLException {
		String sql =
				"SELECT f.FZ_ID, f.SSKL_ID, f.ACHSEN, g.FZG_ID, g.STATUS " +
						"FROM FAHRZEUG f " +
						"LEFT JOIN FAHRZEUGGERAT g ON f.FZ_ID = g.FZ_ID " +
						"WHERE f.KENNZEICHEN = ?";

		try (PreparedStatement ps = con.prepareStatement(sql)) {
			ps.setString(1, kennzeichen);
			try (ResultSet rs = ps.executeQuery()) {
				if (!rs.next()) {
					return null;
				}
				int fzId = rs.getInt("FZ_ID");
				int ssklId = rs.getInt("SSKL_ID");
				int achsen = rs.getInt("ACHSEN");

				Integer fzgId = (Integer) rs.getObject("FZG_ID");
				String status = rs.getString("STATUS");

				boolean hasActiveDevice = false;
				int activeDeviceId = 0;

				if (fzgId != null) {
					hasActiveDevice = status != null && status.equalsIgnoreCase("active");
					if (hasActiveDevice) {
						activeDeviceId = fzgId;
					}
				}

				return new Vehicle(fzId, ssklId, achsen, hasActiveDevice, activeDeviceId);
			}
		}
	}

	/**
	 * Alle Buchungen zu Kennzeichen und Abschnitt laden (ohne Join auf BUCHUNGSSTATUS).
	 * B_ID = 1 wird als "offen" interpretiert (siehe isVehicleRegistered).
	 */
	private BookingInfos loadBookings(Connection con, String kennzeichen, int abschnittId) throws SQLException {
		String sql =
				"SELECT BUCHUNG_ID, KATEGORIE_ID, B_ID " +
						"FROM BUCHUNG " +
						"WHERE KENNZEICHEN = ? AND ABSCHNITTS_ID = ?";

		Booking any = null;
		Booking open = null;

		try (PreparedStatement ps = con.prepareStatement(sql)) {
			ps.setString(1, kennzeichen);
			ps.setInt(2, abschnittId);

			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					int buchungId = rs.getInt("BUCHUNG_ID");
					int kategorieId = rs.getInt("KATEGORIE_ID");
					int statusId = rs.getInt("B_ID");

					Booking b = new Booking(buchungId, kategorieId, statusId);

					if (any == null) {
						any = b;
					}
					if (statusId == 1) {
						open = b;
					}
				}
			}
		}
		return new BookingInfos(any, open);
	}

	/**
	 * Achszahl zu einer Kategorie.
	 *
	 * Tabelle: MAUTKATEGORIE
	 */
	private int getAchszahlForKategorie(Connection con, int kategorieId) throws SQLException {
		String sql = "SELECT ACHSZAHL FROM MAUTKATEGORIE WHERE KATEGORIE_ID = ?";

		try (PreparedStatement ps = con.prepareStatement(sql)) {
			ps.setInt(1, kategorieId);
			try (ResultSet rs = ps.executeQuery()) {
				if (!rs.next()) {
					throw new DataException("Keine Mautkategorie mit ID " + kategorieId + " gefunden");
				}
				return rs.getInt("ACHSZAHL");
			}
		}
	}

	/**
	 * Kosten für automatisches Verfahren berechnen.
	 *
	 * Tabellen: MAUTABSCHNITT, MAUTKATEGORIE
	 * Formel: KOSTEN = LAENGE * MAUTSATZ_JE_KM
	 */
	private double calculateToll(Connection con, int abschnittId, int achszahl, int ssklId) throws SQLException {
		String sql =
				"SELECT a.LAENGE, k.MAUTSATZ_JE_KM " +
						"FROM MAUTABSCHNITT a, MAUTKATEGORIE k " +
						"WHERE a.ABSCHNITTS_ID = ? " +
						"AND k.SSKL_ID = ? " +
						"AND k.ACHSZAHL = ?";

		try (PreparedStatement ps = con.prepareStatement(sql)) {
			ps.setInt(1, abschnittId);
			ps.setInt(2, ssklId);
			ps.setInt(3, achszahl);

			try (ResultSet rs = ps.executeQuery()) {
				if (!rs.next()) {
					throw new DataException("Kein Tarif fuer Abschnitt " + abschnittId +
							" und Achszahl " + achszahl + " gefunden");
				}
				double laenge = rs.getDouble("LAENGE");
				double mautsatz = rs.getDouble("MAUTSATZ_JE_KM");
				return laenge * mautsatz;
			}
		}
	}

	/**
	 * Neue Mauterhebung schreiben.
	 *
	 * Tabelle: MAUTERHEBUNG
	 */
	private void insertMauterhebung(Connection con, int abschnittId, int fzgId,
									int ssklId, int achszahl, double kosten) throws SQLException {

		int kategorieId = getKategorieId(con, ssklId, achszahl);
		int mautId = nextMautId(con);

		String sql =
				"INSERT INTO MAUTERHEBUNG (MAUT_ID, ABSCHNITTS_ID, FZG_ID, KATEGORIE_ID, BEFAHRUNGSDATUM, KOSTEN) " +
						"VALUES (?, ?, ?, ?, ?, ?)";

		try (PreparedStatement ps = con.prepareStatement(sql)) {
			ps.setInt(1, mautId);
			ps.setInt(2, abschnittId);
			ps.setInt(3, fzgId);
			ps.setInt(4, kategorieId);
			ps.setDate(5, new Date(System.currentTimeMillis()));
			ps.setDouble(6, kosten);
			ps.executeUpdate();
		}
	}

	/**
	 * Kategorie zur Kombination aus Schadstoffklasse und Achszahl.
	 */
	private int getKategorieId(Connection con, int ssklId, int achszahl) throws SQLException {
		String sql =
				"SELECT KATEGORIE_ID " +
						"FROM MAUTKATEGORIE " +
						"WHERE SSKL_ID = ? AND ACHSZAHL = ?";

		try (PreparedStatement ps = con.prepareStatement(sql)) {
			ps.setInt(1, ssklId);
			ps.setInt(2, achszahl);
			try (ResultSet rs = ps.executeQuery()) {
				if (!rs.next()) {
					throw new DataException("Keine passende Kategorie gefunden");
				}
				return rs.getInt("KATEGORIE_ID");
			}
		}
	}

	/**
	 * nächste freie MAUT_ID bestimmen.
	 */
	private int nextMautId(Connection con) throws SQLException {
		String sql = "SELECT MAX(MAUT_ID) AS MAX_ID FROM MAUTERHEBUNG";

		try (PreparedStatement ps = con.prepareStatement(sql);
			 ResultSet rs = ps.executeQuery()) {
			if (rs.next()) {
				int max = rs.getInt("MAX_ID");
				return max + 1;
			}
			return 1;
		}
	}

	/**
	 * offene Buchung auf "abgeschlossen" setzen.
	 *
	 * BUCHUNG.B_ID auf den Status "abgeschlossen" aus BUCHUNGSSTATUS setzen.
	 */
	private void closeBooking(Connection con, int buchungId) throws SQLException {
		int closedStatusId = getClosedStatusId(con);

		String sql = "UPDATE BUCHUNG SET B_ID = ? WHERE BUCHUNG_ID = ?";

		try (PreparedStatement ps = con.prepareStatement(sql)) {
			ps.setInt(1, closedStatusId);
			ps.setInt(2, buchungId);
			ps.executeUpdate();
		}
	}

	private int getClosedStatusId(Connection con) throws SQLException {
		String sql = "SELECT B_ID FROM BUCHUNGSSTATUS WHERE STATUS = 'abgeschlossen'";

		try (PreparedStatement ps = con.prepareStatement(sql);
			 ResultSet rs = ps.executeQuery()) {
			if (!rs.next()) {
				throw new DataException("Kein Status 'abgeschlossen' gefunden");
			}
			return rs.getInt("B_ID");
		}
	}


	private static class Vehicle {
		final int fzId;
		final int ssklId;
		final int achsen;
		final boolean hasActiveDevice;
		final int activeDeviceId;

		Vehicle(int fzId, int ssklId, int achsen,
				boolean hasActiveDevice, int activeDeviceId) {
			this.fzId = fzId;
			this.ssklId = ssklId;
			this.achsen = achsen;
			this.hasActiveDevice = hasActiveDevice;
			this.activeDeviceId = activeDeviceId;
		}
	}

	private static class Booking {
		final int buchungId;
		final int kategorieId;
		final int statusId; // B_ID

		Booking(int buchungId, int kategorieId, int statusId) {
			this.buchungId = buchungId;
			this.kategorieId = kategorieId;
			this.statusId = statusId;
		}
	}

	private static class BookingInfos {
		final Booking anyBooking;
		final Booking openBooking;

		BookingInfos(Booking anyBooking, Booking openBooking) {
			this.anyBooking = anyBooking;
			this.openBooking = openBooking;
		}
	}
}