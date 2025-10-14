package de.htwberlin.dbtech.aufgaben.ue02;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void registerVehicle(long fz_id, int sskl_id, int nutzer_id, String kennzeichen, String fin, int achsen,
			int gewicht, String zulassungsland) {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateStatusForOnBoardUnit(long fzg_id, String status) {
		// TODO Auto-generated method stub

	}

	@Override
	public void deleteVehicle(long fz_id) {
		// TODO Auto-generated method stub

	}

	@Override
	public List<Mautabschnitt> getTrackInformations(String abschnittstyp) {
		// TODO Auto-generated method stub
		return null;
	}

}
