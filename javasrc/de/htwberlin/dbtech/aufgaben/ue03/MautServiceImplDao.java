package de.htwberlin.dbtech.aufgaben.ue03;

import de.htwberlin.dbtech.aufgaben.ue03.tdg.*;
import de.htwberlin.dbtech.exceptions.*;

import java.math.BigDecimal;
import java.sql.Connection;
import java.time.LocalDate;
import java.util.Map;

public class MautServiceImplDao implements IMautService {

    private Connection connection;

    private FahrzeugDao fahrzeugDao;
    private BuchungDao buchungDao;
    private MautabschnittDao mautabschnittDao;
    private MautkategorieDao mautkategorieDao;
    private MauterhebungDao mauterhebungDao;

    @Override
    public void setConnection(Connection connection) {
        this.connection = connection;
        this.fahrzeugDao = new FahrzeugTdg(connection);
        this.buchungDao = new BuchungTdg(connection);
        this.mautabschnittDao = new MautabschnittTdg(connection);
        this.mautkategorieDao = new MautkategorieTdg(connection);
        this.mauterhebungDao = new MauterhebungTdg(connection);
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

        getConnection();

        boolean istAutomatisch = fahrzeugDao.istAutoRegistriert(kennzeichen);
        boolean istManuell = buchungDao.istManuellRegistriert(mautAbschnitt, kennzeichen);

        if (!istAutomatisch && !istManuell) {
            throw new UnkownVehicleException("Fahrzeug nicht registriert");
        }

        boolean istAchszahlRichtig = pruefeAchszahl(mautAbschnitt, achszahl, kennzeichen,
                istAutomatisch, istManuell);

        boolean istBuchungAbgeschlossen =
                buchungDao.istBuchungAbgeschlossen(mautAbschnitt, kennzeichen);

        if (!istAchszahlRichtig) {
            throw new InvalidVehicleDataException("Fahrzeugdaten (Achszahl) sind nicht korrekt");
        }

        if (istManuell) {
            if (istBuchungAbgeschlossen) {
                throw new AlreadyCruisedException("Strecke wurde bereits befahren");
            }
            buchungDao.setzeBuchungAbgeschlossen(kennzeichen, mautAbschnitt);
        }

        if (istAutomatisch) {
            autoMautberechnung(achszahl, kennzeichen, mautAbschnitt);
        }
    }

    private boolean pruefeAchszahl(int mautAbschnitt, int achszahl, String kennzeichen,
                                   boolean istAutomatisch, boolean istManuell) {

        if (istAutomatisch) {
            int ssklId = fahrzeugDao.ermittleSchadstoffklasseId(kennzeichen, achszahl);
            return ssklId != 0;
        }

        if (istManuell) {
            String regel = buchungDao.ermittleAchszahlRegel(mautAbschnitt, kennzeichen);
            return regel != null && passtZurAchszahl(regel, achszahl);
        }

        return false;
    }

    private void autoMautberechnung(int achszahl, String kennzeichen, int mautAbschnitt) {

        long laengeMeter = mautabschnittDao.ermittleLaengeInMetern(mautAbschnitt);

        int ssklId = fahrzeugDao.ermittleSchadstoffklasseId(kennzeichen, achszahl);

        Map.Entry<Integer, BigDecimal> mp =
                mautkategorieDao.berechneMautpreisFuerFahrzeug(ssklId, achszahl, laengeMeter);

        int kategorieId = mp.getKey();
        BigDecimal preis = mp.getValue();

        long fzgId = fahrzeugDao.ermittleFzgId(kennzeichen, achszahl, ssklId);

        int mautId = mauterhebungDao.ermittleNaechsteMautId();

        mauterhebungDao.fuegeMauterhebungEin(
                mautId,
                mautAbschnitt,
                fzgId,
                kategorieId,
                LocalDate.now(),
                preis
        );
    }

    public static boolean passtZurAchszahl(String regel, int achsen) {
        regel = regel.trim();
        if (regel.startsWith(">=")) {
            int val = Integer.parseInt(regel.substring(2).trim());
            return achsen >= val;
        }
        if (regel.startsWith("=")) {
            int val = Integer.parseInt(regel.substring(1).trim());
            return achsen == val;
        }
        throw new IllegalArgumentException("Unbekannte Regel in ACHSZAHL: " + regel);
    }
}
