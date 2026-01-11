package de.htwberlin.dbtech.aufgaben.ue03;

import de.htwberlin.dbtech.exceptions.AlreadyCruisedException;
import de.htwberlin.dbtech.exceptions.InvalidVehicleDataException;
import de.htwberlin.dbtech.exceptions.UnkownVehicleException;
import de.htwberlin.dbtech.utils.DbCred;
import de.htwberlin.dbtech.utils.DbUnitUtils;
import org.dbunit.IDatabaseTester;
import org.dbunit.JdbcDatabaseTester;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.database.QueryDataSet;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.RowOutOfBoundsException;
import org.dbunit.dataset.csv.CsvDataSet;
import org.dbunit.operation.DatabaseOperation;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.ext.oracle.OracleDataTypeFactory;

import java.io.File;

import static org.junit.Assert.*;

/**
 * Die Klasse enthaelt die Testfaelle fuer die Methoden des Mautservice
 *
 * @author Patrick Dohmeier
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MautServiceTest {
    private static final Logger L = LoggerFactory.getLogger(MautServiceTest.class);
    private static IDatabaseConnection dbTesterCon = null;

    private static final IMautService maut = new MautServiceImpl();

    @BeforeClass
    public static void setUp() {
        L.debug("setup: start");
        try {
            IDatabaseTester dbTester = new JdbcDatabaseTester(DbCred.driverClass, DbCred.url, DbCred.user, DbCred.password,
                    DbCred.schema);
            dbTesterCon = dbTester.getConnection();
            dbTesterCon.getConfig().setProperty(DatabaseConfig.PROPERTY_DATATYPE_FACTORY, new OracleDataTypeFactory());
            IDataSet pre = new CsvDataSet(new File("test-data/ue03-04"));
            dbTester.setDataSet(pre);
            DatabaseOperation.CLEAN_INSERT.execute(dbTesterCon, pre);
            maut.setConnection(dbTesterCon.getConnection());
        } catch (Exception e) {
            DbUnitUtils.closeDbUnitConnectionQuietly(dbTesterCon);
            throw new RuntimeException(e);
        }
    }

    @AfterClass
    public static void tearDown() {
        L.debug("tearDown: start");
        DbUnitUtils.closeDbUnitConnectionQuietly(dbTesterCon);
    }


    /**
     * Der Testfall testet die Mauterhebung eines Fahrzeuges im Fehlerfall.
     */
    @org.junit.Test(expected = UnkownVehicleException.class)
    public void testMauterhebung_1() {
        // Das Fahrzeug mit dem Kennzeichen LDS 677 ist nicht registriert bzw.
        // nicht aktiv und hat auch kein Fahrzeuggeraet verbaut. Es
        // liegt auch keine offenen Buchung im Manuellen Verfahren vor. Das
        // unbekannte Fahrzeug fuehrt zu einer UnkownVehicleException.
        maut.berechneMaut(1200, 4, "LDS 677");
    }

    /**
     * Der Testfall testet die Mauterhebung eines Fahrzeuges im Fehlerfall.
     */
    @org.junit.Test(expected = InvalidVehicleDataException.class)
    public void testMauterhebung_2() {
        // Das Fahrzeug mit dem Kennzeichen HH 8499 ist bekannt und im
        // automatischen System unterwegs. Das Fahrzeug faehrt mit einer hoeheren
        // Achszahl als in der Datenbank registriert. In der Mautberechnung
        // fuehrt das zu einer niedrigeren Mauterhebung je Kilometer und zu einer
        // InvalidVehicleDataException.
        maut.berechneMaut(1200, 4, "HH 8499");
    }

    /**
     * Der Testfall testet die Mauterhebung eines Fahrzeuges im Fehlerfall.
     */
    @org.junit.Test(expected = InvalidVehicleDataException.class)
    public void testMauterhebung_3() {
        // Das Fahrzeug ist bekannt und im Manuellen Verfahren unterwegs.
        // Gebucht wurde aber eine falsche Mautkategorie, sprich eine falsche
        // Achszahl. Der Fahrer bezahlt also weniger Maut fuer die
        // Streckenbefahrung als er muesste. Dies fuehrt zu einer
        // InvalidVehicleDataException.
        maut.berechneMaut(1200, 3, "B CV 8890");
    }

    /**
     * Der Testfall testet die Mauterhebung eines Fahrzeuges im Fehlerfall.
     */
    @org.junit.Test(expected = AlreadyCruisedException.class)
    public void testMauterhebung_4() {
        // Das Fahrzeug ist bekannt und im manuellen Verfahren unterwegs.
        // Auch die Mautkategorie stimmt. Allerdings fuehrt eine Doppelbefahrung
        // bei Einmalbuchung der gleichen Strecke zu einer
        // AlreadyCruisedException
        maut.berechneMaut(4174, 10, "DV 9413 NJ");
    }

    /**
     * Der Testfall testet die Mauterhebung eines Fahrzeuges im Erfolgsfall.
     *
     */
    @org.junit.Test
    public void testMauterhebung_5() throws Exception {
        // Das Fahrzeug ist bekannt und im manuellen Verfahren unterwegs.
        // Die Streckenbefahrung ist noch im Status offen, und
        // die Kontrolldaten stimmen mit den gebuchten Daten ueberein.
        // Der Buchungsstatus der Streckenbefahrung wird auf "abgeschlossen"
        // gesetzt
        maut.berechneMaut(1200, 4, "B CV 8890");

        // hole Daten aus der aktuellen Tabelle BUCHUNG
        QueryDataSet databaseDataSet = new QueryDataSet(dbTesterCon);
        String sql = "select * from BUCHUNG order by BUCHUNG_ID asc";
        databaseDataSet.addTable("BUCHUNG", sql);
        ITable actualTable = databaseDataSet.getTable("BUCHUNG");

        assertEquals("Die Buchung ist nicht auf abgeschlossen gesetzt worden", "3",
                actualTable.getValue(3, "B_ID").toString());
    }

    /**
     * Der Testfall testet die Mauterhebung eines Fahrzeuges im Erfolgsfall.
     *
     */
    @org.junit.Test
    public void testMauterhebung_6()  throws Exception {
        // Das Fahrzeug ist bekannt & aktiv, sowie mit einem Fahrzeuggeraet
        // ausgestattet und im Automatischen Verfahren unterwegs.
        // Die Hoehe der Maut wird anhand des gefahrenen Abschnitts und der
        // Mautkategorie berechnet und verbucht.
        // Achtung, die Id der neuen Mauterhebung muss so gewaehlt werden, dass sie groesser
        // als der bisherige maximale Wert ist.
        maut.berechneMaut(1433, 5, "M 6569");

        try {
            // hole Daten aus der aktuellen Tabelle MAUTERHEBUNG
            QueryDataSet databaseDataSet = new QueryDataSet(dbTesterCon);
            String sql = "select * from MAUTERHEBUNG order by maut_id asc";
            databaseDataSet.addTable("MAUTERHEBUNG", sql);
            ITable actualTable = databaseDataSet.getTable("MAUTERHEBUNG");

            assertEquals("Die Berechnung der Maut war nicht korrekt", "0.68",
                    actualTable.getValue(18, "KOSTEN").toString().replace(',', '.'));
        } catch (RowOutOfBoundsException e) {
            fail("Es wurde keine Mauterhebung im Automatischen Verfahren gespeichert");
        }
    }

}
