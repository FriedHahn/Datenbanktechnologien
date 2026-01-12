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
import org.dbunit.database.DatabaseConfig;
import org.dbunit.ext.oracle.OracleDataTypeFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static org.junit.Assert.*;

/**
 * Testet die DAO/TDG-basierte Implementierung des Mautservice.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MautServiceImplDaoTest {

    private static final Logger L = LoggerFactory.getLogger(MautServiceImplDaoTest.class);
    private static IDatabaseConnection dbTesterCon = null;

    // WICHTIG: Hier wird die DAO-Variante getestet
    private static final IMautService maut = new MautServiceImplDao();

    @BeforeClass
    public static void setUp() {
        L.debug("setup: start");
        try {
            IDatabaseTester dbTester = new JdbcDatabaseTester(
                    DbCred.driverClass, DbCred.url, DbCred.user, DbCred.password, DbCred.schema
            );
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

    @org.junit.Test(expected = UnkownVehicleException.class)
    public void testMauterhebung_1() {
        maut.berechneMaut(1200, 4, "LDS 677");
    }

    @org.junit.Test(expected = InvalidVehicleDataException.class)
    public void testMauterhebung_2() {
        maut.berechneMaut(1200, 4, "HH 8499");
    }

    @org.junit.Test(expected = InvalidVehicleDataException.class)
    public void testMauterhebung_3() {
        maut.berechneMaut(1200, 3, "B CV 8890");
    }

    @org.junit.Test(expected = AlreadyCruisedException.class)
    public void testMauterhebung_4() {
        maut.berechneMaut(4174, 10, "DV 9413 NJ");
    }

    @org.junit.Test
    public void testMauterhebung_5() throws Exception {
        maut.berechneMaut(1200, 4, "B CV 8890");

        QueryDataSet databaseDataSet = new QueryDataSet(dbTesterCon);
        String sql = "select * from BUCHUNG order by BUCHUNG_ID asc";
        databaseDataSet.addTable("BUCHUNG", sql);
        ITable actualTable = databaseDataSet.getTable("BUCHUNG");

        assertEquals("Die Buchung ist nicht auf abgeschlossen gesetzt worden", "3",
                actualTable.getValue(3, "B_ID").toString());
    }

    @org.junit.Test
    public void testMauterhebung_6() throws Exception {
        maut.berechneMaut(1433, 5, "M 6569");

        try {
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
