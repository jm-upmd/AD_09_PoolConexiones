package dam.jmdh;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;

import org.apache.commons.dbcp2.BasicDataSource;


class HiloTransferencias implements Runnable {

  public final int NUM_TRANSF = 2000; // Número de transferencias realizadas
  public final double MIN_IMPORTE = 10;
  public final double MAX_IMPORTE = 200;

  private final BasicDataSource ds;
  private final String[] cuentas;
  private final String nomHilo;

  public HiloTransferencias(BasicDataSource ds, String[] cuentas, String nomHilo) {
    this.ds = ds;
    this.cuentas = cuentas;
    this.nomHilo = nomHilo;
  }

  // Transferencia de cuenta origen a cuenta destino en una transacción
  private synchronized boolean transferencia(String orig, String dest, double importe, Connection c) throws SQLException {

    boolean autoComm = c.getAutoCommit();  // para restaurarlo después

    try {

      c.setAutoCommit(false);

      int saldoOrig;
      try (PreparedStatement psSaldoOrig = c.prepareStatement("select saldo from cuentas where num_cuenta=?")) {
        psSaldoOrig.setString(1, orig);
        try (ResultSet rsSaldoOrig = psSaldoOrig.executeQuery()) {
          rsSaldoOrig.next();
          saldoOrig = rsSaldoOrig.getInt(1);
        }
      }

      if (saldoOrig < importe) {
        return false;
      }

      try (PreparedStatement psIncrSaldo = c.prepareStatement("update cuentas set saldo=saldo+? where num_cuenta=?")) {
        // Resta importe de la transferencia a la cuenta origen
        psIncrSaldo.setDouble(1, -importe);
        psIncrSaldo.setString(2, orig);
        psIncrSaldo.executeUpdate();

        // Suma importe a la cuenta destino
        psIncrSaldo.setDouble(1, importe);
        psIncrSaldo.setString(2, dest);
        psIncrSaldo.executeUpdate();

      }

      c.commit();

      return true;

    } catch (Exception ex) {
      try {
          // Si se produce algún error se restaura estado inicial de de ambas cuentas
        c.rollback();
      } catch (SQLException ex1) {
      }
      throw ex;   // Se propaga excepción una vez hecho rollback
    } finally {
      c.setAutoCommit(autoComm);  // Restaurar modo autocommmit anterior
    }
  }

  @Override
  public void run() {

    System.out.printf("[Hilo %s] comienza ejecución.\n", this.nomHilo);
// Cada hilo tiene su propia conexión que obtiene del pool y devuelve al final, cuando se hace close() con ella
    try (Connection c = ds.getConnection()) {

      Random r = new Random();

      int intentos = 0;

      for (int i = 0; i < NUM_TRANSF; i++) {

        int iC1, iC2;
        double importe;

        try {

          do {
            intentos++;

            // Elegir dos cuentas al azar y un importe al azar
            iC1 = r.nextInt(cuentas.length);
            do {
              iC2 = r.nextInt(cuentas.length);
            } while (iC1 == iC2);
            importe = r.nextDouble(MIN_IMPORTE, MAX_IMPORTE);
              System.out.printf("%s -> Realizando transferencia de cuenta %s a %s, IMPORTE: %f%n",this.nomHilo,cuentas[iC1],cuentas[iC2],importe);

          } while (!transferencia(cuentas[iC1], cuentas[iC2], importe, c));

        } catch (SQLException ex) {
          System.out.printf("[Hilo %s] ERROR: %s\n", this.nomHilo, ex.getMessage());
        }
      }
      System.out.printf("[Hilo %s] termina ejecución, %d intentos, %d transferencias.\n", this.nomHilo, intentos, NUM_TRANSF);
    } catch (SQLException exC) {
      System.out.printf("[Hilo %s] ERROR (No puede obtener conexión): %s\n", this.nomHilo, exC.getMessage());
    } finally {
      System.out.printf("[Hilo %s] libera conexión.\n", this.nomHilo);
    }
  }
}

public class JDBCPoolConexiones {

  public static void muestraErrorSQL(SQLException e) {
    System.out.printf("SQL ERROR mensaje: %s\n", e.getMessage());
    System.out.printf("SQL Estado: %s\n", e.getSQLState());
    System.out.printf("SQL código específico: %s\n", e.getErrorCode());
  }

  public static void main(String[] args) {

    String basedatos = "acceso_datos";
    String host = "localhost";
    String port = "3306";
    String parAdic = "";
    String urlConnection = "jdbc:mysql://" + host + ":" + port + "/" + basedatos + parAdic;
    String user = "root";
    String pwd = "12345";

    final BasicDataSource ds = new BasicDataSource();
    ds.setUrl(urlConnection);
    ds.setUsername(user);
    ds.setPassword(pwd);

    //Si se devuelve una conexión pool ya hay 50 disponibles entonces esa conexión se cierra.
    ds.setMinIdle(5); // num. mínimo de conexiones libres en el pool para asignación inmediata.
    ds.setMaxIdle(50); // num. máximo e conexiones libres en el pool para asignación inmediata.

    ds.setMaxOpenPreparedStatements(200); // Pool de sentencias preparadas

    final int NUM_HILOS = 50;

    String[] cuentas = {
      "CUENTA0", "CUENTA1", "CUENTA2", "CUENTA3", "CUENTA4", "CUENTA5",
      "CUENTA6", "CUENTA7", "CUENTA8", "CUENTA9"
    };
    String moneda = "EUR";
    double saldoInic = 4000;

    try (Connection c = ds.getConnection()) {

      // Crea cuentas si no existen
      try (PreparedStatement psCreaCuenta = c.prepareStatement(
              "insert into cuentas(num_cuenta,cod_moneda,saldo) values(?,?,?) on duplicate key update cod_moneda=?,saldo=?")) {
        for (String cuenta : cuentas) {
          int i = 1;
          psCreaCuenta.setString(i++, cuenta);
          psCreaCuenta.setString(i++, moneda);
          psCreaCuenta.setDouble(i++, saldoInic);
          psCreaCuenta.setString(i++, moneda);
          psCreaCuenta.setDouble(i++, saldoInic);
          psCreaCuenta.executeUpdate();
        }
      }

      // Ejecuta 50 hilos concurrentes, cada uno con un HiloTransferencias
      for (int i = 0; i < NUM_HILOS; i++) {
        Thread ht = new Thread(new HiloTransferencias(ds, cuentas, "H" + (i + 1)));
        ht.start();
      }

    } catch (SQLException ex) {
      muestraErrorSQL(ex);
    }
  }
}
