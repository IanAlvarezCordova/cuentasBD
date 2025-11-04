import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class GeneradorCuentasBD {

    private static final String URL = "jdbc:mysql://localhost:3306/tu_base_de_datos";
    private static final String USER = "root";
    private static final String PASS = "tu_password";

    private static final Random random = new Random();

    public static void main(String[] args) {
        try (Connection conn = DriverManager.getConnection(URL, USER, PASS)) {
            System.out.println("Conexión exitosa a la BD.");

            // 1. Cargar clientes desde BD
            List<Long> clientesIds = cargarClientesDesdeBD(conn);
            System.out.println("Clientes encontrados: " + clientesIds.size());

            // 2. Generar cuentas con distribución
            List<Cuenta> cuentas = generarCuentasConDistribucion(clientesIds);

            // 3. Insertar cuentas en BD
            insertarCuentasEnBD(conn, cuentas);

            // 4. Mostrar resumen
            mostrarResumenCuentas(conn);

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // --- 1. Cargar solo los IDs de clientes ---
    private static List<Long> cargarClientesDesdeBD(Connection conn) throws SQLException {
        List<Long> ids = new ArrayList<>();
        String sql = "SELECT id FROM cliente ORDER BY id";
        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                ids.add(rs.getLong("id"));
            }
        }
        return ids;
    }

    // --- 2. Generar cuentas con distribución ---
    private static List<Cuenta> generarCuentasConDistribucion(List<Long> clientesIds) {
        List<Cuenta> cuentas = new ArrayList<>();
        int total = clientesIds.size();

        // Calcular cuántos clientes por grupo
        int con2 = (int) (total * 0.45);
        int con3 = (int) (total * 0.15);
        int con4 = (int) (total * 0.05);
        int con1 = total - (con2 + con3 + con4);

        // Dividir clientes en grupos
        int idx = 0;
        List<Long> grupo1 = clientesIds.subList(idx, idx += con1);
        List<Long> grupo2 = clientesIds.subList(idx, idx += con2);
        List<Long> grupo3 = clientesIds.subList(idx, idx += con3);
        List<Long> grupo4 = clientesIds.subList(idx, idx + con4);

        long cuentaId = obtenerProximoIdCuenta() + 1; // para evitar colisiones

        cuentas.addAll(crearCuentasParaGrupo(grupo1, 1, cuentaId));
        cuentaId += grupo1.size();

        cuentas.addAll(crearCuentasParaGrupo(grupo2, 2, cuentaId));
        cuentaId += grupo2.size() * 2;

        cuentas.addAll(crearCuentasParaGrupo(grupo3, 3, cuentaId));
        cuentaId += grupo3.size() * 3;

        cuentas.addAll(crearCuentasParaGrupo(grupo4, 4, cuentaId));

        return cuentas;
    }

    private static List<Cuenta> crearCuentasParaGrupo(List<Long> clientes, int numCuentas, long inicioId) {
        List<Cuenta> cuentas = new ArrayList<>();
        long id = inicioId;
        for (Long idCliente : clientes) {
            for (int i = 0; i < numCuentas; i++) {
                double saldo = random.nextDouble() * 7000; // o 7000 fijo
                cuentas.add(new Cuenta(id++, idCliente, saldo));
            }
        }
        return cuentas;
    }

    // --- 3. Insertar cuentas en BD ---
    private static void insertarCuentasEnBD(Connection conn, List<Cuenta> cuentas) throws SQLException {
        String sql = "INSERT INTO cuenta (id, id_cliente, saldo) VALUES (?, ?, ?)";
        conn.setAutoCommit(false); // transacción

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            int batchSize = 1000;
            int count = 0;

            for (Cuenta c : cuentas) {
                ps.setLong(1, c.id);
                ps.setLong(2, c.idCliente);
                ps.setDouble(3, c.saldo);
                ps.addBatch();

                if (++count % batchSize == 0) {
                    ps.executeBatch();
                }
            }
            ps.executeBatch(); // último batch
            conn.commit();
            System.out.println("Insertadas " + cuentas.size() + " cuentas en la BD.");
        } catch (SQLException e) {
            conn.rollback();
            throw e;
        } finally {
            conn.setAutoCommit(true);
        }
    }

    // --- 4. Resumen: group by cantidad de cuentas por cliente ---
    private static void mostrarResumenCuentas(Connection conn) throws SQLException {
        String sql = """
            SELECT cantidad_cuentas, COUNT(*) as total_clientes
            FROM (
                SELECT id_cliente, COUNT(*) as cantidad_cuentas
                FROM cuenta
                GROUP BY id_cliente
            ) sub
            GROUP BY cantidad_cuentas
            ORDER BY cantidad_cuentas
            """;

        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            System.out.println("\n=== RESUMEN: CLIENTES POR NÚMERO DE CUENTAS ===");
            while (rs.next()) {
                int cant = rs.getInt("cantidad_cuentas");
                long total = rs.getLong("total_clientes");
                System.out.println("Clientes con " + cant + " cuenta(s): " + total);
            }
        }
    }

    // --- Obtener el máximo id de cuenta para evitar colisiones ---
    private static long obtenerProximoIdCuenta() {
        try (Connection conn = DriverManager.getConnection(URL, USER, PASS);
             PreparedStatement ps = conn.prepareStatement("SELECT COALESCE(MAX(id), 0) FROM cuenta");
             ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getLong(1) : 0;
        } catch (SQLException e) {
            return 0;
        }
    }

    // Clase Cuenta (solo para memoria)
    static class Cuenta {
        final long id;
        final long idCliente;
        final double saldo;

        Cuenta(long id, long idCliente, double saldo) {
            this.id = id;
            this.idCliente = idCliente;
            this.saldo = saldo;
        }
    }
}
