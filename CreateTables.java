/**
 * CreateTables.java
 *
 * @author Santosh Kumar Nunna (sn7916@rit.edu)
 * @author Suhas Cholleti (sc3614@rit.edu)
 * This class implements the required methodology to create the tables as per the ER diagram. The necessary structure,
 * primary, foreign keys assignment to the attrubutes are all set in here.
 */

import java.sql.*;

public class CreateTables {

    public static void createTables(String[] args) {
        // Load the JDBC driver and formulate the database URL from the given input arguments.
        final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
        final String DB_URL = args[0] + "&useUnicode=true&characterEncoding=UTF-8&user=" + args[1] + "&password=" + args[2];

        // Initialize the connection and statement objects
        Connection conn = null;
        Statement stmt = null;
        try {
            // Registering the jdbc driver
            Class.forName("com.mysql.jdbc.Driver");

            // Initiating the connection the database with the supplied URL
            System.out.println("Connecting to database...");
            conn = DriverManager.getConnection(DB_URL);

            // Creating input statement
            stmt = conn.createStatement();
            // SQL query to create the Player table
            String sql = "CREATE TABLE Player " +
                    "(id INTEGER not NULL, " +
                    " name VARCHAR(255), " +
                    " PRIMARY KEY ( id ))";
            // Executing the sql query and closing the statement object
            stmt.executeUpdate(sql);
            stmt.close();

            // SQL query to create the Map table
            stmt = conn.createStatement();
            sql = "CREATE TABLE Map " +
                    "(id INTEGER not NULL, " +
                    " name VARCHAR(255), " +
                    " PRIMARY KEY ( id ))";
            // Executing the sql query and closing the statement object
            stmt.executeUpdate(sql);
            stmt.close();

            // SQL query to create the Weapon table
            stmt = conn.createStatement();
            sql = "CREATE TABLE Weapon " +
                    "(id INTEGER not NULL, " +
                    " name VARCHAR(255), " +
                    " PRIMARY KEY ( id ))";
            // Executing the sql query and closing the statement object
            stmt.executeUpdate(sql);
            stmt.close();

            // SQL query to create the Game table
            stmt = conn.createStatement();
            sql = "CREATE TABLE Game " +
                    "(id INTEGER not NULL, " +
                    " game_id VARCHAR(255), " +
                    " size INTEGER, " +
                    " date_time DATETIME , " +
                    " mode VARCHAR(5) , " +
                    " map_id INTEGER, " +
                    " PRIMARY KEY ( id )," +
                    " FOREIGN KEY (map_id) references Map(id))";
            // Executing the sql query and closing the statement object
            stmt.executeUpdate(sql);
            stmt.close();


            // SQL query to create the Team table
            stmt = conn.createStatement();
            sql = "CREATE TABLE Team " +
                    "(player_id INTEGER not NULL, " +
                    " game_id INTEGER not NULL, " +
                    " team_id INTEGER, " +
                    " placement INTEGER, " +
                    " size INTEGER, " +
                    " PRIMARY KEY ( player_id, game_id )," +
                    " FOREIGN KEY (player_id) references Player(id), " +
                    " FOREIGN KEY (game_id) references Game(id))";
            // Executing the sql query and closing the statement object
            stmt.executeUpdate(sql);
            stmt.close();


            // SQL query to create the PlayerStats table
            stmt = conn.createStatement();
            sql = "CREATE TABLE PlayerStats " +
                    "(player_id INTEGER not NULL, " +
                    " game_id INTEGER not NULL, " +
                    " assists INTEGER , " +
                    " revives INTEGER, " +
                    " damage INTEGER, " +
                    " kills INTEGER, " +
                    " distance_walked FLOAT, " +
                    " distance_ride FLOAT, " +
                    " survive_time FLOAT, " +
                    " PRIMARY KEY ( player_id, game_id )," +
                    " FOREIGN KEY (player_id) references Player(id), " +
                    " FOREIGN KEY (game_id) references Game(id))";
            // Executing the sql query and closing the statement object
            stmt.executeUpdate(sql);
            stmt.close();

            // SQL query to create the Kills table
            stmt = conn.createStatement();
            sql = "CREATE TABLE kills " +
                    "(id INTEGER not NULL, " +
                    " game_id INTEGER, " +
                    " killed_by INTEGER , " +
                    " victim INTEGER, " +
                    " killer INTEGER, " +
                    " killer_position_x FLOAT, " +
                    " killer_position_y FLOAT, " +
                    " victim_position_x FLOAT, " +
                    " victim_position_y FLOAT, " +
                    " PRIMARY KEY (id)," +
                    " FOREIGN KEY (killed_by) references Weapon(id), " +
                    " FOREIGN KEY (victim) references Player(id), " +
                    " FOREIGN KEY (killer) references Player(id), " +
                    " FOREIGN KEY (game_id) references Game(id))";
            // Executing the sql query and closing the statement object
            stmt.executeUpdate(sql);
            stmt.close();

            // Closing the database connection
            conn.close();
        } catch (Exception se) {
            se.printStackTrace();
        } finally {
            // Closing all the statement objects if they are still active
            try {
                if (stmt != null)
                    stmt.close();
                if (conn != null)
                    conn.close();
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }
    }
}