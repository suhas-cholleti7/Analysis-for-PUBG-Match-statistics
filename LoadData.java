/**
 * LoadData.java
 *
 * @author Santosh Kumar Nunna (sn7916@rit.edu)
 * @author Suhas Cholleti (sc3614@rit.edu)
 * This class contains the necessary methodology to load the data from the input .csv files to the 
 * tables created in the schema. We perform the data cleaning before inserting into the tables. 
 * If the plater dies due to bluezone of fall damage, he doesn't have a killer, such data is discarded. 
 * Some entries of the aggregate table doesn't have any player name associated with them, hence they are
 * discarded.
 */

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Scanner;


public class LoadData {
    // Creating hashmaps to store the player names data and game id to retrieve it later
    static HashMap<String, Integer> names = new HashMap<>();
    static HashMap<String, Integer> games = new HashMap<>();


    public static void loadAggregateMatchData(String[] args) {
        // Initializing the JDBC driver
        final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
        // Database URL using the given arguments
        final String DB_URL = args[0] + "&useUnicode=true&characterEncoding=UTF-8&user=" + args[1] + "&password=" + args[2];
        // Initializing the connections objects and statement variables
        Connection conn = null;
        PreparedStatement stmtGame = null;
        PreparedStatement stmtPlayer = null;
        PreparedStatement stmtPlayerStats = null;
        PreparedStatement stmtTeam = null;
        // Data files to be loaded
        String[] file_name = {"agg_match_stats_one_match.csv"};
        int name_counter = 1;
        int current_name_id;
        int game_counter = 1;
        int current_game_id;
        // Reading filepath and initializing connection to the database using the URL and arguments.
        String filepath = args[3];
        for (int j = 0; j < file_name.length; j++) {
            try (InputStream gzipStream = new FileInputStream(filepath + file_name[j]);
                 Scanner sc = new Scanner(gzipStream, "UTF-8")
            ) {
                // Connecting to database
                Class.forName("com.mysql.jdbc.Driver");
                System.out.println("Connecting to database...");
                conn = DriverManager.getConnection(DB_URL);

                // Initialize the commit object and set autocommit to False, we will manually commit data in chunks
                conn.setAutoCommit(false);
                sc.nextLine();// First Line

                // Insert statement creation with values to be executed for each table depending on the available data
                // and attributes.
                String insertStatmentGame = "INSERT INTO game(id, game_id, size, date_time, mode) "
                        + "VALUES(?,?,?,?,?)";
                String insertStatmentPlayer = "INSERT INTO player(id, name) "
                        + "VALUES(?,?)";
                String insertStatmentPlayerStats = "INSERT INTO playerstats(player_id, game_id, assists, revives, " +
                        "damage, kills, distance_walked, distance_ride, survive_time) "
                        + "VALUES(?,?,?,?,?,?,?,?,?)";
                String insertStatmentTeam = "INSERT INTO team(player_id, game_id, team_id, placement, size) "
                        + "VALUES(?,?,?,?,?)";
                // Initializing the prepare statements.
                stmtGame = conn.prepareStatement(insertStatmentGame);
                stmtPlayer = conn.prepareStatement(insertStatmentPlayer);
                stmtPlayerStats = conn.prepareStatement(insertStatmentPlayerStats);
                stmtTeam = conn.prepareStatement(insertStatmentTeam);
                int i = 0;
                // Parsing through each line in the input file
                while (sc.hasNextLine()) {
                    String currentLine = sc.nextLine();
                    String[] splitArray = currentLine.split(",");
                    if (splitArray[11].length() == 0) {
                        i++;
                        continue;
                    }
                    // Checking if the name is already present in the map
                    if (!names.containsKey(splitArray[11])) {
                        stmtPlayer.setInt(1, name_counter);
                        stmtPlayer.setString(2, splitArray[11]);
                        stmtPlayer.addBatch();
                        names.put(splitArray[11], name_counter);
                        current_name_id = name_counter;
                        name_counter++;
                    } else {
                        current_name_id = names.get(splitArray[11]);
                    }
                    // Checking if the game id is already present in the map
                    if (!games.containsKey(splitArray[2])) {
                        stmtGame.setInt(1, game_counter);
                        stmtGame.setString(2, splitArray[2]);
                        stmtGame.setInt(3, Integer.parseInt(splitArray[1]));
                        String date = splitArray[0].replace("T", " ");
                        date = date.replace("+", ".");
                        stmtGame.setTimestamp(4, java.sql.Timestamp.valueOf(date));
                        stmtGame.setString(5, splitArray[3]);
                        stmtGame.addBatch();
                        games.put(splitArray[2], game_counter);
                        current_game_id = game_counter;
                        game_counter++;
                    } else {
                        current_game_id = games.get(splitArray[2]);
                    }
                    // Inserting data for the playerstats table
                    stmtPlayerStats.setInt(1, current_name_id);
                    stmtPlayerStats.setInt(2, current_game_id);
                    stmtPlayerStats.setInt(3, Integer.parseInt(splitArray[5]));
                    stmtPlayerStats.setInt(4, Integer.parseInt(splitArray[6]));
                    stmtPlayerStats.setInt(5, Integer.parseInt(splitArray[9]));
                    stmtPlayerStats.setInt(6, Integer.parseInt(splitArray[10]));
                    stmtPlayerStats.setDouble(7, Double.parseDouble(splitArray[8]));
                    stmtPlayerStats.setDouble(8, Double.parseDouble(splitArray[7]));
                    stmtPlayerStats.setDouble(9, Double.parseDouble(splitArray[12]));
                    stmtPlayerStats.addBatch();

                    // Inserting data for the team table
                    stmtTeam.setInt(1, current_name_id);
                    stmtTeam.setInt(2, current_game_id);
                    stmtTeam.setInt(3, Integer.parseInt(splitArray[13]));
                    stmtTeam.setInt(4, Integer.parseInt(splitArray[14]));
                    stmtTeam.setInt(5, Integer.parseInt(splitArray[4]));
                    stmtTeam.addBatch();
                    if (i % 100 == 0) {
                        System.out.println(i);
                        stmtPlayer.executeBatch();
                        stmtGame.executeBatch();
                        stmtPlayerStats.executeBatch();
                        stmtTeam.executeBatch();
                        conn.commit();
                    }
                    i++;
                }
                // After gathering a minimal or certain chunk of data into the cache we commit all of them using the
                // execute batch option.
                stmtPlayer.executeBatch();
                stmtGame.executeBatch();
                stmtPlayerStats.executeBatch();
                stmtTeam.executeBatch();
                // Once the data is executed or pushed, we commit the data
                conn.commit();
                // Closing the connection after the final commit
                conn.close();
            // Checking for error to trace back to point of issue
            } catch (Exception se) {
                se.printStackTrace();
            // Close the statement objects or flush them if they are not emptied to prevent unnecessary duplicate and
            // dangling pointers
            } finally {
                try {
                    if (stmtPlayer != null)
                        stmtPlayer.close();
                    if (stmtGame != null)
                        stmtGame.close();
                    if (stmtPlayerStats != null)
                        stmtPlayerStats.close();
                    if (stmtTeam != null)
                        stmtTeam.close();
                    if (conn != null)
                        conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    public static void loadKillsMatchData(String[] args) {
        // Initializing the JDBC driver
        final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
        // Database URL using the given arguments
        final String DB_URL = args[0] + "&useUnicode=true&characterEncoding=UTF-8&user=" + args[1] + "&password=" + args[2];
        // Initializing the connections objects and statement variables
        Connection conn = null;
        PreparedStatement stmtMap = null;
        PreparedStatement stmtGame = null;
        PreparedStatement stmtWeapon = null;
        PreparedStatement stmtKills = null;
        // Reading filepath and initializing connection to the database using the URL and arguments.
        String filepath = args[3];
        // Data files to be loaded
        String[] file_name = {"kill_match_stats_one_match.csv"};
        int map_counter = 1;
        HashMap<String, Integer> maps = new HashMap<>();
        int current_map_id;
        int weapon_counter = 1;
        HashMap<String, Integer> weapons = new HashMap<>();
        int current_weapon_id;
        int kill_id = 1;
        for (int j = 0; j < file_name.length; j++) {
            try (InputStream gzipStream = new FileInputStream(filepath + file_name[j]);
                 Scanner sc = new Scanner(gzipStream, "UTF-8")
            ) {
                // Registering the jdbc driver connection
                Class.forName("com.mysql.jdbc.Driver");

                // Connecting to database
                System.out.println("Connecting to database...");
                conn = DriverManager.getConnection(DB_URL);
                // Initialize the commit object and set autocommit to False, we will manually commit data in chunks
                conn.setAutoCommit(false);
                sc.nextLine();// First Line
                // Insert statement creation with values to be executed for each table depending on the available data
                // and attributes.
                String insertStatmentMap = "INSERT INTO map(id, name) "
                        + "VALUES(?,?)";
                String updateStatmentGame = "UPDATE game SET map_id=? where id=?";
                String insertStatmentWeapon = "INSERT INTO weapon(id, name) "
                        + "VALUES(?,?)";
                String insertStatmentKills = "INSERT INTO kills(id, game_id, killed_by, victim, killer, killer_position_x," +
                        " killer_position_y, victim_position_x, victim_position_y) "
                        + "VALUES(?,?,?,?,?,?,?,?,?)";
                // Initializing the prepare statements.
                stmtMap = conn.prepareStatement(insertStatmentMap);
                stmtGame = conn.prepareStatement(updateStatmentGame);
                stmtWeapon = conn.prepareStatement(insertStatmentWeapon);
                stmtKills = conn.prepareStatement(insertStatmentKills);
                int i = 0;
                // Parsing through each line in the input file
                while (sc.hasNextLine()) {
                    String currentLine = sc.nextLine();
                    String[] splitArray = currentLine.split(",");
                    if (splitArray[3].length() == 0 || !names.containsKey(splitArray[1]) || !names.containsKey(splitArray[8])
                            || !games.containsKey(splitArray[6])) {
                        i++;
                        continue;
                    }

                    // Checking if the map id is already present in the hash map
                    if (!maps.containsKey(splitArray[5])) {
                        stmtMap.setInt(1, map_counter);
                        stmtMap.setString(2, splitArray[5]);
                        stmtMap.addBatch();
                        maps.put(splitArray[5], map_counter);
                        current_map_id = map_counter;
                        map_counter++;
                    } else {
                        current_map_id = maps.get(splitArray[5]);
                    }
                    // Updating the the game table with the map id
                    stmtGame.setInt(1, current_map_id);
                    stmtGame.setInt(2, games.get(splitArray[6]));
                    // Once the data is assigned to all the attributes add the object to batch
                    stmtGame.addBatch();

                    // Checking if the map id is already present in the hash map
                    if (!weapons.containsKey(splitArray[0])) {
                        stmtWeapon.setInt(1, weapon_counter);
                        stmtWeapon.setString(2, splitArray[0]);
                        stmtWeapon.addBatch();
                        weapons.put(splitArray[0], weapon_counter);
                        current_weapon_id = weapon_counter;
                        weapon_counter++;
                    } else {
                        current_weapon_id = weapons.get(splitArray[0]);
                    }
                    // Inserting data for the kills table
                    stmtKills.setInt(1, kill_id);
                    stmtKills.setInt(2, games.get(splitArray[6]));
                    stmtKills.setInt(3, current_weapon_id);
                    stmtKills.setInt(4, names.get(splitArray[8]));
                    stmtKills.setInt(5, names.get(splitArray[1]));
                    stmtKills.setFloat(6, Float.parseFloat(splitArray[3]));
                    stmtKills.setFloat(7, Float.parseFloat(splitArray[4]));
                    stmtKills.setFloat(8, Float.parseFloat(splitArray[10]));
                    stmtKills.setFloat(9, Float.parseFloat(splitArray[11]));
                    stmtKills.addBatch();
                    kill_id++;

                    // Checking for every multiple of 100 and inserting the data into the tables.
                    if (i % 100 == 0) {
                        stmtMap.executeBatch();
                        stmtGame.executeBatch();
                        stmtWeapon.executeBatch();
                        stmtKills.executeBatch();
                        // Commit data once insertion is finished else the data will not be saved.
                        conn.commit();
                    }
                    i++;
                }
                // Final commit and batch execution
                stmtMap.executeBatch();
                stmtGame.executeBatch();
                stmtWeapon.executeBatch();
                stmtKills.executeBatch();
                // Commit the batch and close the connection to the database.
                conn.commit();
                conn.close();
            // Checking for error to trace back to point of issue
            } catch (Exception se) {
                se.printStackTrace();
            // Close the statement objects or flush them if they are not emptied to prevent unnecessary duplicate and
            // dangling pointers
            } finally {
                try {
                    if (stmtMap != null)
                        stmtMap.close();
                    if (stmtGame != null)
                        stmtGame.close();
                    if (stmtWeapon != null)
                        stmtWeapon.close();
                    if (stmtKills != null)
                        stmtKills.close();
                    if (conn != null)
                        conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }


}