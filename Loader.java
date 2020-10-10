/**
 * Loader.java
 *
 * @author Santosh Kumar Nunna (sn7916@rit.edu)
 * @author Suhas Cholleti (sc3614@rit.edu)
 * This file is the main class file. The loader class receives the input arguments and calls the necessary functions
 * in order of required execution.
 */
public class Loader {
    public static void main(String args[]) {
        // Calling the createTables method
        CreateTables.createTables(args);
        // Calling the loadAggregateMatchData method, loads data into game, team, player and playerstats table
        LoadData.loadAggregateMatchData(args);
        // Calling the loadKillsMatchData method, loads data into map, kills and weapon table.
        LoadData.loadKillsMatchData(args);
    }
}