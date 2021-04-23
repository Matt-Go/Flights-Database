import java.io.FileInputStream;
import java.sql.*;
import java.util.Properties;

/**
 * Runs queries against a back-end database.
 * This class is responsible for searching for flights.
 */
public class QuerySearchOnly
{
  // `dbconn.properties` config file
  private String configFilename;

  // DB Connection
  protected Connection conn;

  // Canned queries
  private static final String CHECK_FLIGHT_CAPACITY = "SELECT capacity FROM Flights WHERE fid = ?";
  protected PreparedStatement checkFlightCapacityStatement;

  private static final String DIRECT_SEARCH_SQL =
          "SELECT TOP (?) day_of_month,carrier_id,flight_num,origin_city,dest_city,actual_time,fid,capacity,price,canceled "
                  + "FROM Flights "
                  + "WHERE origin_city = ? AND dest_city = ? AND day_of_month = ? "
                  + "AND canceled = 0 "
                  + "ORDER BY actual_time ASC, fid ASC";
  protected PreparedStatement directSearchStatement;

  private static final String INDIRECT_SEARCH_SQL =
          "SELECT TOP (?) F1.day_of_month,F1.carrier_id,F1.flight_num,F1.origin_city,F1.dest_city,F1.actual_time,F1.fid,F1.capacity,F1.price,F1.canceled, "
                  + "F2.day_of_month as day2,F2.carrier_id as c2,F2.flight_num as fnum2,F2.origin_city as o2,F2.dest_city as dest2,F2.actual_time as time2,F2.fid as fid2,F2.capacity as capacity2,F2.price as price2,F2.canceled, "
                  + "(F1.actual_time + F2.actual_time) AS total_time "
                  + "FROM Flights AS F1, Flights AS F2 "
                  + "WHERE F2.origin_city = F1.dest_city "
                  + "AND F1.origin_city = ? "
                  + "AND F2.dest_city = ? "
                  + "AND F1.day_of_month = ? "
                  + "AND F2.day_of_month = F1.day_of_month "
                  + "AND F1.canceled = 0 "
                  + "AND F2.canceled = 0 "
                  + "ORDER BY F1.actual_time + F2.actual_time ASC, F1.fid ASC, F2.fid ASC";
  protected PreparedStatement indirectSearchStatement;

  private static final String ITINERARY_UPDATE = "INSERT INTO ITINERARIES VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
  protected PreparedStatement itineraryUpdateStatement;

  private static final String CLEAR_ITINERARIES = "DELETE FROM Itineraries";
  private PreparedStatement clearItinerariesStatement;

  class Flight
  {
    public int fid;
    public int dayOfMonth;
    public String carrierId;
    public String flightNum;
    public String originCity;
    public String destCity;
    public int time;
    public int capacity;
    public int price;
    public int cancelled;

    @Override
    public String toString()
    {
      return "ID: " + fid + " Day: " + dayOfMonth + " Carrier: " + carrierId +
              " Number: " + flightNum + " Origin: " + originCity + " Dest: " + destCity + " Duration: " + time +
              " Capacity: " + capacity + " Price: " + price;
    }
  }

  public QuerySearchOnly(String configFilename)
  {
    this.configFilename = configFilename;
  }

  /** Open a connection to SQL Server in Microsoft Azure.  */
  public void openConnection() throws Exception
  {
    Properties configProps = new Properties();
    configProps.load(new FileInputStream(configFilename));

    String jSQLDriver = configProps.getProperty("flightservice.jdbc_driver");
    String jSQLUrl = configProps.getProperty("flightservice.url");
    String jSQLUser = configProps.getProperty("flightservice.sqlazure_username");
    String jSQLPassword = configProps.getProperty("flightservice.sqlazure_password");

    /* load jdbc drivers */
    Class.forName(jSQLDriver).newInstance();

    /* open connections to the flights database */
    conn = DriverManager.getConnection(jSQLUrl, // database
            jSQLUser, // user
            jSQLPassword); // password

    conn.setAutoCommit(true); //by default automatically commit after each statement
    /* In the full Query class, you will also want to appropriately set the transaction's isolation level:
          conn.setTransactionIsolation(...)
       See Connection class's JavaDoc for details.
    */
    conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
  }

  public void closeConnection() throws Exception
  {
    conn.close();
  }

  /**
   * prepare all the SQL statements in this method.
   * "preparing" a statement is almost like compiling it.
   * Note that the parameters (with ?) are still not filled in
   */
  public void prepareStatements() throws Exception
  {
    checkFlightCapacityStatement = conn.prepareStatement(CHECK_FLIGHT_CAPACITY);

    /* add here more prepare statements for all the other queries you need */
    /* . . . . . . */

    directSearchStatement = conn.prepareStatement(DIRECT_SEARCH_SQL);
    indirectSearchStatement = conn.prepareStatement(INDIRECT_SEARCH_SQL);

    itineraryUpdateStatement = conn.prepareStatement(ITINERARY_UPDATE);

    clearItinerariesStatement = conn.prepareStatement(CLEAR_ITINERARIES);
    try {
      clearItinerariesStatement.executeUpdate();
    } catch (SQLException e) {

    }
  }

  private int itineraryCount;



  /**
   * Implement the search function.
   *
   * Searches for flights from the given origin city to the given destination
   * city, on the given day of the month. If {@code directFlight} is true, it only
   * searches for direct flights, otherwise it searches for direct flights
   * and flights with two "hops." Only searches for up to the number of
   * itineraries given by {@code numberOfItineraries}.
   *
   * The results are sorted based on total flight time.
   *
   * @param originCity
   * @param destinationCity
   * @param directFlight if true, then only search for direct flights, otherwise include indirect flights as well
   * @param dayOfMonth
   * @param numberOfItineraries number of itineraries to return
   *
   * @return If no itineraries were found, return "No flights match your selection\n".
   * If an error occurs, then return "Failed to search\n".
   *
   * Otherwise, the sorted itineraries printed in the following format:
   *
   * Itinerary [itinerary number]: [number of flights] flight(s), [total flight time] minutes\n
   * [first flight in itinerary]\n
   * ...
   * [last flight in itinerary]\n
   *
   * Each flight should be printed using the same format as in the {@code Flight} class. Itinerary numbers
   * in each search should always start from 0 and increase by 1.
   *
   * @see Flight#toString()
   */
  public String transaction_search(String originCity, String destinationCity, boolean directFlight, int dayOfMonth,
                                   int numberOfItineraries)
  {
    // Please implement your own (safe) version that uses prepared statements rather than string concatenation.
    // You may use the `Flight` class (defined above).

    try {
      clearItinerariesStatement.clearParameters();
      clearItinerariesStatement.executeUpdate();
    } catch (SQLException error) {
      error.printStackTrace();
      return "Failed to clear itineraries\n";
    }
    String result = "";
    if (directFlight) {
      try {
        result = directSearch(numberOfItineraries, originCity, destinationCity, dayOfMonth);
      } catch (SQLException error) {
        error.printStackTrace();
        return "Failed to search\n";
      }
      if (result.length() == 0) {
        return "No flights match your selection\n";
      } else {
        return result;
      }
    } else {
      try {
        result = indirectSearch(numberOfItineraries, originCity, destinationCity, dayOfMonth);
      } catch (SQLException error) {
        error.printStackTrace();
        return "Failed to search\n";
      }
      if (result.length() == 0) {
        return "No flights match your selection\n";
      } else {
        return result;
      }
    }
  }

  /**
   * Same as {@code transaction_search} except that it only performs single hop search and
   * do it in an unsafe manner.
   *
   * @param originCity
   * @param destinationCity
   * @param directFlight
   * @param dayOfMonth
   * @param numberOfItineraries
   *
   * @return The search results. Note that this implementation *does not conform* to the format required by
   * {@code transaction_search}.
   */
  //private String transaction_search_unsafe(String originCity, String destinationCity, boolean directFlight,
  //                                        int dayOfMonth, int numberOfItineraries)
  //{
  //  StringBuffer sb = new StringBuffer();
  //
  //  try
  //  {
  //    // one hop itineraries
  //    String unsafeSearchSQL =
  //            "SELECT TOP (" + numberOfItineraries + ") day_of_month,carrier_id,flight_num,origin_city,dest_city,actual_time,capacity,price "
  //                    + "FROM Flights "
  //                    + "WHERE origin_city = \'" + originCity + "\' AND dest_city = \'" + destinationCity + "\' AND day_of_month =  " + dayOfMonth + " "
  //                    + "ORDER BY actual_time ASC";
  //
  //    Statement searchStatement = conn.createStatement();
  //    ResultSet oneHopResults = searchStatement.executeQuery(unsafeSearchSQL);
  //
  //    while (oneHopResults.next())
  //    {
  //      int result_dayOfMonth = oneHopResults.getInt("day_of_month");
  //      String result_carrierId = oneHopResults.getString("carrier_id");
  //      String result_flightNum = oneHopResults.getString("flight_num");
  //      String result_originCity = oneHopResults.getString("origin_city");
  //      String result_destCity = oneHopResults.getString("dest_city");
  //      int result_time = oneHopResults.getInt("actual_time");
  //      int result_capacity = oneHopResults.getInt("capacity");
  //      int result_price = oneHopResults.getInt("price");
  //
  //      sb.append("Day: ").append(result_dayOfMonth)
  //              .append(" Carrier: ").append(result_carrierId)
  //              .append(" Number: ").append(result_flightNum)
  //              .append(" Origin: ").append(result_originCity)
  //              .append(" Destination: ").append(result_destCity)
  //              .append(" Duration: ").append(result_time)
  //              .append(" Capacity: ").append(result_capacity)
  //              .append(" Price: ").append(result_price)
  //              .append('\n');
  //    }
  //    oneHopResults.close();
  //  } catch (SQLException e) { e.printStackTrace(); }
  //
  //  return sb.toString();
  //}

  /**
   * Shows an example of using PreparedStatements after setting arguments.
   * You don't need to use this method if you don't want to.
   */
  private int checkFlightCapacity(int fid) throws SQLException
  {
    checkFlightCapacityStatement.clearParameters();
    checkFlightCapacityStatement.setInt(1, fid);
    ResultSet results = checkFlightCapacityStatement.executeQuery();
    results.next();
    int capacity = results.getInt("capacity");
    results.close();

    return capacity;
  }

  private String directSearch(int numberOfItineraries, String originCity, String destinationCity, int dayOfMonth) throws SQLException
  {

    StringBuffer sb = new StringBuffer();

    directSearchStatement.clearParameters();
    directSearchStatement.setInt(1, numberOfItineraries);
    directSearchStatement.setString(2, originCity);
    directSearchStatement.setString(3, destinationCity);
    directSearchStatement.setInt(4, dayOfMonth);
    ResultSet rs = directSearchStatement.executeQuery();
    //int row_id = 0;
    itineraryCount = 0;
    while (rs.next()) {

      int result_fid = rs.getInt("fid");
      int result_dayOfMonth = rs.getInt("day_of_month");
      String result_carrierId = rs.getString("carrier_id");
      String result_flightNum = rs.getString("flight_num");
      String result_originCity = rs.getString("origin_city");
      String result_destCity = rs.getString("dest_city");
      int result_time = rs.getInt("actual_time");
      int result_capacity = rs.getInt("capacity");
      int result_price = rs.getInt("price");

      sb.append("Itinerary ").append(itineraryCount).append((": "))
              .append("1 flight(s), ")
              .append(result_time).append(" minutes")
              .append('\n')
              .append("ID: ").append(result_fid)
              .append(" Day: ").append(result_dayOfMonth)
              .append(" Carrier: ").append(result_carrierId)
              .append(" Number: ").append(result_flightNum)
              .append(" Origin: ").append(result_originCity)
              .append(" Dest: ").append(result_destCity)
              .append(" Duration: ").append(result_time)
              .append(" Capacity: ").append(result_capacity)
              .append(" Price: ").append(result_price)
              .append('\n');

      itineraryUpdateStatement.clearParameters();
      itineraryUpdateStatement.setInt(1, itineraryCount);
      itineraryUpdateStatement.setInt(2, result_fid);
      itineraryUpdateStatement.setInt(3, -1);
      itineraryUpdateStatement.setInt(4, result_dayOfMonth);
      itineraryUpdateStatement.setInt(5, result_capacity);
      itineraryUpdateStatement.setInt(6, -1);
      itineraryUpdateStatement.setInt(7, result_price);
      itineraryUpdateStatement.setString(8, result_carrierId);
      itineraryUpdateStatement.setString(9, "");
      itineraryUpdateStatement.setString(10, result_flightNum);
      itineraryUpdateStatement.setString(11, "");
      itineraryUpdateStatement.setString(12, result_originCity);
      itineraryUpdateStatement.setString(13, "");
      itineraryUpdateStatement.setString(14, result_destCity);
      itineraryUpdateStatement.setString(15, "");
      itineraryUpdateStatement.setInt(16, result_time);
      itineraryUpdateStatement.setInt(17, -1);
      itineraryUpdateStatement.setInt(18, -1);
      itineraryUpdateStatement.setInt(19, 1);
      itineraryUpdateStatement.executeUpdate();
      itineraryCount++;
      //row_id++;
    }
    rs.close();
    return sb.toString();
  }

  private String indirectSearch(int numberOfItineraries, String originCity, String destinationCity, int dayOfMonth) throws SQLException {
    //if (numberOfItineraries == 1) {
    //  return directSearch(numberOfItineraries, originCity, destinationCity, dayOfMonth);
    //}
    String result = "";
    result = directSearch(numberOfItineraries, originCity, destinationCity, dayOfMonth);
    numberOfItineraries = numberOfItineraries - itineraryCount;

    StringBuffer sb = new StringBuffer();

    indirectSearchStatement.clearParameters();
    indirectSearchStatement.setInt(1, numberOfItineraries);
    indirectSearchStatement.setString(2, originCity);
    indirectSearchStatement.setString(3, destinationCity);
    indirectSearchStatement.setInt(4, dayOfMonth);
    ResultSet rs = indirectSearchStatement.executeQuery();
    //int flight_count = 0;
    while (rs.next()) {

      int result_dayOfMonth = rs.getInt(1);
      String result_carrierId = rs.getString(2);
      String result_flightNum = rs.getString(3);
      String result_originCity = rs.getString(4);
      String result_destCity = rs.getString(5);
      int result_time = rs.getInt(6);
      int result_fid = rs.getInt(7);
      int result_capacity = rs.getInt(8);
      int result_price = rs.getInt(9);

      int result_dayOfMonth2 = rs.getInt(11);
      String result_carrierId2 = rs.getString(12);
      String result_flightNum2 = rs.getString(13);
      String result_originCity2 = rs.getString(14);
      String result_destCity2 = rs.getString(15);
      int result_time2 = rs.getInt(16);
      int result_fid2 = rs.getInt(17);
      int result_capacity2 = rs.getInt(18);
      int result_price2 = rs.getInt(19);

      int total_price = result_price + result_price2;

      int result_total_time = rs.getInt("total_time");

      sb.append("Itinerary ").append(itineraryCount).append((": "))
              .append("2 flight(s), ")
              .append(result_total_time).append(" minutes")
              .append('\n')
              .append("ID: ").append(result_fid)
              .append(" Day: ").append(result_dayOfMonth)
              .append(" Carrier: ").append(result_carrierId)
              .append(" Number: ").append(result_flightNum)
              .append(" Origin: ").append(result_originCity)
              .append(" Dest: ").append(result_destCity)
              .append(" Duration: ").append(result_time)
              .append(" Capacity: ").append(result_capacity)
              .append(" Price: ").append(result_price)
              .append('\n')
              .append("ID: ").append(result_fid2)
              .append(" Day: ").append(result_dayOfMonth2)
              .append(" Carrier: ").append(result_carrierId2)
              .append(" Number: ").append(result_flightNum2)
              .append(" Origin: ").append(result_originCity2)
              .append(" Dest: ").append(result_destCity2)
              .append(" Duration: ").append(result_time2)
              .append(" Capacity: ").append(result_capacity2)
              .append(" Price: ").append(result_price2)
              .append('\n');

      itineraryUpdateStatement.clearParameters();
      itineraryUpdateStatement.setInt(1, itineraryCount);
      itineraryUpdateStatement.setInt(2, result_fid);
      itineraryUpdateStatement.setInt(3, result_fid2);
      itineraryUpdateStatement.setInt(4, result_dayOfMonth);
      itineraryUpdateStatement.setInt(5, result_capacity);
      itineraryUpdateStatement.setInt(6, result_capacity2);
      itineraryUpdateStatement.setInt(7, total_price);
      itineraryUpdateStatement.setString(8, result_carrierId);
      itineraryUpdateStatement.setString(9, result_carrierId2);
      itineraryUpdateStatement.setString(10, result_flightNum);
      itineraryUpdateStatement.setString(11, result_flightNum2);
      itineraryUpdateStatement.setString(12, result_originCity);
      itineraryUpdateStatement.setString(13, result_originCity2);
      itineraryUpdateStatement.setString(14, result_destCity);
      itineraryUpdateStatement.setString(15, result_destCity2);
      itineraryUpdateStatement.setInt(16, result_time);
      itineraryUpdateStatement.setInt(17, result_time2);
      itineraryUpdateStatement.setInt(18, result_price2);
      itineraryUpdateStatement.setInt(19, 1);
      itineraryUpdateStatement.executeUpdate();

      itineraryCount++;
    }
    rs.close();
    return result + sb.toString();
  }
}
