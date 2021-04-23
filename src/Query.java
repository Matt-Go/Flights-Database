import java.sql.*;
import java.util.ArrayList;

public class Query extends QuerySearchOnly {

	// Logged In User
	private String username; // customer username is unique

	// Reservation ID
	private int res_id_pay;
	private int curr_res_id = 1;

	private ArrayList<ArrayList<Integer>> itineraries;

	private static final String CLEAR_USERS = "DELETE FROM Users";
	private PreparedStatement clearUsersStatement;

	private static final String CLEAR_RESERVATIONS = "DELETE FROM Reservations";
	private PreparedStatement clearReservationsStatement;

	private static final String CLEAR_ITINERARIES = "DELETE FROM Itineraries";
	private PreparedStatement clearItinerariesStatement;

	private static final String CREATE_CUSTOMER = "INSERT INTO Users VALUES (?, ?, ?)";
	private PreparedStatement createCustomerStatement;

	private static final String LOGIN = "SELECT COUNT(*) as count FROM Users WHERE username = ? AND password = ?";
	private PreparedStatement loginStatement;

	private static final String GET_ITINERARY = "SELECT * FROM Itineraries WHERE itinerary_id = ?";
	private PreparedStatement getItineraryStatement;

	private static final String UPDATE_RESERVATION = "INSERT INTO Reservations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
	private PreparedStatement updateReservationStatement;

	private static final String COUNT_RESERVATION = "SELECT COUNT(*) AS count FROM Reservations";
	private PreparedStatement countReservationStatement;

	private static final String DAY_RESERVATION = "SELECT day AS rday FROM Reservations";
	private PreparedStatement dayReservationStatement;

	private static final String GET_RESERVATION = "SELECT * FROM Reservations";
	private PreparedStatement getReservationStatement;

	private static final String UPDATE_PAID_RESERVATION = "UPDATE Reservations SET paid = 1 WHERE reservation_id = ?";
	private PreparedStatement updatePaidReservationStatement;

	private static final String GET_LAST_RESERVATION = "SELECT TOP 1 * FROM Reservations ORDER BY reservation_id DESC";
	private PreparedStatement getLastReservationStatement;

	private static final String CANCEL_RESERVATION = "DELETE FROM Reservations WHERE reservation_id = ?";
	private PreparedStatement cancelReservationStatement;

	private static final String GET_USER_BALANCE = "SELECT balance FROM Users WHERE username = ?";
	private PreparedStatement getUserBalanceStatement;

	private static final String UPDATE_USER_BALANCE = "UPDATE Users SET balance = ? WHERE username = ?";
	private PreparedStatement updateUserBalanceStatement;

	// transactions
	private static final String BEGIN_TRANSACTION_SQL = "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; BEGIN TRANSACTION;";
	protected PreparedStatement beginTransactionStatement;

	private static final String COMMIT_SQL = "COMMIT TRANSACTION";
	protected PreparedStatement commitTransactionStatement;

	private static final String ROLLBACK_SQL = "ROLLBACK TRANSACTION";
	protected PreparedStatement rollbackTransactionStatement;


	public Query(String configFilename) {
		super(configFilename);
	}


	/**
	 * Clear the data in any custom tables created. Do not drop any tables and do not
	 * clear the flights table. You should clear any tables you use to store reservations
	 * and reset the next reservation ID to be 1.
	 */
	public void clearTables() throws Exception
	{
		// your code here
		try {
			clearUsersStatement.executeUpdate();
			clearReservationsStatement.executeUpdate();
			clearItinerariesStatement.executeUpdate();
		} catch (SQLException error) {
		}
	}


	/**
	 * prepare all the SQL statements in this method.
	 * "preparing" a statement is almost like compiling it.
	 * Note that the parameters (with ?) are still not filled in
	 */
	@Override
	public void prepareStatements() throws Exception
	{
		super.prepareStatements();
		beginTransactionStatement = conn.prepareStatement(BEGIN_TRANSACTION_SQL);
		commitTransactionStatement = conn.prepareStatement(COMMIT_SQL);
		rollbackTransactionStatement = conn.prepareStatement(ROLLBACK_SQL);

		/* add here more prepare statements for all the other queries you need */
		/* . . . . . . */
		clearUsersStatement = conn.prepareStatement(CLEAR_USERS);
		clearReservationsStatement = conn.prepareStatement(CLEAR_RESERVATIONS);
		clearItinerariesStatement = conn.prepareStatement(CLEAR_ITINERARIES);

		createCustomerStatement = conn.prepareStatement(CREATE_CUSTOMER);
		loginStatement = conn.prepareStatement(LOGIN);
		getItineraryStatement = conn.prepareStatement(GET_ITINERARY);
		updateReservationStatement = conn.prepareStatement(UPDATE_RESERVATION);
		countReservationStatement = conn.prepareStatement(COUNT_RESERVATION);
		dayReservationStatement = conn.prepareStatement(DAY_RESERVATION);
		getReservationStatement = conn.prepareStatement(GET_RESERVATION);
		updatePaidReservationStatement = conn.prepareStatement(UPDATE_PAID_RESERVATION);
		getUserBalanceStatement = conn.prepareStatement(GET_USER_BALANCE);
		updateUserBalanceStatement = conn.prepareStatement(UPDATE_USER_BALANCE);
		getLastReservationStatement = conn.prepareStatement(GET_LAST_RESERVATION);
		cancelReservationStatement = conn.prepareStatement(CANCEL_RESERVATION);

		clearTables();
	}


	/**
	 * Takes a user's username and password and attempts to log the user in.
	 *
	 * @return If someone has already logged in, then return "User already logged in\n"
	 * For all other errors, return "Login failed\n".
	 *
	 * Otherwise, return "Logged in as [username]\n".
	 */
	public String transaction_login(String username, String password)
	{
		try {
			if (this.username != null) {
				return "User already logged in\n";
			}
			loginStatement.clearParameters();
			loginStatement.setString(1, username);
			loginStatement.setString(2, password);
			ResultSet results = loginStatement.executeQuery();
			results.next();
			int count = results.getInt("count");
			results.close();
			if (count == 1) {
				this.username = username;
				return "Logged in as " + username + "\n";
			}
		} catch (SQLException error) {
			error.printStackTrace();
			return "Login failed\n";
		}
		return "Login failed\n";
	}

	/**
	 * Implement the create user function.
	 *
	 * @param username new user's username. User names are unique the system.
	 * @param password new user's password.
	 * @param initAmount initial amount to deposit into the user's account, should be >= 0 (failure otherwise).
	 *
	 * @return either "Created user {@code username}\n" or "Failed to create user\n" if failed.
	 */
	public String transaction_createCustomer (String username, String password, int initAmount)
	{
		try {
			createCustomerStatement.clearParameters();
			createCustomerStatement.setString(1, username);
			createCustomerStatement.setString(2, password);
			createCustomerStatement.setInt(3, initAmount);
			createCustomerStatement.executeUpdate();
			return "Created user " + username + "\n";
		} catch (SQLException error) {
			error.printStackTrace();
			return "Failed to create user\n";
		}
	}

	/**
	 * Implements the book itinerary function.
	 *
	 * @param itineraryId ID of the itinerary to book. This must be one that is returned by search in the current session.
	 *
	 * @return If the user is not logged in, then return "Cannot book reservations, not logged in\n".
	 * If try to book an itinerary with invalid ID, then return "No such itinerary {@code itineraryId}\n".
	 * If the user already has a reservation on the same day as the one that they are trying to book now, then return
	 * "You cannot book two flights in the same day\n".
	 * For all other errors, return "Booking failed\n".
	 *
	 * And if booking succeeded, return "Booked flight(s), reservation ID: [reservationId]\n" where
	 * reservationId is a unique number in the reservation system that starts from 1 and increments by 1 each time a
	 * successful reservation is made by any user in the system.
	 */
	public String transaction_book(int itineraryId) {
		if (username == null) {
			return "Cannot book reservations, not logged in\n";
		}

		try {
			beginTransaction();
			getItineraryStatement.clearParameters();
			getItineraryStatement.setInt(1, itineraryId);
			ResultSet rs1 = getItineraryStatement.executeQuery();
			rs1.next();
			int fid1 = rs1.getInt("fid1");
			int fid2 = rs1.getInt("fid2");
			int day = rs1.getInt("day");
			int price = rs1.getInt("total_price");
			int capacity1 = rs1.getInt("capacity1");
			int capacity2 = rs1.getInt("capacity2");
			String carrier1 = rs1.getString("carrier1");
			String carrier2 = rs1.getString("carrier2");
			String flightNum1 = rs1.getString("flight_num1");
			String flightNum2 = rs1.getString("flight_num2");
			String origin_city1 = rs1.getString("origin_city1");
			String origin_city2 = rs1.getString("origin_city2");
			String dest_city1 = rs1.getString("dest_city1");
			String dest_city2 = rs1.getString("dest_city2");
			int duration1 = rs1.getInt("duration1");
			int duration2 = rs1.getInt("duration2");
			int price2 = rs1.getInt("price2");
			int direct = rs1.getInt("direct");
			dayReservationStatement.clearParameters();
			ResultSet day_rs = dayReservationStatement.executeQuery();
			while (day_rs.next()) {
				if (day == day_rs.getInt("rday")) {
					day_rs.close();
					rollbackTransaction();
					return "You cannot book two flights in the same day\n";
				}
			}
			rs1.close();
			day_rs.close();
			getItineraryStatement.clearParameters();
			getItineraryStatement.setInt(1, itineraryId);
			ResultSet cap = getItineraryStatement.executeQuery();
			while (cap.next()) {
				if (itineraryId == cap.getInt("itinerary_id")){
					if (cap.getInt("capacity1") == 0 || cap.getInt("capacity2") == 0) {
						cap.close();
						rollbackTransaction();
						return "Flight(s) has no capacity\n";
					}
				}
			}

			getItineraryStatement.clearParameters();
			getItineraryStatement.setInt(1, itineraryId);
			ResultSet rs = getItineraryStatement.executeQuery();
			while (rs.next()) {
				if (itineraryId == rs.getInt("itinerary_id")) {
					countReservationStatement.clearParameters();
					ResultSet count = countReservationStatement.executeQuery();
					count.next();
					int res_id = count.getInt("count");
					if (res_id == 0) {
						curr_res_id = 1;
					} else {
						ResultSet last = getLastReservationStatement.executeQuery();
						last.next();
						curr_res_id = last.getInt("reservation_id");
						curr_res_id++;
						last.close();
					}
					updateReservationStatement.clearParameters();
					updateReservationStatement.setInt(1, curr_res_id);
					updateReservationStatement.setInt(2, 0);
					updateReservationStatement.setInt(3, fid1);
					updateReservationStatement.setInt(4, fid2);
					updateReservationStatement.setInt(5, day);
					updateReservationStatement.setInt(6, price);
					updateReservationStatement.setInt(7, capacity1);
					updateReservationStatement.setInt(8, capacity2);
					updateReservationStatement.setString(9, carrier1);
					updateReservationStatement.setString(10, carrier2);
					updateReservationStatement.setString(11, flightNum1);
					updateReservationStatement.setString(12, flightNum2);
					updateReservationStatement.setString(13, origin_city1);
					updateReservationStatement.setString(14, origin_city2);
					updateReservationStatement.setString(15, dest_city1);
					updateReservationStatement.setString(16, dest_city2);
					updateReservationStatement.setInt(17, duration1);
					updateReservationStatement.setInt(18, duration2);
					updateReservationStatement.setInt(19, price2);
					updateReservationStatement.setInt(20, direct);
					updateReservationStatement.executeUpdate();

					count.close();

					rs.close();
					commitTransaction();
					return "Booked flight(s), reservation ID: " + curr_res_id + "\n";
				} else {
					rs.close();
					rollbackTransaction();
					return "No such itinerary " + itineraryId + "\n";
				}
			}
		} catch (SQLException error) {
			error.printStackTrace();
			return "Booking failed\n";
		}
		return "Booking failed\n";
	}

	/**
	 * Implements the pay function.
	 *
	 * @param reservationId the reservation to pay for.
	 *
	 * @return If no user has logged in, then return "Cannot pay, not logged in\n"
	 * If the reservation is not found / not under the logged in user's name, then return
	 * "Cannot find unpaid reservation [reservationId] under user: [username]\n"
	 * If the user does not have enough money in their account, then return
	 * "User has only [balance] in account but itinerary costs [cost]\n"
	 * For all other errors, return "Failed to pay for reservation [reservationId]\n"
	 *
	 * If successful, return "Paid reservation: [reservationId] remaining balance: [balance]\n"
	 * where [balance] is the remaining balance in the user's account.
	 */
	public String transaction_pay (int reservationId)
	{
		if (username == null) {
			return "Cannot pay, not logged in\n";
		}
		try {
			beginTransaction();
			getReservationStatement.clearParameters();
			ResultSet reservation = getReservationStatement.executeQuery();
			while (reservation.next()) {
				int res_id = reservation.getInt("reservation_id");
			    if (res_id == reservationId && reservation.getInt("paid") == 1){
					reservation.close();
					rollbackTransaction();
					return "Cannot find unpaid reservation " + reservationId + " under user: " + username + "\n";
				} else if (res_id == reservationId) {
					res_id_pay = res_id;
					int payment = reservation.getInt("total_price");
					getUserBalanceStatement.clearParameters();
					getUserBalanceStatement.setString(1, username);
					ResultSet user = getUserBalanceStatement.executeQuery();
					user.next();
					int balance = user.getInt("balance");
					if (balance < payment) {
						reservation.close();
						rollbackTransaction();
						return "User has only " + balance + " in account but itinerary costs " + payment  +"\n";
					} else {
						int new_balance = balance - payment;
						updateUserBalanceStatement.clearParameters();
						updateUserBalanceStatement.setInt(1, new_balance);
						updateUserBalanceStatement.setString(2, username);
						updateUserBalanceStatement.executeUpdate();

						updatePaidReservationStatement.clearParameters();
						updatePaidReservationStatement.setInt(1, res_id);
						updatePaidReservationStatement.executeUpdate();

						user.close();
						reservation.close();

						commitTransaction();;
						return "Paid reservation: " + res_id + " remaining balance: " + new_balance + "\n";
					}
			    }
			}
		} catch (SQLException error) {
			error.printStackTrace();
			return "Failed to pay for reservation " +  reservationId + "\n";
		}
		return "Cannot find unpaid reservation " + reservationId + " under user: " + username + "\n";
	}

	/**
	 * Implements the reservations function.
	 *
	 * @return If no user has logged in, then return "Cannot view reservations, not logged in\n"
	 * If the user has no reservations, then return "No reservations found\n"
	 * For all other errors, return "Failed to retrieve reservations\n"
	 *
	 * Otherwise return the reservations in the following format:
	 *
	 * Reservation [reservation ID] paid: [true or false]:\n"
	 * [flight 1 under the reservation]
	 * [flight 2 under the reservation]
	 * Reservation [reservation ID] paid: [true or false]:\n"
	 * [flight 1 under the reservation]
	 * [flight 2 under the reservation]
	 * ...
	 *
	 * Each flight should be printed using the same format as in the {@code Flight} class.
	 *
	 * @see Flight#toString()
	 */
	public String transaction_reservations()
	{
		if (username == null) {
			return "Cannot view reservations, not logged in\n";
		}

		StringBuffer sb = new StringBuffer();
		try {
		getReservationStatement.clearParameters();
		ResultSet rs = getReservationStatement.executeQuery();
			while (rs.next()) {
				int res_id = rs.getInt("reservation_id");
				String paid = rs.getInt("paid") == 0 ? "false" : "true";
				int fid1 = rs.getInt("fid1");
				int fid2 = rs.getInt("fid2");
				int day = rs.getInt("day");
				int price = rs.getInt("total_price");
				int capacity1 = rs.getInt("capacity1");
				int capacity2 = rs.getInt("capacity2");
				String carrier1 = rs.getString("carrier1");
				String carrier2 = rs.getString("carrier2");
				String flightNum1 = rs.getString("flight_num1");
				String flightNum2 = rs.getString("flight_num2");
				String origin_city1 = rs.getString("origin_city1");
				String origin_city2 = rs.getString("origin_city2");
				String dest_city1 = rs.getString("dest_city1");
				String dest_city2 = rs.getString("dest_city2");
				int duration1 = rs.getInt("duration1");
				int duration2 = rs.getInt("duration2");
				int price2 = rs.getInt("price2");
				int direct = rs.getInt("direct");

				if (direct == 1) {
					sb.append("Reservation ").append(res_id)
							.append(" paid: ").append(paid).append(":\n")
							.append("ID: ").append(fid1)
							.append(" Day: ").append(day)
							.append(" Carrier: ").append(carrier1)
							.append(" Number: ").append(flightNum1)
							.append(" Origin: ").append(origin_city1)
							.append(" Dest: ").append(dest_city1)
							.append(" Duration: ").append(duration1)
							.append(" Capacity: ").append(capacity1)
							.append(" Price: ").append(price).append("\n");
				} else if(direct == 0) {
					sb.append("Reservation ").append(res_id)
							.append(" paid: ").append(paid).append(":\n")
							.append("ID: ").append(fid1)
							.append(" Day: ").append(day)
							.append(" Carrier: ").append(carrier1)
							.append(" Number: ").append(flightNum1)
							.append(" Origin: ").append(origin_city1)
							.append(" Dest: ").append(dest_city1)
							.append(" Duration: ").append(duration1)
							.append(" Capacity: ").append(capacity1)
							.append(" Price: ").append(price).append("\n")
							.append("ID: ").append(fid2)
							.append(" Day: ").append(day)
							.append(" Carrier: ").append(carrier2)
							.append(" Number: ").append(flightNum2)
							.append(" Origin: ").append(origin_city2)
							.append(" Dest: ").append(dest_city2)
							.append(" Duration: ").append(duration2)
							.append(" Capacity: ").append(capacity2)
							.append(" Price: ").append(price2).append("\n");
				}

				rs.close();
				return sb.toString();
			}
		} catch (SQLException error) {
			error.printStackTrace();
			return "Failed to retrieve reservations\n";
		}
		return "Failed to retrieve reservations\n";
	}

	/**
	 * Implements the cancel operation.
	 *
	 * @param reservationId the reservation ID to cancel
	 *
	 * @return If no user has logged in, then return "Cannot cancel reservations, not logged in\n"
	 * For all other errors, return "Failed to cancel reservation [reservationId]"
	 *
	 * If successful, return "Canceled reservation [reservationId]"
	 *
	 * Even though a reservation has been canceled, its ID should not be reused by the system.
	 */
	public String transaction_cancel(int reservationId)
	{
		// only implement this if you are interested in earning extra credit for the HW!
		if (username == null) {
			return "Cannot cancel reservations, not logged in\n";
		}

		try {
			beginTransaction();
			getReservationStatement.clearParameters();
			ResultSet rs = getReservationStatement.executeQuery();
			while (rs.next()) {
				int res_id = rs.getInt("reservation_id");
				if (reservationId == res_id) {
					cancelReservationStatement.clearParameters();
					cancelReservationStatement.setInt(1, res_id);
					cancelReservationStatement.executeUpdate();

					getUserBalanceStatement.clearParameters();
					getUserBalanceStatement.setString(1, username);
					ResultSet balance = getUserBalanceStatement.executeQuery();
					balance.next();
					int curr_balance = balance.getInt("balance");

					int refund = rs.getInt("total_price");

					int new_balance = curr_balance + refund;
					updateUserBalanceStatement.clearParameters();
					updateUserBalanceStatement.setInt(1, new_balance);
					updateUserBalanceStatement.setString(2, username);
					updateUserBalanceStatement.executeUpdate();
					balance.close();

					rs.close();
					commitTransaction();
					return "Canceled reservation " + reservationId + "\n";
				} else {
					rs.close();
					rollbackTransaction();
					return "Failed to cancel reservation " + reservationId + "\n";
				}
			}
		} catch (SQLException error) {
			error.printStackTrace();
			return "Failed to cancel reservation " + reservationId + "\n";
		}
		return "Failed to cancel reservation " + reservationId + "\n";
	}

	/* some utility functions below */

	public void beginTransaction() throws SQLException
	{
		conn.setAutoCommit(false);
		beginTransactionStatement.executeUpdate();
	}

	public void commitTransaction() throws SQLException
	{
		commitTransactionStatement.executeUpdate();
		conn.setAutoCommit(true);
	}

	public void rollbackTransaction() throws SQLException
	{
		rollbackTransactionStatement.executeUpdate();
		conn.setAutoCommit(true);
	}
}
