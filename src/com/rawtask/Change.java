package com.rawtask;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Change {
	public static String getLastComment(Connection connection, Integer requestedId) {
		Statement statement = null;
		try {
			statement = connection.createStatement();
			final ResultSet rs = statement
					.executeQuery("SELECT newvalue FROM ticket_change WHERE field='comment' and ticket_id="
							+ requestedId + " ORDER BY time DESC LIMIT 1");
			String v = "";
			while (rs.next()) {
				v = rs.getString(1);
			}
			return v;
		} catch (final SQLException e) {
			e.printStackTrace();
		} finally {
			if (statement != null) {
				try {
					statement.close();
				} catch (final SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return "";
	}

	private final int idTicket;
	private final long time;
	private final String author;
	private final String field;
	private final String oldValue;

	private final String newValue;

	public Change(int idTicket, long time, String author, String field, String oldValue, String newValue) {
		this.idTicket = idTicket;
		this.time = time;
		this.author = author;
		this.field = field;
		this.oldValue = oldValue;
		this.newValue = newValue;
	}

	public void commit(Connection connection) throws SQLException {
		final String sql = "INSERT INTO ticket_change (ticket_id,time,author,field,oldvalue,newvalue) VALUES (?,?,?,?,?,?)";

		final PreparedStatement preparedStatement = connection.prepareStatement(sql);
		preparedStatement.setInt(1, this.idTicket);
		preparedStatement.setLong(2, this.time);
		preparedStatement.setString(3, this.author);
		preparedStatement.setString(4, this.field);
		preparedStatement.setString(5, this.oldValue);
		preparedStatement.setString(6, this.newValue);
		TaskServer.LOGGER.finest("Store change on ticket " + this.idTicket + ", field : " + this.field + " : "
				+ this.oldValue + " -> " + this.newValue + " , by " + this.author);
		preparedStatement.executeUpdate();
		preparedStatement.close();

	}
}
