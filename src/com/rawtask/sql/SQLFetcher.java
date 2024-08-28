package com.rawtask.sql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class SQLFetcher {
	private final Connection connection;

	public SQLFetcher(Connection connection) {
		this.connection = connection;
	}

	public synchronized String dump(String table) throws SQLException {
		final StringBuilder b = new StringBuilder();
		final Statement statement = this.connection.createStatement();
		final ResultSet rs = statement.executeQuery("SELECT * FROM " + table);
		b.append("TABLE " + table + "\n");
		final ResultSetMetaData rsmd = rs.getMetaData();

		final int columnsNumber = rsmd.getColumnCount();
		while (rs.next()) {
			for (int i = 0; i < columnsNumber; i++) {
				b.append(rs.getObject(i + 1));
				if (i < columnsNumber - 1) {
					b.append(",");
				}
			}
			b.append("\n");
		}
		statement.close();
		return b.toString();
	}

	public synchronized List<String> fetchColumnFromTable(String table, String field) throws SQLException {
		final List<String> result = new ArrayList<String>();
		final Statement statement = this.connection.createStatement();
		final ResultSet rs = statement.executeQuery("SELECT " + field + " FROM " + table);
		while (rs.next()) {
			result.add(rs.getString(1));
		}
		statement.close();
		return result;
	}

	public synchronized List<Integer> fetchColumnId(String sql) throws SQLException {
		final List<Integer> result = new ArrayList<Integer>();
		final Statement statement = this.connection.createStatement();
		final ResultSet rs = statement.executeQuery(sql);
		while (rs.next()) {
			result.add((int) rs.getLong(1));
		}
		statement.close();
		return result;
	}

	public synchronized List<Record> fetchRecordFromTable(String table) throws SQLException {
		return this.fetchRecordFromTable(table, "name");
	}

	public synchronized List<Record> fetchRecordFromTable(String table, String field) throws SQLException {
		final List<Record> result = new ArrayList<Record>();
		final Statement statement = this.connection.createStatement();
		final ResultSet rs = statement.executeQuery("SELECT id," + field + " FROM " + table);
		while (rs.next()) {
			result.add(new Record(String.valueOf(rs.getLong(1)), rs.getString(2)));
		}
		statement.close();
		return result;
	}
}
