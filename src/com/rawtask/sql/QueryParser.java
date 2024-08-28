package com.rawtask.sql;

import java.util.ArrayList;
import java.util.List;

public class QueryParser {
	private static String NOT_CONTAINS = "!~=";
	private static String CONTAINS = "~=";
	private static String ENDS_WITH = "$=";
	private static String BEGINS_WITH = "^=";
	private static String IS_NOT = "!=";
	private static String IS = "=";

	public static String convertToSQL(String query) {
		final String[] parts = query.split("&");
		int max = 0;
		int page = 0;
		String order = null;
		final List<String> whereParts = new ArrayList<>();
		for (int i = 0; i < parts.length; i++) {
			final String part = parts[i];
			int index = part.indexOf(QueryParser.NOT_CONTAINS);
			if (index > 0) {
				whereParts.add(part.replace(QueryParser.NOT_CONTAINS, " NOT LIKE '%") + "%'");
			} else {
				index = part.indexOf(QueryParser.CONTAINS);
				if (index > 0) {
					whereParts.add(part.replace(QueryParser.CONTAINS, " LIKE '%") + "%'");
				} else {
					index = part.indexOf(QueryParser.ENDS_WITH);
					if (index > 0) {
						whereParts.add(part.replace(QueryParser.ENDS_WITH, " LIKE '%") + "'");
					} else {
						index = part.indexOf(QueryParser.BEGINS_WITH);
						if (index > 0) {
							whereParts.add(part.replace(QueryParser.BEGINS_WITH, " LIKE '") + "%'");
						} else {
							index = part.indexOf(QueryParser.IS_NOT);
							if (index > 0) {
								whereParts.add(part.replace(QueryParser.IS_NOT, "!='") + "'");
							} else {
								index = part.indexOf(QueryParser.IS);
								if (index > 0) {
									if (part.startsWith("max=")) {
										max = Integer.parseInt(part.substring(4));
									} else if (part.startsWith("page=")) {
										page = Integer.parseInt(part.substring(5));
									} else if (part.startsWith("order=")) {
										order = part.substring(6);
									} else {
										whereParts.add(part.replace(QueryParser.IS, "='") + "'");
									}
								} else {
									throw new IllegalArgumentException(part + " is not a valid query part");
								}
							}
						}
					}
				}
			}
		}

		final StringBuilder builder = new StringBuilder();
		builder.append("SELECT id FROM ticket ");
		final int size = whereParts.size();
		if (size > 0) {
			builder.append("WHERE ");
			for (int i = 0; i < size; i++) {
				final String p = whereParts.get(i);
				builder.append(p);
				if (i < size - 1) {
					builder.append(" AND ");
				}
			}
		}
		if (order != null) {
			builder.append(" ORDER BY ");
			builder.append(order);
		}
		if (max > 0) {
			builder.append(" LIMIT ");
			builder.append(max);
			if (page > 0) {
				builder.append(" OFFSET ");
				builder.append(max * page);
			}
		}

		return builder.toString();
	}
}
