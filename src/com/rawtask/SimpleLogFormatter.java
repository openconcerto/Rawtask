package com.rawtask;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

public class SimpleLogFormatter extends Formatter {
	private final SimpleDateFormat DF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	private final Date dat = new Date();

	@Override
	public String format(LogRecord record) {
		this.dat.setTime(record.getMillis());
		return this.DF.format(this.dat) + ":" + record.getLevel().getName() + ":" + record.getLoggerName() + ":"
				+ record.getSourceMethodName() + ": " + record.getMessage() + "\n";
	}

}
