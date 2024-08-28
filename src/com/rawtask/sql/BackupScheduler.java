package com.rawtask.sql;

import java.io.File;
import java.util.Calendar;
import java.util.logging.Level;

import com.rawtask.TaskServer;

public class BackupScheduler {
	private final File backupDir;
	private final DBTools dbTools;

	public BackupScheduler(File backupDir, DBTools dbTools) {
		this.backupDir = backupDir;
		this.dbTools = dbTools;
	}

	private void backup() throws Exception {
		this.dbTools.exportToXML(this.backupDir);
	}

	public void start(final int hour) {
		final Thread t = new Thread(new Runnable() {

			@Override
			public void run() {
				while (!Thread.currentThread().isInterrupted()) {
					final Calendar c = Calendar.getInstance();
					final int h = c.get(Calendar.HOUR_OF_DAY);
					final int d = c.get(Calendar.DAY_OF_YEAR);
					try {
						if (h == hour) {
							backup();
							c.setTimeInMillis(System.currentTimeMillis());
							if (c.get(Calendar.HOUR_OF_DAY) == h && c.get(Calendar.DAY_OF_YEAR) == d) {
								// The backup took less than an hour, so let's
								// sleep 2 hours...
								Thread.sleep(1000 * 60 * 120L);
							}
						}
					} catch (Exception e) {
						TaskServer.LOGGER.log(Level.SEVERE, "backup failed", e);
					}
					// Wait 1 minute
					try {
						Thread.sleep(1000 * 60L);
					} catch (InterruptedException e) {
						TaskServer.LOGGER.log(Level.WARNING, "interrupted", e);
					}
				}
			}
		});
		t.setDaemon(true);
		t.setName("Backup scheduler thread");
		t.start();
	}
}
