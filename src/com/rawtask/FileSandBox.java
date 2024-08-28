package com.rawtask;

import java.io.File;

public class FileSandBox {
	public static File getSandboxedFile(File root, String path) {
		final File f = new File(root, path);
		try {
			final String pRoot = root.getCanonicalPath();
			final String pFile = f.getCanonicalPath();
			if (!pFile.startsWith(pRoot)) {
				return root;
			}
		} catch (final Exception e) {
			return root;
		}
		return f;

	}
}
