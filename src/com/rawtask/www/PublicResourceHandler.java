package com.rawtask.www;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.MimeTypes;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import com.rawtask.FileSandBox;

public class PublicResourceHandler extends AbstractHandler {
	private final File rootDir;
	private final MimeTypes _mimeTypes = new MimeTypes();

	public PublicResourceHandler(File wwwDir) {
		this.rootDir = wwwDir;
	}

	@Override
	public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
			throws IOException, ServletException {
		if (!"admin".equals(baseRequest.getUserPrincipal().getName())) {
			return;
		}
		String requestURI = request.getRequestURI();
		target = target.toLowerCase();
		if (target.toLowerCase().endsWith(".html")) {
			return;
		}
		if (!requestURI.endsWith("/favicon.ico")) {
			if (!requestURI.startsWith("/rawtask/")) {
				return;
			}
			requestURI = requestURI.substring(8);
		}

		final File f = FileSandBox.getSandboxedFile(this.rootDir, requestURI);

		if (!f.exists() || f.isDirectory()) {
			baseRequest.setHandled(false);
			return;
		}

		response.setStatus(HttpServletResponse.SC_OK);

		String mime = this._mimeTypes.getMimeByExtension(requestURI);
		if (mime == null) {
			mime = this._mimeTypes.getMimeByExtension(request.getPathInfo());
		}

		// set the headers
		if (mime != null) {
			response.setContentType(mime.toString());
		}
		response.addDateHeader(HttpHeader.LAST_MODIFIED.name(), f.lastModified());
		response.setContentLength((int) f.length());
		final OutputStream out = response.getOutputStream();

		final FileInputStream in = new FileInputStream(f);
		final byte[] b = new byte[(int) f.length()];
		in.read(b);
		in.close();
		out.write(b);

		baseRequest.setHandled(true);
	}

}
