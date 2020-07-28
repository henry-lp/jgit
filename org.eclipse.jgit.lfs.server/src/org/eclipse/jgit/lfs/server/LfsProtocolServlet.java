/*
 * Copyright (C) 2015, Sasa Zivkov <sasa.zivkov@sap.com> and others
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Distribution License v. 1.0 which is available at
 * https://www.eclipse.org/org/documents/edl-v10.php.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 */
package org.eclipse.jgit.lfs.server;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.http.HttpStatus.SC_FORBIDDEN;
import static org.apache.http.HttpStatus.SC_INSUFFICIENT_STORAGE;
import static org.apache.http.HttpStatus.SC_INTERNAL_SERVER_ERROR;
import static org.apache.http.HttpStatus.SC_NOT_FOUND;
import static org.apache.http.HttpStatus.SC_OK;
import static org.apache.http.HttpStatus.SC_SERVICE_UNAVAILABLE;
import static org.apache.http.HttpStatus.SC_UNAUTHORIZED;
import static org.apache.http.HttpStatus.SC_UNPROCESSABLE_ENTITY;
import static org.eclipse.jgit.lfs.lib.Constants.DOWNLOAD;
import static org.eclipse.jgit.lfs.lib.Constants.UPLOAD;
import static org.eclipse.jgit.lfs.lib.Constants.VERIFY;
import static org.eclipse.jgit.util.HttpSupport.HDR_AUTHORIZATION;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.text.MessageFormat;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jgit.lfs.errors.LfsBandwidthLimitExceeded;
import org.eclipse.jgit.lfs.errors.LfsException;
import org.eclipse.jgit.lfs.errors.LfsInsufficientStorage;
import org.eclipse.jgit.lfs.errors.LfsRateLimitExceeded;
import org.eclipse.jgit.lfs.errors.LfsRepositoryNotFound;
import org.eclipse.jgit.lfs.errors.LfsRepositoryReadOnly;
import org.eclipse.jgit.lfs.errors.LfsUnauthorized;
import org.eclipse.jgit.lfs.errors.LfsUnavailable;
import org.eclipse.jgit.lfs.errors.LfsValidationError;
import org.eclipse.jgit.lfs.internal.LfsText;
import org.eclipse.jgit.lfs.server.internal.LfsGson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LFS protocol handler implementing the LFS batch API [1]
 *
 * [1] https://github.com/github/git-lfs/blob/master/docs/api/v1/http-v1-batch.md
 *
 * @since 4.3
 */
public abstract class LfsProtocolServlet extends HttpServlet {
	private static final Logger LOG = LoggerFactory
			.getLogger(LfsProtocolServlet.class);

	private static final long serialVersionUID = 1L;

	private static final String CONTENTTYPE_VND_GIT_LFS_JSON =
			"application/vnd.git-lfs+json; charset=utf-8"; //$NON-NLS-1$

	private static final int SC_RATE_LIMIT_EXCEEDED = 429;

	private static final int SC_BANDWIDTH_LIMIT_EXCEEDED = 509;

	/**
	 * Get the large file repository for the given request and path.
	 *
	 * @param request
	 *            the request
	 * @param path
	 *            the path
	 * @param auth
	 *            the Authorization HTTP header
	 * @return the large file repository storing large files.
	 * @throws org.eclipse.jgit.lfs.errors.LfsException
	 *             implementations should throw more specific exceptions to
	 *             signal which type of error occurred:
	 *             <dl>
	 *             <dt>{@link org.eclipse.jgit.lfs.errors.LfsValidationError}</dt>
	 *             <dd>when there is a validation error with one or more of the
	 *             objects in the request</dd>
	 *             <dt>{@link org.eclipse.jgit.lfs.errors.LfsRepositoryNotFound}</dt>
	 *             <dd>when the repository does not exist for the user</dd>
	 *             <dt>{@link org.eclipse.jgit.lfs.errors.LfsRepositoryReadOnly}</dt>
	 *             <dd>when the user has read, but not write access. Only
	 *             applicable when the operation in the request is "upload"</dd>
	 *             <dt>{@link org.eclipse.jgit.lfs.errors.LfsRateLimitExceeded}</dt>
	 *             <dd>when the user has hit a rate limit with the server</dd>
	 *             <dt>{@link org.eclipse.jgit.lfs.errors.LfsBandwidthLimitExceeded}</dt>
	 *             <dd>when the bandwidth limit for the user or repository has
	 *             been exceeded</dd>
	 *             <dt>{@link org.eclipse.jgit.lfs.errors.LfsInsufficientStorage}</dt>
	 *             <dd>when there is insufficient storage on the server</dd>
	 *             <dt>{@link org.eclipse.jgit.lfs.errors.LfsUnavailable}</dt>
	 *             <dd>when LFS is not available</dd>
	 *             <dt>{@link org.eclipse.jgit.lfs.errors.LfsException}</dt>
	 *             <dd>when an unexpected internal server error occurred</dd>
	 *             </dl>
	 * @since 4.7
	 */
	protected abstract LargeFileRepository getLargeFileRepository(
			LfsRequest request, String path, String auth) throws LfsException;

	/**
	 * LFS request.
	 *
	 * @since 4.5
	 */
	protected static class LfsRequest {
		private String operation;

		private List<LfsObject> objects;

		/**
		 * Get the LFS operation.
		 *
		 * @return the operation
		 */
		public String getOperation() {
			return operation;
		}

		/**
		 * Get the LFS objects.
		 *
		 * @return the objects
		 */
		public List<LfsObject> getObjects() {
			return objects;
		}

		/**
		 * @return true if the operation is upload.
		 * @since 4.7
		 */
		public boolean isUpload() {
			return operation.equals(UPLOAD);
		}

		/**
		 * @return true if the operation is download.
		 * @since 4.7
		 */
		public boolean isDownload() {
			return operation.equals(DOWNLOAD);
		}

		/**
		 * @return true if the operation is verify.
		 * @since 4.7
		 */
		public boolean isVerify() {
			return operation.equals(VERIFY);
		}
	}

	/** {@inheritDoc} */
	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse res)
			throws ServletException, IOException {
		try (java.io.Writer w = new java.io.BufferedWriter(new java.io.OutputStreamWriter(res.getOutputStream(), java.nio.charset.StandardCharsets.UTF_8))) {
			java.io.Reader r = new java.io.BufferedReader(new java.io.InputStreamReader(req.getInputStream(), java.nio.charset.StandardCharsets.UTF_8));
			org.eclipse.jgit.lfs.server.LfsProtocolServlet.LfsRequest request = org.eclipse.jgit.lfs.server.internal.LfsGson.fromJson(r, org.eclipse.jgit.lfs.server.LfsProtocolServlet.LfsRequest.class);
			java.lang.String path = req.getPathInfo();
			res.setContentType(org.eclipse.jgit.lfs.server.LfsProtocolServlet.CONTENTTYPE_VND_GIT_LFS_JSON);
			org.eclipse.jgit.lfs.server.LargeFileRepository repo = null;
			try {
				repo = getLargeFileRepository(request, path, req.getHeader(org.eclipse.jgit.util.HttpSupport.HDR_AUTHORIZATION));
				if (repo == null) {
					java.lang.String error = java.text.MessageFormat.format(org.eclipse.jgit.lfs.internal.LfsText.get().lfsFailedToGetRepository, path);
					org.eclipse.jgit.lfs.server.LfsProtocolServlet.LOG.error(error);
					throw new org.eclipse.jgit.lfs.errors.LfsException(error);
				}
				res.setStatus(org.apache.http.HttpStatus.SC_OK);
				org.eclipse.jgit.lfs.server.TransferHandler handler = org.eclipse.jgit.lfs.server.TransferHandler.forOperation(request.operation, repo, request.objects);
				org.eclipse.jgit.lfs.server.internal.LfsGson.toJson(handler.process(), w);
			} catch (org.eclipse.jgit.lfs.errors.LfsValidationError e) {
				sendError(res, w, org.apache.http.HttpStatus.SC_UNPROCESSABLE_ENTITY, e.getMessage());
			} catch (org.eclipse.jgit.lfs.errors.LfsRepositoryNotFound e) {
				sendError(res, w, org.apache.http.HttpStatus.SC_NOT_FOUND, e.getMessage());
			} catch (org.eclipse.jgit.lfs.errors.LfsRepositoryReadOnly e) {
				sendError(res, w, org.apache.http.HttpStatus.SC_FORBIDDEN, e.getMessage());
			} catch (org.eclipse.jgit.lfs.errors.LfsRateLimitExceeded e) {
				sendError(res, w, org.eclipse.jgit.lfs.server.LfsProtocolServlet.SC_RATE_LIMIT_EXCEEDED, e.getMessage());
			} catch (org.eclipse.jgit.lfs.errors.LfsBandwidthLimitExceeded e) {
				sendError(res, w, org.eclipse.jgit.lfs.server.LfsProtocolServlet.SC_BANDWIDTH_LIMIT_EXCEEDED, e.getMessage());
			} catch (org.eclipse.jgit.lfs.errors.LfsInsufficientStorage e) {
				sendError(res, w, org.apache.http.HttpStatus.SC_INSUFFICIENT_STORAGE, e.getMessage());
			} catch (org.eclipse.jgit.lfs.errors.LfsUnavailable e) {
				sendError(res, w, org.apache.http.HttpStatus.SC_SERVICE_UNAVAILABLE, e.getMessage());
			} catch (org.eclipse.jgit.lfs.errors.LfsUnauthorized e) {
				sendError(res, w, org.apache.http.HttpStatus.SC_UNAUTHORIZED, e.getMessage());
			} catch (org.eclipse.jgit.lfs.errors.LfsException e) {
				sendError(res, w, org.apache.http.HttpStatus.SC_INTERNAL_SERVER_ERROR, e.getMessage());
			} finally {
				w.flush();
			}
		}
	}

	private void sendError(HttpServletResponse rsp, Writer writer, int status,
			String message) {
		rsp.setStatus(status);
		LfsGson.toJson(message, writer);
	}
}
