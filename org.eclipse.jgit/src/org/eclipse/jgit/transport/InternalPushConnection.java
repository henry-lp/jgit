/*
 * Copyright (C) 2015, Google Inc. and others
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Distribution License v. 1.0 which is available at
 * https://www.eclipse.org/org/documents/edl-v10.php.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 */

package org.eclipse.jgit.transport;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.UncheckedIOException;

import org.eclipse.jgit.errors.TransportException;
import org.eclipse.jgit.internal.JGitText;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.transport.resolver.ReceivePackFactory;
import org.eclipse.jgit.transport.resolver.ServiceNotAuthorizedException;
import org.eclipse.jgit.transport.resolver.ServiceNotEnabledException;

class InternalPushConnection<C> extends BasePackPushConnection {
	private Thread worker;

	/**
	 * Constructor for InternalPushConnection.
	 *
	 * @param transport
	 *            a {@link org.eclipse.jgit.transport.PackTransport}
	 * @param receivePackFactory
	 *            a
	 *            {@link org.eclipse.jgit.transport.resolver.ReceivePackFactory}
	 * @param req
	 *            a request
	 * @param remote
	 *            the {@link org.eclipse.jgit.lib.Repository}
	 * @throws org.eclipse.jgit.errors.TransportException
	 *             if any.
	 */
	public InternalPushConnection(PackTransport transport,
			final ReceivePackFactory<C> receivePackFactory,
			final C req, final Repository remote) throws TransportException {
		super(transport);
		final PipedOutputStream out_w;
		try (final java.io.PipedInputStream out_r = new java.io.PipedInputStream()) {
			out_w = new java.io.PipedOutputStream(out_r);
		} catch (java.io.IOException err) {
			remote.close();
			throw new org.eclipse.jgit.errors.TransportException(uri, org.eclipse.jgit.internal.JGitText.get().cannotConnectPipes, err);
		}

		worker = new Thread("JGit-Receive-Pack") { //$NON-NLS-1$
			@Override
			public void run() {
				try {
					final ReceivePack rp = receivePackFactory.create(req, remote);
					rp.receive(out_r, in_w, System.err);
				} catch (ServiceNotEnabledException
						| ServiceNotAuthorizedException e) {
					// Ignored. Client cannot use this repository.
				} catch (IOException e) {
					// Since the InternalPushConnection is used in tests, we
					// want to avoid hiding exceptions because they can point to
					// programming errors on the server side. By rethrowing, the
					// default handler will dump it to stderr.
					throw new UncheckedIOException(e);
				} finally {
					try {
						out_r.close();
					} catch (IOException e2) {
						// Ignore close failure, we probably crashed above.
					}

					try {
						in_w.close();
					} catch (IOException e2) {
						// Ignore close failure, we probably crashed above.
					}

					remote.close();
				}
			}
		};
		worker.start();

		init(in_r, out_w);
		readAdvertisedRefs();
	}

	/** {@inheritDoc} */
	@Override
	public void close() {
		super.close();

		if (worker != null) {
			try {
				worker.join();
			} catch (InterruptedException ie) {
				// Stop waiting and return anyway.
			} finally {
				worker = null;
			}
		}
	}
}
