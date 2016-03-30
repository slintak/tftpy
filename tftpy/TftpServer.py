"""This module implements the TFTP Server functionality. Instantiate an
instance of the server, and then run the listen() method to listen for client
requests. Logging is performed via a standard logging object set in
TftpShared."""

import socket, os, time
import select
from TftpShared import *
from TftpPacketTypes import *
from TftpPacketFactory import TftpPacketFactory
from TftpContexts import TftpContextServer

class TftpServer(TftpSession):
    """This class implements a tftp server object. Run the listen() method to
    listen for client requests.  It takes two optional arguments. tftproot is
    the path to the tftproot directory to serve files from and/or write them
    to. dyn_file_func is a callable that must return a file-like object to
    read from during downloads. This permits the serving of dynamic
    content."""

    def __init__(self, tftproot='/tftpboot', dyn_file_func=None):
        self.listenip = None
        self.listenport = None
        self.sock = None
        # FIXME: What about multiple roots?
        self.root = os.path.abspath(tftproot)
        self.dyn_file_func = dyn_file_func
        # A dict of sessions, where each session is keyed by a string like
        # ip:tid for the remote end.
        self.sessions = {}

        self.shutdown_gracefully = False
        self.shutdown_immediately = False

        if self.dyn_file_func:
            if not callable(self.dyn_file_func):
                raise TftpException, "A dyn_file_func supplied, but it is not callable."
        elif os.path.exists(self.root):
            log.debug("tftproot %s does exist", self.root)
            if not os.path.isdir(self.root):
                raise TftpException, "The tftproot must be a directory."
            else:
                log.debug("tftproot %s is a directory", self.root)
                if os.access(self.root, os.R_OK):
                    log.debug("tftproot %s is readable", self.root)
                else:
                    raise TftpException, "The tftproot must be readable"
                if os.access(self.root, os.W_OK):
                    log.debug("tftproot %s is writable", self.root)
                else:
                    log.warning("The tftproot %s is not writable" % self.root)
        else:
            raise TftpException, "The tftproot does not exist."

    def listen(self,
               listenip="",
               listenport=DEF_TFTP_PORT,
               timeout=SOCK_TIMEOUT,
               singleport=False):
        """Start a server listening on the supplied interface and port. This
        defaults to INADDR_ANY (all interfaces) and UDP port 69. You can also
        supply a different socket timeout value, if desired."""
        # tftp_factory = TftpPacketFactory()

        # Don't use new 2.5 ternary operator yet
        # listenip = listenip if listenip else '0.0.0.0'
        if not listenip:
            listenip = '0.0.0.0'

        log.debug("Server requested on ip %s, port %s, singleport %s"
                % (listenip, listenport, singleport))
        try:
            # FIXME - sockets should be non-blocking
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.sock.bind((listenip, listenport))
        except socket.error, err:
            # Reraise it for now.
            raise

        log.debug("Starting receive loop...")
        while True:
            log.debug("shutdown_immediately is %s", self.shutdown_immediately)
            log.debug("shutdown_gracefully is %s", self.shutdown_gracefully)
            if self.shutdown_immediately:
                log.warn("Shutting down now. Session count: %d" % len(self.sessions))
                self.sock.close()
                for key in self.sessions:
                    self.sessions[key].end()
                self.sessions = []
                break

            elif self.shutdown_gracefully:
                if not self.sessions:
                    log.warn("In graceful shutdown mode and all sessions complete.")
                    self.sock.close()
                    break

            self.deletion_list = []

            if singleport:
                self.singleport_loop(timeout)
            else:
                self.standard_loop(timeout)

            log.debug("Looping on all sessions to check for timeouts")
            now = time.time()
            for key in self.sessions:
                try:
                    self.sessions[key].checkTimeout(now)
                except TftpTimeout, err:
                    log.error(str(err))
                    self.sessions[key].retry_count += 1
                    if self.sessions[key].retry_count >= TIMEOUT_RETRIES:
                        log.debug("hit max retries on %s, giving up",
                            self.sessions[key])
                        self.deletion_list.append(key)
                    else:
                        log.debug("resending on session %s", self.sessions[key])
                        self.sessions[key].state.resendLast()

            log.debug("Iterating deletion list.")
            for key in self.deletion_list:
                log.debug('')
                log.debug("Session %s complete" % key)
                if self.sessions.has_key(key):
                    log.debug("Gathering up metrics from session before deleting")
                    self.sessions[key].end()
                    metrics = self.sessions[key].metrics
                    if metrics.duration == 0:
                        log.debug("Duration too short, rate undetermined")
                    else:
                        log.debug("Transferred %d bytes in %.2f seconds"
                            % (metrics.bytes, metrics.duration))
                        log.debug("Average rate: %.2f kbps" % metrics.kbps)
                    log.debug("%.2f bytes in resent data" % metrics.resent_bytes)
                    log.debug("%d duplicate packets" % metrics.dupcount)
                    log.debug("Deleting session %s", key)

                    if not singleport:
                        self.sessions[key].sock.close()
                    del self.sessions[key]
                    log.debug("Session list is now %s", self.sessions)
                else:
                    log.warn("Strange, session %s is not on the deletion list"
                        % key)

        log.debug("server returning from while loop")
        self.shutdown_gracefully = self.shutdown_immediately = False

    def singleport_loop(self, timeout):
        # Build the inputlist array of sockets to select() on.
        inputlist = [self.sock]

        # Block until some socket has input on it.
        log.debug("Performing select on this inputlist: %s", inputlist)
        readyinput, readyoutput, readyspecial = select.select(
            inputlist,
            [],
            [],
            SOCK_TIMEOUT
        )

        for readysock in readyinput:
            buffer, (raddress, rport) = readysock.recvfrom(MAX_BLKSIZE)
            key = '%s:%s' % (raddress, rport)

            log.debug("Read %d bytes from %s" % (len(buffer), key,))

            if self.shutdown_gracefully:
                log.warn("Discarding data, in graceful shutdown mode")
                continue

            if key not in self.sessions:
                log.debug(
                    "Creating new server context for session key = %s",
                    key
                )

                self.sessions[key] = TftpContextServer(
                    raddress,
                    rport,
                    timeout,
                    self.root,
                    self.dyn_file_func,
                    readysock
                )

                try:
                    self.sessions[key].start(buffer)
                except TftpFileNotFound, err:
                    self.deletion_list.append(key)
                    log.debug("File not found for session %s: %s"
                        % (key, str(err)))
                except TftpException, err:
                    self.deletion_list.append(key)
                    log.error("Fatal exception thrown from "
                              "session %s: %s" % (key, str(err)))
            else:
                try:
                    self.sessions[key].cycle(buffer)
                    if self.sessions[key].state is None:
                        log.debug("Successful transfer.")
                        self.deletion_list.append(key)
                except TftpFileNotFound, err:
                    self.deletion_list.append(key)
                    log.debug("File not found for session %s: %s"
                        % (key, str(err)))
                except TftpException, err:
                    self.deletion_list.append(key)
                    log.error("Fatal exception thrown from "
                              "session %s: %s"
                              % (key, str(err)))

    def standard_loop(self, timeout):
        # Build the inputlist array of sockets to select() on.
        inputlist = []
        inputlist.append(self.sock)
        for key in self.sessions:
            inputlist.append(self.sessions[key].sock)

        # Block until some socket has input on it.
        log.debug("Performing select on this inputlist: %s", inputlist)
        readyinput, readyoutput, readyspecial = select.select(
            inputlist,
            [],
            [],
            SOCK_TIMEOUT
        )

        # Handle the available data, if any. Maybe we timed-out.
        for readysock in readyinput:
            # Is the traffic on the main server socket? ie. new session?
            if readysock == self.sock:
                log.debug("Data ready on our main socket")
                buffer, (raddress, rport) = self.sock.recvfrom(MAX_BLKSIZE)

                log.debug("Read %d bytes", len(buffer))

                if self.shutdown_gracefully:
                    log.warn("Discarding data on main port, in graceful shutdown mode")
                    continue

                # Forge a session key based on the client's IP and port,
                # which should safely work through NAT.
                key = "%s:%s" % (raddress, rport)

                if not self.sessions.has_key(key):
                    log.debug("Creating new server context for "
                                 "session key = %s", key)
                    self.sessions[key] = TftpContextServer(raddress,
                                                           rport,
                                                           timeout,
                                                           self.root,
                                                           self.dyn_file_func)
                    try:
                        self.sessions[key].start(buffer)
                    except TftpException, err:
                        self.deletion_list.append(key)
                        log.error("Fatal exception thrown from "
                                  "session %s: %s" % (key, str(err)))
                else:
                    log.warn("received traffic on main socket for "
                             "existing session??")
                log.debug("Currently handling these sessions:")
                for session_key, session in self.sessions.items():
                    log.debug("    %s" % session)

            else:
                # Must find the owner of this traffic.
                for key in self.sessions:
                    if readysock == self.sessions[key].sock:
                        log.debug("Matched input to session key %s"
                            % key)
                        try:
                            self.sessions[key].cycle()
                            if self.sessions[key].state == None:
                                log.debug("Successful transfer.")
                                self.deletion_list.append(key)
                        except TftpException, err:
                            self.deletion_list.append(key)
                            log.error("Fatal exception thrown from "
                                      "session %s: %s"
                                      % (key, str(err)))
                        # Break out of for loop since we found the correct
                        # session.
                        break

                else:
                    log.error("Can't find the owner for this packet. "
                              "Discarding.")

    def stop(self, now=False):
        """Stop the server gracefully. Do not take any new transfers,
        but complete the existing ones. If force is True, drop everything
        and stop. Note, immediately will not interrupt the select loop, it
        will happen when the server returns on ready data, or a timeout.
        ie. SOCK_TIMEOUT"""
        if now:
            self.shutdown_immediately = True
        else:
            self.shutdown_gracefully = True
