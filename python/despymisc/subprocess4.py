"""
    .. _despymisc-subprocess4:

    **subprocess4**
    ---------------

    Contains a subclass of subprocess.Popen, but defines wait4 method to make sure
    it waits properly for completion.

"""

import subprocess
import os
import errno
import signal
import psutil

class Popen(subprocess.Popen):
    """
        This class defines a subclass of the subprocess.Popen module.
        It defines a wait4 method to ensure that the wait is properly
        done.

        Parameters
        ----------
        args : various
            Passed directly to subprocess.Popen

        kwargs : dict
            Passed directly to subprocess.Popen
    """
    def __init__(self, args, **kwargs):
        self.rusage = None
        subprocess.Popen.__init__(self, args, **kwargs)

    def wait4(self):
        #pylint: disable=no-member
        """ Wait for child process to terminate.
            Returns returncode attribute.

            Returns
            -------
            int
                The return code from whatever was executed by Popen.

            Raises
            ------
            OSError
                If there is an error waiting.
            """

        while self.returncode is None:
            try:
                (pid, sts, rusage) = os.wait4(self.pid, 0)
                self.rusage = rusage
            except OSError as exc:
                if exc.errno != errno.ECHILD:
                    raise

                # This happens if SIGCLD is set to be ignored or waiting
                # for child processes has otherwise been disabled for our
                # process.  This child is dead, we can't get the status.
                pid = self.pid
                sts = 0

            # Check the pid and loop as waitpid has been known to return
            # 0 even without WNOHANG in odd situations.  issue14396.
            if pid == self.pid:
                self._handle_exitstatus(sts)
            else:
                try:
                    _ = psutil.Process(self.pid)
                except psutil.NoSuchProcess:
                    print 'Process finished but wait4() returned a mismatched pid'
                    self.returncode = 1

        if self.returncode == -signal.SIGSEGV:
            print "SEGMENTATION FAULT"
        return self.returncode
