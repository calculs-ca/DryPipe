from datetime import timedelta


class SlurmJobStateCode:
    def __init__(self, short_code, long_code):
        self.short_code = short_code
        self.long_code = long_code

    def __str__(self):
        return self.long_code

    def __repr__(self):
        return self.long_code

    def __eq__(self, other):
        return self.long_code == other.long_code

    def __hash__(self):
        return hash(self.long_code)


class SlurmJobStateCodes:
    """
        https://slurm.schedmd.com/job_state_codes.html
    """

    """
    BF BOOT_FAIL
    Job terminated due to launch failure, typically due to a hardware failure (e.g. unable to boot the node or block and the job can not be requeued).
    """
    BOOT_FAIL = SlurmJobStateCode("BF", "BOOT_FAIL")

    """
    CA CANCELLED
    Job was explicitly cancelled by the user or system administrator. The job may or may not have been initiated.    
    """
    CANCELLED = SlurmJobStateCode("CA", "CANCELLED")

    """
    CD COMPLETED
    Job has terminated all processes on all nodes with an exit code of zero.
    """
    COMPLETED = SlurmJobStateCode("CD", "COMPLETED")

    """
    CF CONFIGURING
    Job has been allocated resources, but are waiting for them to become ready for use (e.g. booting).
    """
    CONFIGURING = SlurmJobStateCode("CF", "CONFIGURING")

    """
    CG COMPLETING
    Job is in the process of completing. Some processes on some nodes may still be active.
    """
    COMPLETING = SlurmJobStateCode("CG", "COMPLETING")

    """
    DL DEADLINE
    Job terminated on deadline.
    """
    DEADLINE = SlurmJobStateCode("DL", "DEADLINE")

    """
    F FAILED
    Job terminated with non-zero exit code or other failure condition.
    """
    FAILED = SlurmJobStateCode("F", "FAILED")

    """
    NF NODE_FAIL
    Job terminated due to failure of one or more allocated nodes.
    """
    NODE_FAIL = SlurmJobStateCode("NF", "NODE_FAIL")

    """
    OOM OUT_OF_MEMORY
    Job experienced out of memory error.
    """
    OUT_OF_MEMORY = SlurmJobStateCode("OOM", "OUT_OF_MEMORY")

    """
    PD PENDING
    Job is awaiting resource allocation.
    """
    PENDING = SlurmJobStateCode("PD", "PENDING")

    """
    PR PREEMPTED
    Job terminated due to preemption.
    """
    PREEMPTED = SlurmJobStateCode("PR", "PREEMPTED")

    """
    R RUNNING
    Job currently has an allocation.
    """
    RUNNING = SlurmJobStateCode("R", "RUNNING")

    """
    RD RESV_DEL_HOLD
    Job is being held after requested reservation was deleted.
    """
    RESV_DEL_HOLD = SlurmJobStateCode("RD", "RESV_DEL_HOLD")

    """
    RF REQUEUE_FED
    Job is being requeued by a federation.
    """
    REQUEUE_FED = SlurmJobStateCode("RF", "REQUEUE_FED")

    """
    RH REQUEUE_HOLD
    Held job is being requeued.
    """
    REQUEUE_HOLD = SlurmJobStateCode("RH", "REQUEUE_HOLD")

    """
    RQ REQUEUED
    Completing job is being requeued.
    """

    REQUEUED = SlurmJobStateCode("RQ", "REQUEUED")

    """
    RS RESIZING
    Job is about to change size.
    """
    RESIZING = SlurmJobStateCode("RS", "RESIZING")

    """
    RV REVOKED
    Sibling was removed from cluster due to other cluster starting the job.
    """
    REVOKED = SlurmJobStateCode("RV", "REVOKED")

    """
    SI SIGNALING
    Job is being signaled.
    """
    SIGNALING = SlurmJobStateCode("SI", "SIGNALING")

    """
    SE SPECIAL_EXIT
    The job was requeued in a special state. This state can be set by users, typically in EpilogSlurmctld, if the job has terminated with a particular exit value.
    """
    SPECIAL_EXIT = SlurmJobStateCode("SE", "SPECIAL_EXIT")

    """
    SO STAGE_OUT
    Job is staging out files.
    """
    STAGE_OUT = SlurmJobStateCode("SO", "STAGE_OUT")

    """
    ST STOPPED
    Job has an allocation, but execution has been stopped with SIGSTOP signal. CPUS have been retained by this job.
    """
    STOPPED = SlurmJobStateCode("ST", "STOPPED")

    """
    S SUSPENDED
    Job has an allocation, but execution has been suspended and CPUs have been released for other jobs.
    """
    SUSPENDED = SlurmJobStateCode("S", "SUSPENDED")

    """
    TO TIMEOUT
    Job terminated upon reaching its time limit.
    """
    TIMEOUT = SlurmJobStateCode("TO", "TIMEOUT")


    is_running_or_will_run = {PENDING, RUNNING, COMPLETING, CONFIGURING}

    has_provably_ended = {
        FAILED, COMPLETED, TIMEOUT, STOPPED, PREEMPTED, REVOKED, SPECIAL_EXIT,
        BOOT_FAIL, CANCELLED, DEADLINE, OUT_OF_MEMORY, NODE_FAIL
    }

    is_in_queue_or_in_progression = {PENDING, RUNNING, COMPLETING, CONFIGURING}

    failed_cancelled_or_timed_out = {FAILED, CANCELLED, TIMEOUT}

    @classmethod
    def all_codes_by_long_code(cls):
        for _, o in cls.__dict__.items():
            if isinstance(o, SlurmJobStateCode):
                yield o.long_code, o

    @classmethod
    def all_codes_by_short_code(cls):
        for _, o in cls.__dict__.items():
            if isinstance(o, SlurmJobStateCode):
                yield o.short_code, o


class SlurmJobStateShortCodes:

    _all_codes_by_short_code = dict(SlurmJobStateCodes.all_codes_by_short_code())

    @staticmethod
    def _lookup_code(short_code):
        code = SlurmJobStateShortCodes._all_codes_by_short_code.get(short_code)
        if code is None:
            raise Exception(f"Code {short_code} not found.")
        return code

    @staticmethod
    def is_running_or_will_run(shot_code):
        return SlurmJobStateShortCodes._lookup_code(shot_code) in SlurmJobStateCodes.is_running_or_will_run

class SlurmJobStateLongCodes:

    _all_codes_by_long_code = dict(SlurmJobStateCodes.all_codes_by_long_code())

    @staticmethod
    def _lookup_code(long_code):
        code = SlurmJobStateLongCodes._all_codes_by_long_code.get(long_code)
        if code is None:
            raise Exception(f"Code {long_code} not found.")
        return code


    @staticmethod
    def has_provably_ended(long_code: str):
        code = SlurmJobStateLongCodes._lookup_code(long_code)
        return code in SlurmJobStateCodes.has_provably_ended

    @staticmethod
    def is_completed(long_code: str):
        return SlurmJobStateCodes.COMPLETED.long_code == long_code

    @staticmethod
    def has_failed(long_code: str):
        return SlurmJobStateCodes.FAILED.long_code == long_code

    @staticmethod
    def is_canceled(long_code: str):
        return SlurmJobStateCodes.CANCELLED.long_code == long_code

    @staticmethod
    def pending_or_running(long_code: str):
        return SlurmJobStateLongCodes._lookup_code(long_code) in SlurmJobStateCodes.is_running_or_will_run

    @staticmethod
    def has_failed_cancelled_or_timed_out(long_code: str):
        return SlurmJobStateLongCodes._lookup_code(long_code) in SlurmJobStateCodes.failed_cancelled_or_timed_out




class SlurmTime:
    def __init__(self, time_input):
        if isinstance(time_input, timedelta):
            self.td = time_input
        elif isinstance(time_input, str):
            self.td = self._parse_string(time_input)
        else:
            raise TypeError("must be str or timedelta.")

    def _parse_string(self, time_str: str) -> timedelta:
        if "-" in time_str:
            days_part, time_part = time_str.split("-")
            days = int(days_part)
        else:
            days = 0
            time_part = time_str

        parts = list(map(int, time_part.split(":")))
        
        if len(parts) == 3:    # HH:MM:SS
            return timedelta(days=days, hours=parts[0], minutes=parts[1], seconds=parts[2])
        elif len(parts) == 2:  # MM:SS
            return timedelta(days=days, minutes=parts[0], seconds=parts[1])
        else:
            raise ValueError(f"Format SLURM invalide ou non supporté : {time_str}")

    def __mul__(self, multiplier: int) -> 'SlurmTime':
        if not isinstance(multiplier, int):
            raise TypeError(f"multiplier must be int, got {multiplier}")
        return SlurmTime(self.td * multiplier)

    def __rmul__(self, multiplier: int) -> 'SlurmTime':
        return self.__mul__(multiplier)

    def __str__(self) -> str:
        days = self.td.days
        remaining_seconds = self.td.seconds
        
        hours = remaining_seconds // 3600
        minutes = (remaining_seconds % 3600) // 60
        seconds = remaining_seconds % 60
        
        if days > 0:
            return f"{days:02d}-{hours:02d}:{minutes:02d}:{seconds:02d}"
        else:
            return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

    def __repr__(self) -> str:
        return f"SlurmTime('{self.__str__()}')"
