

class SlurmJobStateCodes:
    """
        https://slurm.schedmd.com/job_state_codes.html
    """

    """
    BF BOOT_FAIL
    Job terminated due to launch failure, typically due to a hardware failure (e.g. unable to boot the node or block and the job can not be requeued).
    """
    BOOT_FAIL = "BF"

    """
    CA CANCELLED
    Job was explicitly cancelled by the user or system administrator. The job may or may not have been initiated.    
    """
    CANCELLED = "CA"

    """
    CD COMPLETED
    Job has terminated all processes on all nodes with an exit code of zero.
    """
    COMPLETED = "CD"

    """
    CF CONFIGURING
    Job has been allocated resources, but are waiting for them to become ready for use (e.g. booting).
    """
    CONFIGURING = "CF"

    """
    CG COMPLETING
    Job is in the process of completing. Some processes on some nodes may still be active.
    """
    COMPLETING = "CG"

    """
    DL DEADLINE
    Job terminated on deadline.
    """
    DEADLINE = "DL"

    """
    F FAILED
    Job terminated with non-zero exit code or other failure condition.
    """
    FAILED = "F"

    """
    NF NODE_FAIL
    Job terminated due to failure of one or more allocated nodes.
    """
    NODE_FAIL = "NF"

    """
    OOM OUT_OF_MEMORY
    Job experienced out of memory error.
    """
    OUT_OF_MEMORY = "OOM"

    """
    PD PENDING
    Job is awaiting resource allocation.
    """
    PENDING = "PD"

    """
    PR PREEMPTED
    Job terminated due to preemption.
    """
    PREEMPTED = "PR"

    """
    R RUNNING
    Job currently has an allocation.
    """
    RUNNING = "R"

    """
    RD RESV_DEL_HOLD
    Job is being held after requested reservation was deleted.
    """
    RESV_DEL_HOLD = "RD"

    """
    RF REQUEUE_FED
    Job is being requeued by a federation.
    """
    REQUEUE_FED = "RF"

    """
    RH REQUEUE_HOLD
    Held job is being requeued.
    """
    REQUEUE_HOLD = "RH"

    """
    RQ REQUEUED
    Completing job is being requeued.
    """

    REQUEUED = "RQ"

    """
    RS RESIZING
    Job is about to change size.
    """
    RESIZING = "RS"

    """
    RV REVOKED
    Sibling was removed from cluster due to other cluster starting the job.
    """
    REVOKED = "RV"

    """
    SI SIGNALING
    Job is being signaled.
    """
    SIGNALING = "SI"

    """
    SE SPECIAL_EXIT
    The job was requeued in a special state. This state can be set by users, typically in EpilogSlurmctld, if the job has terminated with a particular exit value.
    """
    SPECIAL_EXIT = "SE"

    """
    SO STAGE_OUT
    Job is staging out files.
    """
    STAGE_OUT = "SO"

    """
    ST STOPPED
    Job has an allocation, but execution has been stopped with SIGSTOP signal. CPUS have been retained by this job.
    """
    STOPPED = "ST"

    """
    S SUSPENDED
    Job has an allocation, but execution has been suspended and CPUs have been released for other jobs.
    """
    SUSPENDED = "S"

    """
    TO TIMEOUT
    Job terminated upon reaching its time limit.
    """
    TIMEOUT = "TO"


    _is_running_or_will_run = {PENDING, RUNNING, COMPLETING, CONFIGURING}

    _can_no_longer_run = {
        FAILED, COMPLETED, TIMEOUT, STOPPED, PREEMPTED, REVOKED, SPECIAL_EXIT,
        BOOT_FAIL, CANCELLED, DEADLINE,OUT_OF_MEMORY, NODE_FAIL
    }

    @staticmethod
    def is_running_or_will_run(slurm_code, logger=None):
        if slurm_code in SlurmJobStateCodes._is_running_or_will_run:
            return True
        elif slurm_code in SlurmJobStateCodes._can_no_longer_run:
            return False

        if logger is None:
            logger.warning("rare code: %s", slurm_code)

        return False