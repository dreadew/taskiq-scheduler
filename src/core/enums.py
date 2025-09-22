from enum import Enum


class TaskStatus(str, Enum):
    """
    Enum со статусами задач
    """

    SCHEDULED = "SCHEDULED"
    RUNNING = "RUNNING"
    CANCELLING = "CANCELLING"
    CANCELLED = "CANCELLED"
    DONE = "DONE"
    FAILED = "FAILED"
    STOPPED = "STOPPED"
