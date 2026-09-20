import logging
import traceback
import sys
import re
from typing import Any
from .db import insert_system_log


AUTH_KEYWORDS = [
    r"\b401\b",
    r"\bunauthorized\b",
    r"\bforbidden\b",
    r"\b403\b",
    r"\binvalid credentials\b",
    r"\bmfa\b",
    r"\blogin failed\b",
    r"\btoken expired\b",
    r"\btoken invalid\b",
    r"\bsession invalid\b",
    r"\bauth failed\b",
    r"\bauthentication failed\b",
    r"\bre-authenticate\b",
    r"\binvalid token\b",
    r"\binvalid_grant\b",
    r"\btoken store\b",
    r"\boauth\b",
    r"\bprompt_mfa\b",
]

CONNECTION_KEYWORDS = [
    r"\btimeout\b",
    r"\btimed out\b",
    r"\bconnection error\b",
    r"\bconnection refused\b",
    r"\bnetwork unreachable\b",
    r"\breset by peer\b",
    r"\b502\b",
    r"\b503\b",
    r"\b504\b",
    r"\bbad gateway\b",
    r"\bservice unavailable\b",
    r"\bgateway timeout\b",
    r"\bfailed to establish a new connection\b",
    r"\bmax retries exceeded\b",
    r"\bconnecterror\b",
    r"\btimeouterror\b",
    r"\brate limit\b",
    r"\b429\b",
    r"\btoo many requests\b",
    r"\bsocket\b",
]


def classify_log_event(
    message: str,
    details: str | None = None,
    explicit_category: str | None = None,
    explicit_is_auth: bool | None = None,
    explicit_is_conn: bool | None = None,
) -> tuple[str, bool, bool]:
    """
    Classifies a log entry into (category, is_auth_issue, is_connection_issue).
    """
    combined_text = f"{message} {details or ''}".lower()

    is_auth = explicit_is_auth
    if is_auth is None:
        is_auth = any(re.search(pat, combined_text, re.IGNORECASE) for pat in AUTH_KEYWORDS)

    is_conn = explicit_is_conn
    if is_conn is None:
        is_conn = any(re.search(pat, combined_text, re.IGNORECASE) for pat in CONNECTION_KEYWORDS)

    if explicit_category and explicit_category.lower() != "auto":
        category = explicit_category.lower()
    else:
        if is_auth:
            category = "auth"
        elif is_conn:
            category = "connection"
        elif "sync" in combined_text:
            category = "sync"
        elif "api" in combined_text:
            category = "api"
        elif "database" in combined_text or "sqlite" in combined_text:
            category = "database"
        else:
            category = "general"

    return category, bool(is_auth), bool(is_conn)


class WalkiesDBLogHandler(logging.Handler):
    """
    Standard logging Handler that persists log records to the SQLite database
    and automatically classifies authentication vs connection issues.
    """

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            details = None

            if record.exc_info:
                details = "".join(traceback.format_exception(*record.exc_info))
            elif getattr(record, "details", None):
                details = str(record.details)

            service = getattr(record, "service", None)
            if not service:
                # Infer service from logger name or message
                if "garmin" in record.name.lower() or "garmin" in msg.lower():
                    service = "garmin"
                elif "carelink" in record.name.lower() or "carelink" in msg.lower():
                    service = "carelink"
                elif "google" in record.name.lower() or "auth" in record.name.lower():
                    service = "google_auth"
                else:
                    service = "server"

            user_sub = getattr(record, "user_sub", None)
            if not user_sub:
                user_sub_match = re.search(r"user sub:?\s*([a-zA-Z0-9_\-\.]+)", msg)
                if user_sub_match:
                    user_sub = user_sub_match.group(1)

            explicit_category = getattr(record, "category", None)
            explicit_is_auth = getattr(record, "is_auth_issue", None)
            explicit_is_conn = getattr(record, "is_connection_issue", None)

            category, is_auth, is_conn = classify_log_event(
                msg,
                details=details,
                explicit_category=explicit_category,
                explicit_is_auth=explicit_is_auth,
                explicit_is_conn=explicit_is_conn,
            )

            insert_system_log(
                level=record.levelname,
                category=category,
                service=service,
                message=msg,
                details=details,
                user_sub=user_sub,
                is_auth_issue=is_auth,
                is_connection_issue=is_conn,
            )
        except Exception as e:
            # Never let logging failure crash the main app
            sys.stderr.write(f"[WalkiesDBLogHandler ERROR] Failed to persist log: {e}\n")


def log_system_event(
    level: str,
    message: str,
    category: str | None = None,
    service: str | None = None,
    details: Any = None,
    user_sub: str | None = None,
    is_auth_issue: bool | None = None,
    is_connection_issue: bool | None = None,
) -> int:
    """
    Direct helper to log a structured system event with explicit or auto-classified categories.
    """
    details_str = None
    if details is not None:
        if isinstance(details, Exception):
            details_str = "".join(traceback.format_exception(type(details), details, details.__traceback__))
        elif isinstance(details, (dict, list, str)):
            details_str = details
        else:
            details_str = str(details)

    cat, is_auth, is_conn = classify_log_event(
        message,
        details=str(details_str) if details_str is not None else None,
        explicit_category=category,
        explicit_is_auth=is_auth_issue,
        explicit_is_conn=is_connection_issue,
    )

    return insert_system_log(
        level=level,
        category=cat,
        service=service,
        message=message,
        details=details_str,
        user_sub=user_sub,
        is_auth_issue=is_auth,
        is_connection_issue=is_conn,
    )
