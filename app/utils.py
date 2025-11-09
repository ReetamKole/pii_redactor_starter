import re
import logging
logger = logging.getLogger("app.utils")
EMAIL_RE = re.compile(
    r"\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b",
    re.IGNORECASE
)

PHONE_RE = re.compile(
    r"""
    (?<!\d)                             
    (?:\+?\d{1,3}[\s\-.]?)?             
    (?:\(?\d{2,4}\)?[\s\-.]?)?          
    (?:\d[\s\-.]?){6,14}\d              
    (?!\d)                              
    """,
    re.VERBOSE,
)
PHONE_RE = re.compile(
    r"""
    (?<!\d)          
    (?:
        \+?\d{1,3}   
        [\s\-.]?
    )?
    (?:
        \(?\d{2,4}\)? 
        [\s\-.]?
    )?
    (?:
        \d[\s\-.]?   
    ){6,14}
    \d              
    (?!\d)        
    """,
    re.VERBOSE,
)

CREDIT_CARD_RE = re.compile(
    r"\b\d{15,16}\b"
)

SSN_RE = re.compile(
    r"\b\d{3}-\d{2}-\d{4}\b"
)

DOB_RE = re.compile(
    r"\b(19\d{2}|20\d{2})-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])\b"
)

def redact_text(text: str) -> str:
    if not isinstance(text, str):
        text = str(text)
    text = EMAIL_RE.sub("[REDACTED_EMAIL]", text)
    
    # is_cc    = bool(CREDIT_CARD_RE.search(text))
    # is_ssn   = bool(SSN_RE.search(text))
    # is_dob   = bool(DOB_RE.search(text))
    
    
    # logger.info(f"PII Check: CC={is_cc}, SSN={is_ssn}, DOB={is_dob}")
    # if not(is_cc or is_ssn or is_dob):
    text = PHONE_RE.sub("[REDACTED_PHONE]", text)

    return text

def is_valid_phone(phone: str) -> bool:
    if not isinstance(phone, str):
        return False
    digits = "".join(ch for ch in phone if ch.isdigit())
    return 7 <= len(digits) <= 15