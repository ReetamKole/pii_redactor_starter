import re

EMAIL_RE = re.compile(r"\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b")

SSN_RE = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")

DOB_RE = re.compile(
    r"\b("
        r"(19\d{2}|20\d{2})-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])"         
        r"|"
        r"(0[1-9]|[12]\d|3[01])-(0[1-9]|1[0-2])-(19\d{2}|20\d{2})"        
        r"|"
        r"(0[1-9]|1[0-2])/(0[1-9]|[12]\d|3[01])/(19\d{2}|20\d{2})"         
        r"|"
        r"(0[1-9]|[12]\d|3[01])/(0[1-9]|1[0-2])/(19\d{2}|20\d{2})"        
    r")\b"
)

CC_RE_FLEX = re.compile(r"\b(?:\d[ -]?){13,19}\b")

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

MASTER = re.compile(
    rf"""(
        (?P<email>{EMAIL_RE.pattern})
      | (?P<ssn>{SSN_RE.pattern})
      | (?P<dob>{DOB_RE.pattern})
      | (?P<cc>{CC_RE_FLEX.pattern})
      | (?P<phone>{PHONE_RE.pattern})
    )""",
    re.VERBOSE,
)

def redact_text(text: str) -> str:
    if not isinstance(text, str):
        text = str(text)

    def repl(m: re.Match) -> str:
        g = m.groupdict()

        if g.get("email"):
            return "[REDACTED_EMAIL]"

        if g.get("ssn") or g.get("dob") or g.get("cc"):
            return m.group(0)

        if g.get("phone"):
            return "[REDACTED_PHONE]"

        return m.group(0)

    return MASTER.sub(repl, text)


def is_valid_phone(phone: str) -> bool:
    """Enhanced phone validation with anomaly detection"""
    if not isinstance(phone, str):
        return False
   
    digits = re.sub(r"\D", "", phone)
    
    if not (7 <= len(digits) <= 15):
        return False
  
    if len(set(digits)) == 1:
        return False
    
    if is_sequential(digits):
        return False
   
    invalid_patterns = [
        '0000000000', '1111111111', '2222222222', '3333333333',
        '4444444444', '5555555555', '6666666666', '7777777777',
        '8888888888', '9999999999', '1234567890', '0987654321'
    ]
    
    if digits in invalid_patterns:
        return False
    
    return True


def is_valid_email(email: str) -> bool:
    """Enhanced email validation with anomaly detection"""
    if not isinstance(email, str):
        return False
 
    if not EMAIL_RE.match(email):
        return False
    
    try:
        local, domain = email.rsplit('@', 1)
    except ValueError:
        return False
 
    if len(local) == 0 or len(local) > 64:
        return False
    if len(domain) == 0 or len(domain) > 255:
        return False
    
    suspicious_patterns = [
        'test@test.com', 'admin@admin.com', 'user@user.com',
        'example@example.com', 'fake@fake.com', 'dummy@dummy.com'
    ]
    
    if email.lower() in suspicious_patterns:
        return False
    
    # Check for repeated characters
    if len(set(local.replace('.', '').replace('_', '').replace('-', ''))) <= 2 and len(local) > 3:
        return False
    
    domain_parts = domain.split('.')
    if len(domain_parts) < 2:
        return False
    
    tld = domain_parts[-1]
    if len(tld) < 2 or not tld.isalpha():
        return False
    
    return True


def is_sequential(digits: str) -> bool:
    """Check if digits are in sequential order"""
    if len(digits) < 4:
        return False
    ascending = all(int(digits[i]) == int(digits[i-1]) + 1 for i in range(1, len(digits)))
    descending = all(int(digits[i]) == int(digits[i-1]) - 1 for i in range(1, len(digits)))
    
    return ascending or descending


def detect_anomalies(name: str, email: str, phone: str) -> dict:
    """Detect anomalies in user input data"""
    anomalies = {
        "has_anomaly": False,
        "anomaly_details": []
    }
    if not is_valid_email(email):
        anomalies["has_anomaly"] = True
        anomalies["anomaly_details"].append({
            "field": "email",
            "value": email,
            "issue": "Invalid or suspicious email format"
        })
    if not is_valid_phone(phone):
        anomalies["has_anomaly"] = True
        anomalies["anomaly_details"].append({
            "field": "phone",
            "value": phone,
            "issue": "Invalid or suspicious phone format"
        })
    if not name or len(name.strip()) < 2:
        anomalies["has_anomaly"] = True
        anomalies["anomaly_details"].append({
            "field": "name",
            "value": name,
            "issue": "Name too short or empty"
        })
    suspicious_names = ['test', 'admin', 'user', 'dummy', 'fake', 'example']
    if name.lower().strip() in suspicious_names:
        anomalies["has_anomaly"] = True
        anomalies["anomaly_details"].append({
            "field": "name",
            "value": name,
            "issue": "Suspicious test/dummy name detected"
        })
    
    return anomalies
