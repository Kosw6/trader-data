"""Allowlisted error metadata: never serialize a request, body, URL, or headers."""
import re


def error_metadata(error):
    result = {'httpStatus': None, 'providerErrorCode': None, 'retryAfterSeconds': None}
    response = getattr(error, 'response', None)
    if response is not None:
        status = getattr(response, 'status_code', None)
        if isinstance(status, int) and 100 <= status <= 599:
            result['httpStatus'] = status
        retry_after = response.headers.get('Retry-After', '')
        if isinstance(retry_after, str) and len(retry_after) <= 6 and retry_after.isdigit():
            result['retryAfterSeconds'] = min(int(retry_after), 86400)
        try:
            body = response.json()
            code = body.get('msg_cd') if isinstance(body, dict) else None
            if isinstance(code, str) and re.fullmatch(r'[A-Z0-9_]{1,32}', code):
                result['providerErrorCode'] = code
        except (ValueError, TypeError):
            pass
    if result['providerErrorCode'] is None:
        code = re.search(r'KIS daily price failed: ([A-Z0-9_]{1,32})(?:\s|$)', str(error))
        if code:
            result['providerErrorCode'] = code.group(1)
    status, code = result['httpStatus'], result['providerErrorCode']
    result['failureCategory'] = ('RATE_LIMIT' if status == 429 or code == 'EGW00201'
                                 else 'PROVIDER_5XX' if status is not None and status >= 500
                                 else 'HTTP_OTHER' if status is not None and status >= 400
                                 else 'UNCLASSIFIED')
    return result
