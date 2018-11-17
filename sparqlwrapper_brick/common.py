import urllib.parse
url_encode = urllib.parse.quote_plus

def normalize_uri(s):
    assert isinstance(s, str)
    s = s.replace(' ', '_')
    return url_encode(s)
