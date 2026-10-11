import sys
from urllib.request import urlopen

with urlopen(sys.argv[1], timeout=3) as response:
    assert response.status == 200
