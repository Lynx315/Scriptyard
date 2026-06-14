# vibe-coded

import requests

url = "https://raw.githubusercontent.com/StevenBlack/hosts/master/alternates/porn/hosts"

text = requests.get(url).text

with open("StevenBlack_htmlremove_porn_filters.txt", "w", encoding="utf-8") as f:

    f.write("! Generated from StevenBlack porn hosts list\n\n")

with open("StevenBlack_htmlremove_porn_filters.txt", "a", encoding="utf-8") as f:

    for line in text.splitlines():
        line = line.strip()

        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) < 2:

            continue
        host = parts[1]
        if host in ("localhost", "localhost.localdomain"):

            continue
        f.write(f"{host}##html:remove()\n")

print("Saved to StevenBlack_htmlremove_porn_filters.txt")