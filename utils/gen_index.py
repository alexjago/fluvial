import subprocess
import os.path

import argparse

parser = argparse.ArgumentParser("Create an index of Fluvial-output SVGs")
parser.add_argument("directory", default=".")
parser.add_argument("-m", default="MONTH", help="month")
parser.add_argument("-y", default="YEAR", help="year")


args = parser.parse_args()

SVGs = subprocess.run(["find", args.directory, "-type", "f", "-name", "*.svg"],
                       stdout=subprocess.PIPE,
                       universal_newlines=True)\
        .stdout.strip().split(sep="\n")

combine = {}

for sf in SVGs:
    s = os.path.splitext(os.path.basename(sf))[0]

    route, direction = s.split('_')

    if route not in combine:
        combine[route] = [(route, direction, sf)]
    else:
        combine[route].append((route, direction, sf))


with open(os.path.join(args.directory, "index.html"), 'w') as fn:
    print(f'<html> \n\
<head> \n\
<link rel="stylesheet" href="https://abjago.net/abjago.css"> \n\
<style type="text/css"> \n\
    h4 {{margin-left: 1vw}} \n\
</style> \n\
</head> \n\
<body> \n\
<h4>{args.m} {args.y}</h4> \n\
<table>', file=fn)

    for t in sorted(combine.keys()):
        sr = sorted(combine[t])

        txt = "<tr>"

        for r, d, p in sr:
            txt += f'<td><a href="{p}">{r} {d}</a></td>'

        txt += "</tr>"
        print(txt, file=fn)

    print('</table>\n</body>\n</html>', file=fn)
