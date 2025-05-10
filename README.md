# WvW Skirmish Analysis

This repository contains code for analyzing Guild Wars 2's World vs. World (WvW) skirmish matches.
I'd upload the data myself but the combined .csv files are about 250GB, and the combined .json files are about 2.6GB. Github doesn't like that.
But fortunately, the code builds these files itself.

This code, and resulting data, is subject to the same existing limitations of the API, specifically, that reported kills/deaths do not match in-game reports.
Interpret at your own risk, with that caveat in mind.

## Contents

- `skirmish_analysis.py`: Main Python script for parsing match JSON data, engineering features, and exporting summarized CSV files.
- World vs. World Per Match JSON Files: Expected file folder with an example of a match skirmish included.
- World vs. World .csv Fragments: Expected file folder with an example of a .csv fragment that skirmish_analysis.py produces.
- `README.md`: This file.

## Features Engineered

The base .json files contain information on kills, deaths, and score per skirmish.
They also contain teamname, team color, link team name, tier, and region.
I've also constructed extra information on skirmish ID (absolute and relative), day of week, hour, and any running events/holidays.
I've also attempted to infer when there's sporadic API downtime based on periods where kills and deaths are both zero, but this is very hacky.

## How To Use
- Make a new folder, call it whatever you want, put it wherever you want.
- Download "skirmish_analysis.py"
- [Optionally] Download the World vs. World Per Match JSON Files and World vs. World .csv Fragments folders. Put them inside of the new folder you created.
- Run skirmish_analysis.py and wait about an hour and a half while the initial data compiles itself.
- You can add subsequent skirmishes as they happen by re-running skirmish_analysis.py.

Directory structure of the code should be:

/{name of your directory}
  skirmish_analysis.py
  /World vs. World Per Match JSON Files
  /World vs. World .csv Fragments

## Future Plans

I'd like to add different VP maps to the data, but I haven't done that yet.
I'd like to add major WvW balance patches to see if they change activity levels, but I haven't done that yet.
If you want to take a crack at it, you can email me: sheffplaysgames@gmail.com || Sheff on Discord

## Requirements

- Python 3.9+
- Pretty sure it's all standard library beyond that.
