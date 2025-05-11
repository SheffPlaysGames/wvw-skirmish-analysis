"""
A series of functions and operations designed to grab historical
World vs. World data off of kills.werdes.net and process them down to a
single .csv file that can be used for analyses or data exploration.

Comments or questions about this code can be directed to
sheffplaysgames@gmail.com or Sheff on Discord.
"""

import argparse
from datetime import datetime, timedelta
import json
import logging
import pathlib
import time

# pypi libraries
import pandas as pd
import requests

logging.basicConfig(level=logging.DEBUG)

# Configure a session for retries and pipelining
session = requests.Session()
retry = requests.adapters.Retry(total=3)
session.mount("https://api.kills.werdes.net/",
              requests.adapters.HTTPAdapter(max_retries=retry))

# Setting up some global variables to control file placements.
cwd = pathlib.Path('.')
all_json_dir = cwd / "wvw_per_match_json_files"
merged_json_dir = cwd / "merged"
merged_json_name = pathlib.Path("all_combined_matches.json")
all_csv_dir = cwd / "wvw_per_match_csv_files"
merged_csv_dir = cwd / "merged"
merged_csv_name = pathlib.Path("all_combined_matches.csv")

merged_json_dir.mkdir(parents=True, exist_ok=True)
merged_csv_dir.mkdir(parents=True, exist_ok=True)
all_json_dir.mkdir(parents=True, exist_ok=True)
all_csv_dir.mkdir(parents=True, exist_ok=True)

# These dictionaries contain different skirmish point values for comparisons.
# Dict keys map to the skirmish IDs that we make later.
# Skirmish 1 is the reset skirmish for all of these dictionaries.
# TODO: Add VP values into the resulting .csv file.


def convert_keys_to_int(skirm_dict):
    return dict((int(k), v) for k, v in skirm_dict.items())


def dt(d):
    return datetime.strptime(d, "%Y-%m-%d")


logging.debug("Loading Victory Point data")
with open('lookup_tables/skirmish_vp.json', 'r') as fd:
    j = json.load(fd)
    skirmish_vp_old = convert_keys_to_int(j['initial'])
    # November 22, 2024 to March 28, 2025
    skirmish_vp_2024_na = convert_keys_to_int(j['2024na'])
    skirmish_vp_2024_eu = convert_keys_to_int(j['2024eu'])
    # March 28, 2025 to current
    skirmish_vp_2025_na = convert_keys_to_int(j['2025na'])
    skirmish_vp_2025_eu = convert_keys_to_int(j['2025eu'])

logging.debug("Loading Mappings (regions/borderlands/days of the week)")
# Various mappings used to add context to the .json files.
with open('lookup_tables/maps.json', 'r') as fd:
    j = json.load(fd)
    region_map = j['region_map']
    borderland_map = convert_keys_to_int(j['borderland_map'])
    weekday_map = convert_keys_to_int(j['weekday_map'])

logging.debug("Loading GW2 Events and Holidays")
# The "WvW Events" list contains the dates of special events that include
#    bonuses to WvW XP or reward track progress.
# The "Other Events" list contains the dates of PvP and PvE special events.
# The "Content Releases" list contains release dates of post-2021 expansions
#    and major content patches.
# The "Holidays" list contains the dates of in-game holidays.
# TODO: "Patches" list that contains release dates of WvW patches, including
#    profession balance and gamemode changes.
with open('lookup_tables/events.json', 'r') as fd:
    j = json.load(fd)
    wvw_events = [(x[0], dt(x[1]), dt(x[2])) for x in j['wvw_events']]
    other_events = [(x[0], dt(x[1]), dt(x[2])) for x in j['other_events']]
    holidays = [(x[0], dt(x[1]), dt(x[2])) for x in j['holidays']]
    content_releases = [(x[0], dt(x[1])) for x in j['content_releases']]


def get_match_ids():
    # Pulls a list of all available matches from the API.
    url_slug = "https://api.kills.werdes.net/api/matchlist/all"
    try:
        response = session.get(url_slug, timeout=5)
        response.raise_for_status()
        return [key.split('_')[-1] for key in response.json()]
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch matches: {e}.")
        raise  # fail out if we can't get matches, we know they exist


def get_skirmish_data(match_id):
    # Given a match ID, returns all information available
    # on that match down to skirmish level.
    url_slug = "https://api.kills.werdes.net/api/" + \
            f"match/{match_id}/flattened/unaltered"

    try:
        response = session.get(url_slug, timeout=5)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch match {match_id}: {e}.")
        return None


def build_source_data(output_dir=all_json_dir):
    # Writes all .json files for all skirmishes available in the API.
    # Expect this to take up about 3GB of space on your system.

    match_list = get_match_ids()
    for match_id in match_list:
        filename = f"match_skirmish_data_{match_id}.json"
        filepath = output_dir / filename
        if filepath.exists():
            logging.info(f"{filename} already exists; skipping...")
            continue

        logging.debug(f"Requesting match {match_id}...")
        skirmish_data = get_skirmish_data(match_id)
        time.sleep(.75)

        if skirmish_data is None:
            logging.warning(f"No data for match {match_id} or request failed.")
            continue

        # Compress the data (can be opened with 7z or bunzip on *nix)
        with open(filepath, 'w') as f_out:
            json.dump(skirmish_data, f_out)


def combine_jsons(input_dir=all_json_dir, output_file=merged_json_name):
    match_data_all = []
    filenames = sorted(input_dir.glob("*.json"))

    if merged_json_name.exists():
        logging.info(f'{merged_json_name} already exists, skipping...')
        return

    for n, filename in enumerate(filenames):
        file_path = input_dir / filename

        with open(file_path, 'r') as f_in:
            logging.info(f"Processing {n + 1}/{len(filenames)}: {filename}.")

            try:
                data = json.load(f_in)
                match_data_all.append(data)
            except json.JSONDecodeError as e:
                logging.warning(
                    f"Skipping file {filename} due to JSON error: {e}.")

    with open(output_file, 'w') as f_out:
        json.dump(match_data_all, f_out)


def combine_csv_fragments(input_dir=all_csv_dir, output_file=merged_csv_name):
    csv_dir = input_dir.glob("*.csv")
    file_list = [pd.read_csv(file) for file in csv_dir]
    df_out = pd.concat(file_list, ignore_index=True)
    output_path = merged_csv_dir / output_file
    df_out.to_csv(output_path, index=False)


def build_skirmish_data(match):
    skirmishes = []
    match_region, match_tier = match['match_arenanet_id'].split('-')
    match_start_time = datetime.fromisoformat(match['match_start'])
    # We use this for determining skirmish values.
    match_start_hour = match_start_time.hour

    for skirmish_key, skirmish_data in match['series'].items():
        team_color = skirmish_data['color']

        for timeslot in skirmish_data['series_items']:
            start_time = datetime.fromisoformat(timeslot['timeslot_start'])
            shift_hour = (start_time.hour - match_start_hour) % 24
            # Skirmish ID relative to the 12 skirmishes in a day.
            skirmish_id = (shift_hour // 2)
            # Skirmish ID relative to the 84 skirmishes in a match.
            abs_skirmish_id = ((start_time - match_start_time).total_seconds()
                               // (2 * 60 * 60))

            timeslot_block = {
                'Match ID': match['match_id'],
                'Match Region': region_map[match_region],
                'Tier': match_tier,
                'Match Start Date': match['match_start'],
                'Skirmish Start Date': timeslot['timeslot_start'],
                'Team Name': match['worlds'][team_color]['name'],
                'Team Color': team_color,
                'Map ID': borderland_map[skirmish_data['map_id']],
                'Skirmish ID (Relative)': skirmish_id + 1,
                'Skirmish ID (Absolute)': abs_skirmish_id + 1,
                'Skirmish Day of Week': weekday_map[
                    datetime.weekday(start_time)],
                'Skirmish Day (Relative)': (abs_skirmish_id // 12) + 1,
                'Skirmish Month': start_time.month,
                'Skirmish Year': start_time.year,
                'Skirmish Kills': timeslot['kills'],
                'Skirmish Deaths': timeslot['deaths'],
                'Skirmish Score': timeslot['score_gain'],
                'WvW Event Running': next((e for e, s, e2 in wvw_events
                                           if s <= start_time <= e2), 'No'),
                'PvE Event Running': next((e for e, s, e2 in other_events
                                           if s <= start_time <= e2), 'No'),
                'Holiday Running': next((e for e, s, e2 in holidays
                                         if s <= start_time <= e2), 'No'),
                'Recent Content Release': next(
                    (e for e, s in content_releases
                     if timedelta(0) <= (start_time - s)
                     <= timedelta(days=14)), 'No'),
                'Possible API Downtime': 'Yes' if timeslot[
                    'kills'] == 0 and timeslot['deaths'] == 0 else 'No'
            }

            if match['worlds'][team_color]['additional_worlds']:
                link_team = next(
                    iter(match['worlds'][team_color
                                         ]['additional_worlds'].values()))
                timeslot_block['Team Link'] = link_team['name']
            else:
                timeslot_block['Team Link'] = None
            skirmishes.append(timeslot_block)
    return skirmishes


def aggregate_timeslots(match_list):
    df = pd.DataFrame(match_list)

    # Controlling levels of grouping and aggregation.
    group_cols = ['Match ID', 'Team Name', 'Map ID',
                  'Skirmish ID (Absolute)', 'Match Region']
    sum_cols = ['Skirmish Kills', 'Skirmish Deaths', 'Skirmish Score']
    other_cols = list(set(df.columns) - set(group_cols + sum_cols))

    agg_dict = {col: 'sum' for col in sum_cols}
    agg_dict.update({col: 'first' for col in other_cols})

    return df.groupby(group_cols).agg(agg_dict).reset_index()


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", "-v", action="count", default=0)
    args = parser.parse_args()
    loglv = {0: logging.WARNING, 1: logging.INFO, 2: logging.DEBUG}
    logging.getLogger('root').setLevel(loglv[args.verbose])
    # These lines of code control the construction of the initial data files.
    # You do not need to run them if you just want to explore existing data.
    # If you need to rebuild the dataset, or add new matches to it, uncomment
    # this block of code.

    match_ids_list = get_match_ids()
    build_source_data()

    # with open(merged_json_name, 'r') as f:
    #     matches = json.load(f)
    #     batch_skirmish_list = []
    batch_skirmish_list = []

    for i, f in enumerate(all_json_dir.glob("*.json")):
        with open(f, 'r') as fd:
            match = json.load(fd)
        batch_skirmish_list.extend(build_skirmish_data(match))
        logging.debug(f"Created skirmish data for match {i} ({f})")
        if (i + 1) % 200 == 0:
            logging.info(f"Writing chunk of skirmishes... {i+1}")
            df = aggregate_timeslots(batch_skirmish_list)
            filepath = all_csv_dir / f'aggregated_match_chunk_{i+1}.csv'
            df.to_csv(filepath, index=False)
            batch_skirmish_list = []

    if batch_skirmish_list:
        df = aggregate_timeslots(batch_skirmish_list)
        filepath = all_csv_dir / 'aggregated_match_chunk_final.csv'
        df.to_csv(filepath, index=False)

    combine_csv_fragments(all_csv_dir, merged_csv_name)
