"""
A series of functions and operations designed to grab historical World vs. World data off of kills.werdes.net
and process them down to a single .csv file that can be used for analyses or data exploration.

Comments or questions about this code can be directed to sheffplaysgames@gmail.com or Sheff on Discord.
"""

import json
import os
import pandas as pd
import requests
from datetime import datetime, timedelta
from glob import glob
import time

# Setting up some global variables to control file placements.
all_json_dir = "World vs. World Per Match JSON Files"
merged_json_dir = os.getcwd()
merged_json_name = "all_combined_matches.json"
all_csv_dir = "World vs. World .csv Fragments"
merged_csv_dir = os.getcwd()
merged_csv_name = "all_combined_matches.csv"

os.makedirs(all_json_dir, exist_ok=True)
os.makedirs(all_csv_dir, exist_ok=True)


# These dictionaries contain different skirmish point values for comparisons.
# Dict keys map to the skirmish IDs that we make later.
# Skirmish 1 is the reset skirmish for all of these dictionaries.
# TODO: Add VP values into the resulting .csv file.

skirmish_vp_old = {1: (5, 4, 3),
                   2: (5, 4, 3),
                   3: (5, 4, 3),
                   4: (5, 4, 3),
                   5: (5, 4, 3),
                   6: (5, 4, 3),
                   7: (5, 4, 3),
                   8: (5, 4, 3),
                   9: (5, 4, 3),
                   10: (5, 4, 3),
                   11: (5, 4, 3),
                   12: (5, 4, 3)}

# These are the victory point values used between November 22, 2024 and March 28, 2025.
skirmish_vp_2024_na = {1: (33, 22, 11),
                       2: (21, 14, 7),
                       3: (9, 6, 3),
                       4: (9, 6, 3),
                       5: (9, 6, 3),
                       6: (13, 8, 4),
                       7: (13, 8, 4),
                       8: (13, 8, 4),
                       9: (13, 8, 4),
                       10: (13, 8, 4),
                       11: (21, 14, 7),
                       12: (33, 22, 11)}

skirmish_vp_2024_eu = {1: (42, 28, 14),
                       2: (30, 20, 10),
                       3: (15, 10, 5),
                       4: (6, 4, 2),
                       5: (6, 4, 2),
                       6: (6, 4, 2),
                       7: (15, 10, 5),
                       8: (15, 10, 5),
                       9: (15, 10, 5),
                       10: (15, 10, 5),
                       11: (15, 10, 5),
                       12: (30, 20, 10)}

# These are the current victory point values as of March 28, 2025.
skirmish_vp_2025_na = {1: (43, 32, 21),
                       2: (31, 24, 17),
                       3: (23, 18, 14),
                       4: (19, 16, 13),
                       5: (19, 16, 13),
                       6: (19, 16, 13),
                       7: (23, 18, 14),
                       8: (23, 18, 14),
                       9: (23, 18, 14),
                       10: (23, 18, 14),
                       11: (31, 24, 17),
                       12: (43, 32, 21)}

skirmish_vp_2025_eu = {1: (51, 37, 24),
                       2: (51, 37, 24),
                       3: (31, 24, 17),
                       4: (15, 14, 12),
                       5: (15, 14, 12),
                       6: (15, 14, 12),
                       7: (15, 14, 12),
                       8: (22, 18, 14),
                       9: (22, 18, 14),
                       10: (22, 18, 14),
                       11: (31, 24, 17),
                       12: (31, 24, 17)}

# Various maps used to add context to the .json files.
region_map = {'1': 'NA',
              '2': 'EU'}

borderland_map = {38: 'Eternal Battlegrounds',
                  95: 'Green Alpine BL',
                  96: 'Blue Alpine BL',
                  1099: 'Red Desert BL'}

weekday_map = {0: 'Monday',
               1: 'Tuesday',
               2: 'Wednesday',
               3: 'Thursday',
               4: 'Friday',
               5: 'Saturday',
               6: 'Sunday'}

# The "WvW Events" list contains the dates of special events that include bonuses to WvW XP or reward track progress.
# The "Other Events" list contains the dates of PvP and PvE special events.
# The "Content Releases" list contains release dates of post-2021 expansions and major content patches.
# The "Holidays" list contains the dates of in-game holidays.
# TODO: "Patches" list that contains release dates of WvW patches, including profession balance and gamemode changes.

wvw_events = [('World vs. World Weeklong Bonus', datetime(2021, 5, 14), datetime(2021, 4, 21)),
              ('World vs. World No Downed State', datetime(2021, 6, 18), datetime(2021, 6, 25)),
              ('World vs. World Weeklong Bonus', datetime(2021, 7, 23), datetime(2021, 7, 30)),
              ('World vs. World Restructuring Beta', datetime(2021, 9, 21), datetime(2021, 10, 1)),
              ('World vs. World Weeklong Bonus', datetime(2021, 10, 1), datetime(2021, 10, 5)),
              ('Halloween Bonus Bash', datetime(2021, 10, 5), datetime(2021, 10, 12)),
              ('World vs. World No Downed State', datetime(2021, 11, 5), datetime(2021, 11, 12)),
              ('Extra Life', datetime(2021, 10, 26), datetime(2021, 11, 16)),
              ('World vs. World Weeklong Bonus', datetime(2021, 12, 10), datetime(2021, 10, 17)),
              ('World vs. World Restructuring Beta', datetime(2022, 1, 14), datetime(2022, 1, 21)),
              ('World vs. World Weeklong Bonus', datetime(2022, 2, 18), datetime(2022, 2, 25)),
              ('World vs. World Restructuring Beta', datetime(2022, 8, 12), datetime(2022, 8, 20)),
              ('Guild Wars 2 10th Anniversary', datetime(2022, 8, 23), datetime(2022, 8, 30)),
              ('World vs. World Restructuring Beta', datetime(2022, 10, 15), datetime(2022, 10, 29)),
              ('Extra Life', datetime(2022, 10, 25), datetime(2022, 11, 15)),
              ('Black Friday Bonus Bash', datetime(2022, 11, 22), datetime(2022, 11, 29)),
              ('World vs. World Restructuring Beta', datetime(2022, 12, 2), datetime(2022, 12, 10)),
              ('World vs. World Restructuring Beta', datetime(2023, 6, 9), datetime(2023, 6, 23)),
              ('Bonus Event: World vs. World Rush', datetime(2023, 9, 26), datetime(2023, 10, 3)),
              ('Extra Life', datetime(2023, 10, 31), datetime(2023, 11, 7)),
              ('New Hero Jump Start', datetime(2023, 11, 21), datetime(2023, 12, 5)),
              ('Bonus Event: World vs. World Rush', datetime(2024, 1, 9), datetime(2024, 1, 16)),
              ('World vs. World Restructuring Beta', datetime(2024, 1, 16), datetime(2024, 2, 2)),
              ('Bonus Event: World vs. World Rush', datetime(2024, 3, 19), datetime(2024, 3, 26)),
              ('Guild Wars 2 Anniversary', datetime(2024, 8, 26), datetime(2024, 9, 3)),
              ('Bonus Event: World vs. World Rush', datetime(2024, 9, 24), datetime(2024, 10, 1)),
              ('New Hero Jump Start', datetime(2024, 11, 12), datetime(2024, 11, 25)),
              ('Bonus Event: World vs. World Rush', datetime(2025, 2, 18), datetime(2025, 2, 25)),
              ('World vs. World No Downed State', datetime(2025, 4, 25), datetime(2025, 5, 2))]

other_events = [('Icebrood Saga: Mobilizing Allies', datetime(2021, 1, 19), datetime(2021, 1, 26)),
                ('Icebrood Saga: Mobilizing Allies', datetime(2021, 2, 16), datetime(2021, 2, 23)),
                ('Icebrood Saga: Mobilizing Allies', datetime(2021, 3, 9), datetime(2021, 3, 16)),
                ('Icebrood Saga: Mobilizing Allies', datetime(2021, 3, 23), datetime(2021, 3, 30)),
                ('Choya Carnival!', datetime(2021, 4, 1), datetime(2021, 4, 6)),
                ('Icebrood Saga: Mobilizing Allies', datetime(2021, 4, 6), datetime(2021, 4, 13)),
                ('Icebrood Saga: Mobilizing Allies', datetime(2021, 4, 20), datetime(2021, 4, 13)),
                ('Icebrood Saga: Mobilizing Allies', datetime(2021, 4, 20), datetime(2021, 4, 27)),
                ('Living World Return: Gates of Maguuma and Entanglement', datetime(2021, 5, 25), datetime(2021, 6, 1)),
                ('PvP League Start Bonus Week', datetime(2021, 5, 25), datetime(2021, 6, 1)),
                ('Bonus Event: Fractal Rush', datetime(2021, 6, 1), datetime(2021, 6, 7)),
                ('Living World Return: The Dragons Reach', datetime(2021, 6, 1), datetime(2021, 6, 1)),
                ('Living World Return: Echoes of the Past and Tangled Depths', datetime(2021, 6, 8),
                 datetime(2021, 6, 15)),
                ('Living World Return: Seeds of Truth and Point of No Return', datetime(2021, 6, 15),
                 datetime(2021, 6, 22)),
                ('Living World Returns: Out of the Shadows', datetime(2021, 6, 29), datetime(2021, 7, 6)),
                ('PvP Tournament Week', datetime(2021, 7, 5), datetime(2021, 7, 12)),
                ('Living World Return: Rising Flames', datetime(2021, 7, 6), datetime(2021, 7, 13)),
                ('Living World Return: A Crack in the Ice', datetime(2021, 7, 13), datetime(2021, 7, 20)),
                ('Bonus Event: Wrath of the Twisted Marionette', datetime(2021, 7, 13), datetime(2021, 7, 20)),
                ('Living World Return: The Head of the Snake', datetime(2021, 7, 20), datetime(2021, 7, 27)),
                ('Living World Return: Flashpoint', datetime(2021, 7, 27), datetime(2021, 8, 3)),
                ('Bonus Event: World Boss Rush', datetime(2021, 7, 27), datetime(2021, 8, 3)),
                ('Living World Return: One Path Ends', datetime(2021, 8, 3), datetime(2021, 8, 10)),
                ('PvP League Start Bonus Week', datetime(2021, 8, 3), datetime(2021, 8, 10)),
                ('Living World Return: Daybreak', datetime(2021, 8, 24), datetime(2021, 8, 31)),
                ('Living World Return: A Bug in the System', datetime(2021, 8, 31), datetime(2021, 9, 7)),
                ('Living World Return: Long Live the Lich', datetime(2021, 9, 7), datetime(2021, 9, 14)),
                ('Living World Return: A Star to Guide Us', datetime(2021, 9, 14), datetime(2021, 9, 21)),
                ('Living World Return: All Or Nothing', datetime(2021, 9, 21), datetime(2021, 9, 28)),
                ('Living World Return: War Eternal', datetime(2021, 9, 28), datetime(2021, 10, 5)),
                ('PvP League Start Bonus Week', datetime(2021, 10, 12), datetime(2021, 10, 19)),
                ('Living World Return: Bound By Blood', datetime(2021, 10, 19), datetime(2021, 10, 26)),
                ('Living World Return: Whisper in the Dark', datetime(2021, 10, 26), datetime(2021, 11, 2)),
                ('Living World Return: Shadow in the Ice', datetime(2021, 11, 2), datetime(2021, 11, 9)),
                ('Living World Return: Steel and Fire', datetime(2021, 11, 9), datetime(2021, 11, 16)),
                ('Living World Return: No Quarter', datetime(2021, 11, 16), datetime(2021, 11, 23)),
                ('Living World Return: Jormag Rising', datetime(2021, 11, 23), datetime(2021, 11, 30)),
                ('Living World Return: Champions', datetime(2021, 11, 30), datetime(2021, 12, 7)),
                ('PvP League Start Bonus Week', datetime(2021, 12, 21), datetime(2021, 12, 28)),
                ('Dog Days', datetime(2022, 3, 31), datetime(2022, 4, 7)),
                ('PvP League Start Bonus Week', datetime(2022, 7, 26), datetime(2022, 8, 2)),
                ('Black Lion Requisition Missions', datetime(2023, 2, 28), datetime(2023, 3, 28)),
                ('Future Fashionable', datetime(2023, 3, 30), datetime(2023, 4, 6)),
                ('Bonus Event: Fractal Rush', datetime(2023, 9, 12), datetime(2023, 9, 19)),
                ('Black Lion Stolen Goods Recovery Event', datetime(2023, 9, 12), datetime(2023, 10, 3)),
                ('Dungeon Rush', datetime(2023, 12, 5), datetime(2023, 12, 12)),
                ('Bonus Event: Return to Season 1', datetime(2024, 1, 16), datetime(2024, 1, 23)),
                ('Bonus Event: Player vs. Player Rush', datetime(2024, 1, 23), datetime(2024, 1, 30)),
                ('Bonus Event: Fractal Rush', datetime(2024, 3, 12), datetime(2024, 3, 19)),
                ('Bonus Event: Return to Season 2', datetime(2024, 3, 26), datetime(2024, 4, 2)),
                ('Heights of Glory', datetime(2024, 4, 1), datetime(2024, 4, 9)),
                ('Bonus Event: Heart of Thorns', datetime(2024, 4, 9), datetime(2024, 4, 16)),
                ('Sunken Treasure Week', datetime(2024, 7, 2), datetime(2024, 7, 9)),
                ('Dungeon Rush', datetime(2024, 7, 9), datetime(2024, 7, 16)),
                ('Bonus Event: Player vs. Player Rush', datetime(2024, 7, 16), datetime(2024, 7, 23)),
                ('Bonus Event: Return to Season 3', datetime(2024, 7, 23), datetime(2024, 7, 30)),
                ('Bonus Event: Fractal Rush', datetime(2024, 9, 10), datetime(2024, 9, 17)),
                ('Bonus Event: Path of Fire', datetime(2024, 9, 17), datetime(2024, 9, 24)),
                ('Bonus Event: Return to Season 4', datetime(2024, 10, 1), datetime(2024, 10, 8)),
                ('Bonus Event: Return to the Icebrood Saga', datetime(2025, 1, 14), datetime(2025, 1, 21)),
                ('Bonus Event: Player vs. Player Rush', datetime(2025, 1, 21), datetime(2025, 1, 28)),
                ('Bonus Event: Fractal Rush', datetime(2025, 2, 25), datetime(2025, 3, 4)),
                ('Cozy Cafe', datetime(2025, 4, 1), datetime(2025, 4, 8)),
                ('Dungeon Rush', datetime(2025, 4, 8), datetime(2025, 4, 15))]

content_releases = [('End of Dragons', datetime(2022, 2, 28)),
                    ('What Lies Beneath', datetime(2022, 2, 28)),
                    ('What Lies Within', datetime(2022, 5, 23)),
                    ('Secrets of the Obscure', datetime(2023, 8, 22)),
                    ('Through the Veil', datetime(2023, 11, 7)),
                    ('The Realm of Dreams', datetime(2024, 2, 27)),
                    ('The Midnight King', datetime(2024, 5, 21)),
                    ('Janthir Wilds', datetime(2024, 8, 20)),
                    ('Godspawn', datetime(2024, 11, 19)),
                    ('Repenteance', datetime(2024, 3, 11))]

holidays = [('Lunar New Year', datetime(2021, 2, 2), datetime(2021, 2, 23)),
            ('Lunar New Year', datetime(2022, 1, 25), datetime(2022, 2, 15)),
            ('Lunar New Year', datetime(2023, 1, 10), datetime(2023, 1, 31)),
            ('Lunar New Year', datetime(2024, 1, 30), datetime(2024, 2, 20)),
            ('Lunar New Year', datetime(2025, 1, 28), datetime(2025, 2, 18)),
            ('Super Adventure Box', datetime(2021, 4, 6), datetime(2021, 4, 27)),
            ('Super Adventure Box', datetime(2022, 3, 29), datetime(2022, 4, 19)),
            ('Super Adventure Box', datetime(2023, 3, 28), datetime(2023, 4, 18)),
            ('Super Adventure Box', datetime(2024, 4, 16), datetime(2024, 5, 7)),
            ('Super Adventure Box', datetime(2025, 4, 15), datetime(2025, 5, 6)),
            ('Dragon Bash', datetime(2021, 6, 22), datetime(2021, 7, 13)),
            ('Dragon Bash', datetime(2022, 6, 7), datetime(2022, 6, 28)),
            ('Dragon Bash', datetime(2023, 6, 6), datetime(2023, 6, 27)),
            ('Dragon Bash', datetime(2024, 6, 4), datetime(2024, 6, 25)),
            ('Festival of Four Winds', datetime(2021, 8, 31), datetime(2021, 9, 21)),
            ('Festival of Four Winds', datetime(2022, 8, 2), datetime(2022, 8, 23)),
            ('Festival of Four Winds', datetime(2023, 7, 18), datetime(2023, 8, 8)),
            ('Festival of Four Winds', datetime(2024, 7, 30), datetime(2024, 8, 20)),
            ('Halloween', datetime(2021, 10, 5), datetime(2021, 11, 9)),
            ('Halloween', datetime(2022, 10, 18), datetime(2022, 11, 8)),
            ('Halloween', datetime(2023, 10, 17), datetime(2023, 11, 7)),
            ('Halloween', datetime(2024, 10, 15), datetime(2024, 11, 5)),
            ('Wintersday', datetime(2021, 12, 14), datetime(2022, 1, 4)),
            ('Wintersday', datetime(2022, 12, 13), datetime(2023, 1, 3)),
            ('Wintersday', datetime(2023, 12, 12), datetime(2024, 1, 2)),
            ('Wintersday', datetime(2024, 12, 10), datetime(2025, 1, 2))]


def get_match_ids():
    # Pulls a list of all available matches from the API.
    url_slug = f"https://api.kills.werdes.net/api/matchlist/all"
    try:
        response = requests.get(url_slug, timeout=5)
        response.raise_for_status()
        return [key.split('_')[-1] for key in response.json()]
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch: {e}.")
        return []


def get_skirmish_data(match_id):
    # Given a match ID, returns all information available on that match down to skirmish level.
    url_slug = f"https://api.kills.werdes.net/api/match/{match_id}/flattened/unaltered"

    try:
        response = requests.get(url_slug, timeout=5)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch match {match_id}: {e}.")
        return None


def build_source_data(output_dir=all_json_dir):
    # Writes all .json files for all skirmishes available in the API.
    # Expect this to take up about 3GB of space on your system.

    match_list = get_match_ids()
    for match_id in match_list:
        filename = f"match_skirmish_data_{match_id}.json"
        filepath = os.path.join(output_dir, filename)
        if os.path.exists(filepath):
            print(f"{filename} already exists; skipping...")
            continue

        print(f"Requesting match {match_id}...")
        skirmish_data = get_skirmish_data(match_id)
        time.sleep(2)

        if skirmish_data is None:
            print(f"[WARN] No data for match {match_id}.")
            continue

        filepath_out = os.path.join(output_dir, f"match_skirmish_data_{match_id}.json")
        with open(filepath_out, 'w') as f_out:
            json.dump(skirmish_data, f_out)


def combine_jsons(input_dir=all_json_dir, output_file=merged_json_name):
    match_data_all = []
    filenames = sorted([f for f in os.listdir(input_dir) if f.endswith(".json")])

    existing_filepath = os.path.join(merged_json_dir, merged_json_name)

    if os.path.exists(existing_filepath):
        print(f'Merged .json file already exists, skipping...')
        return

    for n, filename in enumerate(filenames):
        file_path = os.path.join(input_dir, filename)

        with open(file_path, 'r') as f_in:
            print(f"Processing file {n + 1} of {len(filenames)}: {filename}.")

            try:
                data = json.load(f_in)
                match_data_all.append(data)
            except json.JSONDecodeError as e:
                print(f"[WARN] Skipping file {filename} due to JSON error: {e}.")

    with open(output_file, 'w') as f_out:
        json.dump(match_data_all, f_out)


def combine_csv_fragments(input_dir=all_csv_dir, output_file=merged_csv_name):
    csv_dir = glob(os.path.join(input_dir, '*.csv'))
    file_list = [pd.read_csv(file) for file in csv_dir]
    df_out = pd.concat(file_list, ignore_index=True)
    output_path = os.path.join(merged_csv_dir, output_file)
    df_out.to_csv(output_path, index=False)


def build_skirmish_data(match):
    skirmishes = []
    match_region, match_tier = match['match_arenanet_id'].split('-')
    match_start_time = datetime.fromisoformat(match['match_start'])
    match_start_hour = match_start_time.hour  # We use this for determining skirmish values.

    for skirmish_key, skirmish_data in match['series'].items():
        team_color = skirmish_data['color']

        for timeslot in skirmish_data['series_items']:
            start_time = datetime.fromisoformat(timeslot['timeslot_start'])
            shift_hour = (start_time.hour - match_start_hour) % 24
            skirmish_id = (shift_hour // 2)  # Skirmish ID relative to the 12 skirmishes in a day.
            abs_skirmish_id = int((((start_time - match_start_time).total_seconds()) // (
                    2 * 60 * 60)))  # Skirmish ID relative to the 84 skirmishes in a match.

            timeslot_block = {'Match ID': match['match_id'],
                              'Match Region': region_map[match_region],
                              'Tier': match_tier,
                              'Match Start Date': match['match_start'],
                              'Skirmish Start Date': timeslot['timeslot_start'],
                              'Team Name': match['worlds'][team_color]['name'],
                              'Team Color': team_color,
                              'Map ID': borderland_map[skirmish_data['map_id']],
                              'Skirmish ID (Relative)': skirmish_id + 1,
                              'Skirmish ID (Absolute)': abs_skirmish_id + 1,
                              'Skirmish Day of Week': weekday_map[datetime.weekday(start_time)],
                              'Skirmish Day (Relative)': (abs_skirmish_id // 12) + 1,
                              'Skirmish Month': start_time.month,
                              'Skirmish Year': start_time.year,
                              'Skirmish Kills': timeslot['kills'],
                              'Skirmish Deaths': timeslot['deaths'],
                              'Skirmish Score': timeslot['score_gain'],
                              'WvW Event Running': next((e for e, s, e2 in wvw_events if s <= start_time <= e2), 'No'),
                              'PvE Event Running': next((e for e, s, e2 in other_events if s <= start_time <= e2), 'No'),
                              'Holiday Running': next((e for e, s, e2 in holidays if s <= start_time <= e2), 'No'),
                              'Recent Content Release': next((e for e, s in content_releases if timedelta(0) <= (start_time - s) <= timedelta(days=14)), 'No'),
                              'Possible API Downtime': 'Yes' if timeslot['kills'] == 0 and timeslot['deaths'] == 0 else 'No'}

            if match['worlds'][team_color]['additional_worlds']:
                link_team = next(iter(match['worlds'][team_color]['additional_worlds'].values()))
                timeslot_block['Team Link'] = link_team['name']
            else:
                timeslot_block['Team Link'] = None
            skirmishes.append(timeslot_block)
    return skirmishes


def aggregate_timeslots(match_list):
    df = pd.DataFrame(match_list)

    # Controlling levels of grouping and aggregation.
    group_cols = ['Match ID', 'Team Name', 'Map ID', 'Skirmish ID (Absolute)', 'Match Region']
    sum_cols = ['Skirmish Kills', 'Skirmish Deaths', 'Skirmish Score']
    other_cols = list(set(df.columns) - set(group_cols + sum_cols))

    agg_dict = {col: 'sum' for col in sum_cols}
    agg_dict.update({col: 'first' for col in other_cols})

    return df.groupby(group_cols).agg(agg_dict).reset_index()


if __name__ == '__main__':
    # These lines of code control the construction of the initial data files.
    # You do not need to run them if you just want to explore existing data.
    # If you need to rebuild the dataset, or add new matches to it, uncomment this block of code.

    match_ids_list = get_match_ids()
    build_source_data()
    combine_jsons()

    with open(merged_json_name, 'r') as f:
        matches = json.load(f)
        batch_skirmish_list = []

    for i, match in enumerate(matches):
        batch_skirmish_list.extend(build_skirmish_data(match))
        if (i + 1) % 200 == 0:
            print(f"Writing current chunk of skirmishes...")
            df = aggregate_timeslots(batch_skirmish_list)
            filepath = os.path.join(all_csv_dir, f'aggregated_match_chunk_{i}.csv')
            df.to_csv(filepath, index=False)
            batch_skirmish_list = []

    if batch_skirmish_list:
        df = aggregate_timeslots(batch_skirmish_list)
        filepath = os.path.join(all_csv_dir, f'aggregated_match_chunk_final.csv')
        df.to_csv(filepath, index=False)

    combine_csv_fragments(all_csv_dir, merged_csv_name)
