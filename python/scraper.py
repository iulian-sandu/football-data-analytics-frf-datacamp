import requests # type: ignore
import json
import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# get statistics for dinamo, liga-1, romania, season 2023
team_id = 635
league_id = 283
season = 2023

url = f"https://v3.football.api-sports.io/teams/statistics?league={league_id}&season={season}&team={team_id}"

headers = {
  'x-rapidapi-key': 'bf42f2ca5491d4685d8de69c2b851cac',
  'x-rapidapi-host': 'v3.football.api-sports.io'
}

response = requests.request("GET", url, headers=headers)
unique_id = f"dinamo_statistics_2023_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"

with open(f'{unique_id}.jsonl', 'w') as f:
    f.write(json.dumps(response.json()['response']))

logger.info(f"Statistics saved to {unique_id}.jsonl. Can now be loaded into BigQuery.")