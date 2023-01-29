import pandas as pd
import requests
import bs4
from urllib.parse import urlparse, parse_qs
from typing import Dict, Any, Optional
import time


def get_headers(url_type: str) -> Optional[Dict[str, Any]]:
    """
    Gets the headers for the request.

    Args:
        url_type (str): The type of url for which the headers are required.

    Returns:
        requested_header (Dict[str,Any]): The headers for the request.if the url_type is not found, returns None.

    """
    headers: Dict[Dict[str, Any]] = {}

    headers["ntca_govt_in"] = {
        "Host": "ntca.gov.in",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; rv:102.0) Gecko/20100101 Firefox/102.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
    }

    requested_header = headers.get(url_type, None)

    return requested_header


def get_tiger_reserve_data() -> pd.DataFrame:
    """
    Gets the list of tiger reserves in India from government website.

    Returns:
    tiger_reserve_data (pd.DataFrame): A dataframe with the list of tiger reserves.
    """

    tiger_reserve_url = "https://ntca.gov.in/tiger-reserves/#tiger-reserves-2"
    headers = get_headers("ntca_govt_in")
    requests_obj = requests.get(tiger_reserve_url, headers=headers)
    soup = bs4.BeautifulSoup(requests_obj.text, "html.parser")
    table = soup.find(
        "table",
        attrs={
            "class": "sanctions-table table table-striped table-bordered table-responsive"
        },
    )
    tiger_reserve_data = pd.read_html(str(table))[0]

    # Correcting State names:
    tiger_reserve_data["State"] = tiger_reserve_data["State"].replace(
        "Madhy Pradesh", "Madhya Pradesh"
    )
    tiger_reserve_data["State"] = tiger_reserve_data["State"].replace(
        "Odisha", "Orissa"
    )

    # dropping the last row as it is total area:
    tiger_reserve_data = tiger_reserve_data[:-1]

    return tiger_reserve_data


def get_tiger_mortality_data() -> pd.DataFrame:
    """
    Gets the list of tiger mortality in India from government website. The goverment live tracks the tiger mortality.

    Returns:
    tiger_mortality_data (pd.DataFrame): A dataframe with the list of tiger mortality.
    """
    tiger_mortality_url = "https://ntca.gov.in/tiger-mortality/#mortality-details-2021"
    headers = get_headers("ntca_govt_in")
    response = requests.get(tiger_mortality_url, headers=headers)
    soup = bs4.BeautifulSoup(response.text, "html.parser")
    tables = soup.find_all("table")
    mortality_data = []
    for table in tables:
        mortality_data.append(pd.read_html(str(table))[0])
        mortality_data[-1].columns = mortality_data[-1].iloc[0]
        mortality_data[-1] = mortality_data[-1].drop(0)

    tiger_mortality_data = pd.concat(mortality_data, ignore_index=True)

    return tiger_mortality_data


def get_tiger_occurrences_data() -> pd.DataFrame:
    """
    Gets the tiger occurrence data from gbif.org
    This data is crowdsourced & is not official. It has been as reported by the people on Facebook.

    Returns:
    tiger_occurrence_data (pd.DataFrame): A dataframe with the list of tiger occurrence.
    """

    intial_url = "https://www.gbif.org/api/occurrence/search?advanced=false&dataset_key=ff8a7785-27c8-4162-907e-658e28a09ba1&dwca_extension.facetLimit=1000&facet=dataset_key&facetMultiselect=true&geometry=POLYGON((58.18284+12.21440,103.21691+12.21440,103.21691+30.22032,58.18284+30.22032,58.18284+12.21440))&has_coordinate=true&has_geospatial_issue=false&issue.facetLimit=1000&locale=en&month.facetLimit=12&occurrence_status=present&offset=40&type_status.facetLimit=1000"

    params = parse_qs(urlparse(intial_url).query)
    url = "https://www.gbif.org/api/occurrence/search"
    params["offset"] = 0
    params["limit"] = 300  # max limit is 300

    tiger_occurrence_data = []
    while True:
        print(
            f"Fetching data from {params['offset']} to {params['offset'] + params['limit']}"
        )
        response = requests.get(url, params=params)
        data = response.json()
        tiger_occurrence_data.append(pd.DataFrame(data["results"]))
        if data["endOfRecords"]:
            break
        params["offset"] += params["limit"]
        time.sleep(10)

    tiger_occurrence_data = pd.concat(tiger_occurrence_data, ignore_index=True)
    assert tiger_occurrence_data["species"].unique() == "Panthera tigris"

    return tiger_occurrence_data
