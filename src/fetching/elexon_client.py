# File: src/fetching/elexon_client.py

from typing import Any, Dict, Optional
from pathlib import Path
import pandas as pd
import requests
import src.config as config  # Assumes ELEXON_API_KEY is defined here


# ────────────────────────────────────────────────────────────────────────────────
# ENTIRE LIST OF ENDPOINTS (all categories), keyed by a friendly name.
# The value is the URI template (with placeholders) for that endpoint.
# ────────────────────────────────────────────────────────────────────────────────

ENDPOINTS: Dict[str, str] = {
    # ─── 1) BMRS Datasets ───────────────────────────────────────────────────────
    "datasets/ABUC":        "/datasets/ABUC",
    "datasets/ABUC/stream": "/datasets/ABUC/stream",
    "datasets/AGPT":        "/datasets/AGPT",
    "datasets/AGPT/stream": "/datasets/AGPT/stream",
    "datasets/AGWS":        "/datasets/AGWS",
    "datasets/AGWS/stream": "/datasets/AGWS/stream",
    "datasets/AOBE":        "/datasets/AOBE",
    "datasets/AOBE/stream": "/datasets/AOBE/stream",
    "datasets/ATL":         "/datasets/ATL",
    "datasets/ATL/stream":  "/datasets/ATL/stream",
    "datasets/B1610":       "/datasets/B1610",
    "datasets/B1610/stream":"/datasets/B1610/stream",
    "datasets/BEB":         "/datasets/BEB",
    "datasets/BEB/stream":  "/datasets/BEB/stream",
    "datasets/BOALF":       "/datasets/BOALF",
    "datasets/BOALF/stream":"/datasets/BOALF/stream",
    "datasets/BOD":         "/datasets/BOD",
    "datasets/BOD/stream":  "/datasets/BOD/stream",
    "datasets/CBS":         "/datasets/CBS",
    "datasets/CBS/stream":  "/datasets/CBS/stream",
    "datasets/CCM":         "/datasets/CCM",
    "datasets/CCM/stream":  "/datasets/CCM/stream",
    "datasets/CDN":         "/datasets/CDN",
    "datasets/CDN/stream":  "/datasets/CDN/stream",
    "datasets/DAG":         "/datasets/DAG",
    "datasets/DAG/stream":  "/datasets/DAG/stream",
    "datasets/DATL":        "/datasets/DATL",
    "datasets/DATL/stream": "/datasets/DATL/stream",
    "datasets/DCI":         "/datasets/DCI",
    "datasets/DCI/stream":  "/datasets/DCI/stream",
    "datasets/DGWS":        "/datasets/DGWS",
    "datasets/DGWS/stream": "/datasets/DGWS/stream",
    "datasets/DISBSAD":     "/datasets/DISBSAD",
    "datasets/DISBSAD/stream": "/datasets/DISBSAD/stream",
    "datasets/FEIB":        "/datasets/FEIB",
    "datasets/FEIB/stream": "/datasets/FEIB/stream",
    "datasets/FOU2T14D":    "/datasets/FOU2T14D",
    "datasets/FOU2T14D/stream": "/datasets/FOU2T14D/stream",
    "datasets/FOU2T3YW":    "/datasets/FOU2T3YW",
    "datasets/FOU2T3YW/stream": "/datasets/FOU2T3YW/stream",
    "datasets/FREQ":        "/datasets/FREQ",
    "datasets/FREQ/stream": "/datasets/FREQ/stream",
    "datasets/FUELHH":      "/datasets/FUELHH",
    "datasets/FUELHH/stream": "/datasets/FUELHH/stream",
    "datasets/FUELINST":    "/datasets/FUELINST",
    "datasets/FUELINST/stream": "/datasets/FUELINST/stream",
    "datasets/IGCA":        "/datasets/IGCA",
    "datasets/IGCA/stream": "/datasets/IGCA/stream",
    "datasets/IGCPU":       "/datasets/IGCPU",
    "datasets/IGCPU/stream": "/datasets/IGCPU/stream",
    "datasets/IMBALNGC":    "/datasets/IMBALNGC",
    "datasets/IMBALNGC/stream": "/datasets/IMBALNGC/stream",
    "datasets/INDDEM":      "/datasets/INDDEM",
    "datasets/INDDEM/stream": "/datasets/INDDEM/stream",
    "datasets/INDGEN":      "/datasets/INDGEN",
    "datasets/INDGEN/stream": "/datasets/INDGEN/stream",
    "datasets/INDO":        "/datasets/INDO",
    "datasets/INDOD":       "/datasets/INDOD",
    "datasets/INDOD/stream": "/datasets/INDOD/stream",
    "datasets/ITSDO":       "/datasets/ITSDO",
    "datasets/LOLPDRM":     "/datasets/LOLPDRM",
    "datasets/LOLPDRM/stream": "/datasets/LOLPDRM/stream",
    "datasets/MATL":        "/datasets/MATL",
    "datasets/MATL/stream": "/datasets/MATL/stream",
    "datasets/MDP":         "/datasets/MDP",
    "datasets/MDP/stream":  "/datasets/MDP/stream",
    "datasets/MDV":         "/datasets/MDV",
    "datasets/MDV/stream":  "/datasets/MDV/stream",
    "datasets/MELNGC":      "/datasets/MELNGC",
    "datasets/MELNGC/stream": "/datasets/MELNGC/stream",
    "datasets/MELS":        "/datasets/MELS",
    "datasets/MELS/stream": "/datasets/MELS/stream",
    "datasets/MID":         "/datasets/MID",
    "datasets/MID/stream":  "/datasets/MID/stream",
    "datasets/MILS":        "/datasets/MILS",
    "datasets/MILS/stream": "/datasets/MILS/stream",
    "datasets/MNZT":        "/datasets/MNZT",
    "datasets/MNZT/stream": "/datasets/MNZT/stream",
    "datasets/MZT":         "/datasets/MZT",
    "datasets/MZT/stream":  "/datasets/MZT/stream",
    "datasets/NDF":         "/datasets/NDF",
    "datasets/NDF/stream":  "/datasets/NDF/stream",
    "datasets/NDFD":        "/datasets/NDFD",
    "datasets/NDFD/stream": "/datasets/NDFD/stream",
    "datasets/NDFW":        "/datasets/NDFW",
    "datasets/NDFW/stream": "/datasets/NDFW/stream",
    "datasets/NDZ":         "/datasets/NDZ",
    "datasets/NDZ/stream":  "/datasets/NDZ/stream",
    "datasets/NETBSAD":     "/datasets/NETBSAD",
    "datasets/NETBSAD/stream": "/datasets/NETBSAD/stream",
    "datasets/NONBM":       "/datasets/NONBM",
    "datasets/NONBM/stream": "/datasets/NONBM/stream",
    "datasets/NOU2T14D":    "/datasets/NOU2T14D",
    "datasets/NOU2T14D/stream": "/datasets/NOU2T14D/stream",
    "datasets/NOU2T3YW":    "/datasets/NOU2T3YW",
    "datasets/NOU2T3YW/stream": "/datasets/NOU2T3YW/stream",
    "datasets/NTB":         "/datasets/NTB",
    "datasets/NTB/stream":  "/datasets/NTB/stream",
    "datasets/NTO":         "/datasets/NTO",
    "datasets/NTO/stream":  "/datasets/NTO/stream",
    "datasets/OCNMF3Y":     "/datasets/OCNMF3Y",
    "datasets/OCNMF3Y/stream": "/datasets/OCNMF3Y/stream",
    "datasets/OCNMF3Y2":    "/datasets/OCNMF3Y2",
    "datasets/OCNMF3Y2/stream": "/datasets/OCNMF3Y2/stream",
    "datasets/OCNMFD":      "/datasets/OCNMFD",
    "datasets/OCNMFD/stream": "/datasets/OCNMFD/stream",
    "datasets/OCNMFD2":     "/datasets/OCNMFD2",
    "datasets/OCNMFD2/stream": "/datasets/OCNMFD2/stream",
    "datasets/PBC":         "/datasets/PBC",
    "datasets/PBC/stream":  "/datasets/PBC/stream",
    "datasets/PN":          "/datasets/PN",
    "datasets/PN/stream":   "/datasets/PN/stream",
    "datasets/PPBR":        "/datasets/PPBR",
    "datasets/PPBR/stream": "/datasets/PPBR/stream",
    "datasets/QAS":         "/datasets/QAS",
    "datasets/QAS/stream":  "/datasets/QAS/stream",
    "datasets/QPN":         "/datasets/QPN",
    "datasets/QPN/stream":  "/datasets/QPN/stream",
    "datasets/RDRE":        "/datasets/RDRE",
    "datasets/RDRE/stream": "/datasets/RDRE/stream",
    "datasets/RDRI":        "/datasets/RDRI",
    "datasets/RDRI/stream": "/datasets/RDRI/stream",
    "datasets/REMIT":       "/datasets/REMIT",
    "datasets/REMIT/stream": "/datasets/REMIT/stream",
    "datasets/RURE":        "/datasets/RURE",
    "datasets/RURE/stream": "/datasets/RURE/stream",
    "datasets/RURI":        "/datasets/RURI",
    "datasets/RURI/stream": "/datasets/RURI/stream",
    "datasets/RZDF":        "/datasets/RZDF",
    "datasets/RZDR":        "/datasets/RZDR",
    "datasets/RZDR/stream": "/datasets/RZDR/stream",
    "datasets/SEL":         "/datasets/SEL",
    "datasets/SEL/stream":  "/datasets/SEL/stream",
    "datasets/SIL":         "/datasets/SIL",
    "datasets/SIL/stream":  "/datasets/SIL/stream",
    "datasets/SOSO":        "/datasets/SOSO",
    "datasets/SOSO/stream": "/datasets/SOSO/stream",
    "datasets/SYSWARN":     "/datasets/SYSWARN",
    "datasets/SYSWARN/stream": "/datasets/SYSWARN/stream",
    "datasets/TEMP":        "/datasets/TEMP",
    "datasets/TSDF":        "/datasets/TSDF",
    "datasets/TSDF/stream": "/datasets/TSDF/stream",
    "datasets/TSDFD":       "/datasets/TSDFD",
    "datasets/TSDFD/stream": "/datasets/TSDFD/stream",
    "datasets/TSDFW":       "/datasets/TSDFW",
    "datasets/TSDFW/stream": "/datasets/TSDFW/stream",
    "datasets/TUDM":        "/datasets/TUDM",
    "datasets/TUDM/stream": "/datasets/TUDM/stream",
    "datasets/UOU2T14D":    "/datasets/UOU2T14D",
    "datasets/UOU2T14D/stream": "/datasets/UOU2T14D/stream",
    "datasets/UOU2T3YW":    "/datasets/UOU2T3YW",
    "datasets/UOU2T3YW/stream": "/datasets/UOU2T3YW/stream",
    "datasets/WATL":        "/datasets/WATL",
    "datasets/WATL/stream": "/datasets/WATL/stream",
    "datasets/WINDFOR":     "/datasets/WINDFOR",
    "datasets/WINDFOR/stream": "/datasets/WINDFOR/stream",
    "datasets/YAFM":        "/datasets/YAFM",
    "datasets/YAFM/stream": "/datasets/YAFM/stream",
    "datasets/YATL":        "/datasets/YATL",
    "datasets/YATL/stream": "/datasets/YATL/stream",
    "datasets/metadata/latest": "/datasets/metadata/latest",

    # ─── 2) Balancing Mechanism Dynamic ────────────────────────────────────────
    "balancing/dynamic":             "/balancing/dynamic",
    "balancing/dynamic/all":         "/balancing/dynamic/all",
    "balancing/dynamic/rates":       "/balancing/dynamic/rates",
    "balancing/dynamic/rates/all":   "/balancing/dynamic/rates/all",

    # ─── 3) Balancing Mechanism Physical ──────────────────────────────────────
    "balancing/physical":            "/balancing/physical",
    "balancing/physical/all":        "/balancing/physical/all",

    # ─── 4) Balancing Services Adjustment - Disaggregated ─────────────────────
    "balancing/nonbm/disbsad/details":  "/balancing/nonbm/disbsad/details",
    "balancing/nonbm/disbsad/summary":  "/balancing/nonbm/disbsad/summary",

    # ─── 5) Balancing Services Adjustment - Net ────────────────────────────────
    "balancing/nonbm/netbsad":        "/balancing/nonbm/netbsad",
    "balancing/nonbm/netbsad/events": "/balancing/nonbm/netbsad/events",

    # ─── 6) Bid-Offer ──────────────────────────────────────────────────────────
    "balancing/bid-offer":      "/balancing/bid-offer",
    "balancing/bid-offer/all":  "/balancing/bid-offer/all",

    # ─── 7) Bid-Offer Acceptances ──────────────────────────────────────────────
    "balancing/acceptances":            "/balancing/acceptances",
    "balancing/acceptances/all":        "/balancing/acceptances/all",
    "balancing/acceptances/all/latest": "/balancing/acceptances/all/latest",
    "balancing/acceptances/{acceptanceNumber}": "/balancing/acceptances/{acceptanceNumber}",

    # ─── 8) Demand ──────────────────────────────────────────────────────────────
    "demand/actual/total":       "/demand/actual/total",
    "demand/outturn":            "/demand/outturn",
    "demand/outturn/daily":      "/demand/outturn/daily",
    "demand/outturn/daily/stream": "/demand/outturn/daily/stream",
    "demand/outturn/stream":     "/demand/outturn/stream",
    "demand/outturn/summary":    "/demand/outturn/summary",
    "demand/peak":               "/demand/peak",
    "demand/peak/indicative":    "/demand/peak/indicative",
    "demand/peak/indicative/operational/{triadSeason}": "/demand/peak/indicative/operational/{triadSeason}",
    "demand/peak/indicative/settlement/{triadSeason}":   "/demand/peak/indicative/settlement/{triadSeason}",
    "demand/peak/triad":         "/demand/peak/triad",

    # ─── 9) Demand Forecast ─────────────────────────────────────────────────────
    "forecast/demand/daily":                     "/forecast/demand/daily",
    "forecast/demand/daily/evolution":           "/forecast/demand/daily/evolution",
    "forecast/demand/daily/history":             "/forecast/demand/daily/history",
    "forecast/demand/day-ahead":                 "/forecast/demand/day-ahead",
    "forecast/demand/day-ahead/earliest":        "/forecast/demand/day-ahead/earliest",
    "forecast/demand/day-ahead/earliest/stream": "/forecast/demand/day-ahead/earliest/stream",
    "forecast/demand/day-ahead/evolution":       "/forecast/demand/day-ahead/evolution",
    "forecast/demand/day-ahead/history":         "/forecast/demand/day-ahead/history",
    "forecast/demand/day-ahead/latest":          "/forecast/demand/day-ahead/latest",
    "forecast/demand/day-ahead/latest/stream":   "/forecast/demand/day-ahead/latest/stream",
    "forecast/demand/day-ahead/peak":            "/forecast/demand/day-ahead/peak",
    "forecast/demand/total/day-ahead":           "/forecast/demand/total/day-ahead",
    "forecast/demand/total/week-ahead":          "/forecast/demand/total/week-ahead",
    "forecast/demand/total/week-ahead/latest":   "/forecast/demand/total/week-ahead/latest",
    "forecast/demand/weekly":                    "/forecast/demand/weekly",
    "forecast/demand/weekly/evolution":          "/forecast/demand/weekly/evolution",
    "forecast/demand/weekly/history":            "/forecast/demand/weekly/history",

    # ───10) Generation ───────────────────────────────────────────────────────────
    "generation/actual/per-type":                "/generation/actual/per-type",
    "generation/actual/per-type/day-total":      "/generation/actual/per-type/day-total",
    "generation/actual/per-type/wind-and-solar": "/generation/actual/per-type/wind-and-solar",
    "generation/outturn":                        "/generation/outturn",
    "generation/outturn/current":                "/generation/outturn/current",
    "generation/outturn/interconnectors":        "/generation/outturn/interconnectors",
    "generation/outturn/summary":                "/generation/outturn/summary",

    # ───11) Generation Forecast ─────────────────────────────────────────────────
    "forecast/availability/daily":               "/forecast/availability/daily",
    "forecast/availability/daily/evolution":     "/forecast/availability/daily/evolution",
    "forecast/availability/daily/history":       "/forecast/availability/daily/history",
    "forecast/availability/weekly":              "/forecast/availability/weekly",
    "forecast/availability/weekly/evolution":    "/forecast/availability/weekly/evolution",
    "forecast/availability/weekly/history":      "/forecast/availability/weekly/history",
    "forecast/generation/day-ahead":             "/forecast/generation/day-ahead",
    "forecast/generation/wind":                  "/forecast/generation/wind",
    "forecast/generation/wind-and-solar/day-ahead": "/forecast/generation/wind-and-solar/day-ahead",
    "forecast/generation/wind/earliest":             "/forecast/generation/wind/earliest",
    "forecast/generation/wind/earliest/stream":      "/forecast/generation/wind/earliest/stream",
    "forecast/generation/wind/evolution":             "/forecast/generation/wind/evolution",
    "forecast/generation/wind/history":               "/forecast/generation/wind/history",
    "forecast/generation/wind/latest":                "/forecast/generation/wind/latest",
    "forecast/generation/wind/latest/stream":         "/forecast/generation/wind/latest/stream",
    "forecast/generation/wind/peak":                   "/forecast/generation/wind/peak",

    # ───12) Health Check ─────────────────────────────────────────────────────────
    "health": "/health",

    # ───13) Indicated Forecast ───────────────────────────────────────────────────
    "forecast/indicated/day-ahead":           "/forecast/indicated/day-ahead",
    "forecast/indicated/day-ahead/evolution": "/forecast/indicated/day-ahead/evolution",
    "forecast/indicated/day-ahead/history":   "/forecast/indicated/day-ahead/history",

    # ───14) Indicative Imbalance Settlement ──────────────────────────────────────
    "balancing/settlement/acceptance/volumes/all/{bidOffer}/{settlementDate}": "/balancing/settlement/acceptance/volumes/all/{bidOffer}/{settlementDate}",
    "balancing/settlement/acceptance/volumes/all/{bidOffer}/{settlementDate}/{settlementPeriod}": "/balancing/settlement/acceptance/volumes/all/{bidOffer}/{settlementDate}/{settlementPeriod}",
    "balancing/settlement/acceptances/all/{settlementDate}/{settlementPeriod}":       "/balancing/settlement/acceptances/all/{settlementDate}/{settlementPeriod}",
    "balancing/settlement/default-notices":    "/balancing/settlement/default-notices",
    "balancing/settlement/indicative/cashflows/all/{bidOffer}/{settlementDate}":       "/balancing/settlement/indicative/cashflows/all/{bidOffer}/{settlementDate}",
    "balancing/settlement/indicative/cashflows/all/{bidOffer}/{settlementDate}/{settlementPeriod}": "/balancing/settlement/indicative/cashflows/all/{bidOffer}/{settlementDate}/{settlementPeriod}",
    "balancing/settlement/indicative/volumes/all/{bidOffer}/{settlementDate}":          "/balancing/settlement/indicative/volumes/all/{bidOffer}/{settlementDate}",
    "balancing/settlement/indicative/volumes/all/{bidOffer}/{settlementDate}/{settlementPeriod}": "/balancing/settlement/indicative/volumes/all/{bidOffer}/{settlementDate}/{settlementPeriod}",
    "balancing/settlement/market-depth/{settlementDate}":        "/balancing/settlement/market-depth/{settlementDate}",
    "balancing/settlement/market-depth/{settlementDate}/{settlementPeriod}":          "/balancing/settlement/market-depth/{settlementDate}/{settlementPeriod}",
    "balancing/settlement/messages/{settlementDate}":            "/balancing/settlement/messages/{settlementDate}",
    "balancing/settlement/messages/{settlementDate}/{settlementPeriod}":          "/balancing/settlement/messages/{settlementDate}/{settlementPeriod}",
    "balancing/settlement/stack/all/{bidOffer}/{settlementDate}/{settlementPeriod}": "/balancing/settlement/stack/all/{bidOffer}/{settlementDate}/{settlementPeriod}",
    "balancing/settlement/summary/{settlementDate}/{settlementPeriod}":           "/balancing/settlement/summary/{settlementDate}/{settlementPeriod}",
    "balancing/settlement/system-prices/{settlementDate}":                    "/balancing/settlement/system-prices/{settlementDate}",
    "balancing/settlement/system-prices/{settlementDate}/{settlementPeriod}":  "/balancing/settlement/system-prices/{settlementDate}/{settlementPeriod}",

    # ───15) Margin Forecast ────────────────────────────────────────────────────────
    "forecast/margin/daily":               "/forecast/margin/daily",
    "forecast/margin/daily/evolution":     "/forecast/margin/daily/evolution",
    "forecast/margin/daily/history":       "/forecast/margin/daily/history",
    "forecast/margin/weekly":              "/forecast/margin/weekly",
    "forecast/margin/weekly/evolution":    "/forecast/margin/weekly/evolution",
    "forecast/margin/weekly/history":      "/forecast/margin/weekly/history",

    # ───16) Market Index ──────────────────────────────────────────────────────────
    "balancing/pricing/market-index": "/balancing/pricing/market-index",

    # ───17) Non-BM STOR ───────────────────────────────────────────────────────────
    "balancing/nonbm/stor":         "/balancing/nonbm/stor",
    "balancing/nonbm/stor/events":  "/balancing/nonbm/stor/events",

    # ───18) Non-BM Volumes ────────────────────────────────────────────────────────
    "balancing/nonbm/volumes": "/balancing/nonbm/volumes",

    # ───19) REMIT ─────────────────────────────────────────────────────────────────
    "remit":                    "/remit",
    "remit/list/by-event":      "/remit/list/by-event",
    "remit/list/by-event/stream": "/remit/list/by-event/stream",
    "remit/list/by-publish":    "/remit/list/by-publish",
    "remit/list/by-publish/stream": "/remit/list/by-publish/stream",
    "remit/revisions":          "/remit/revisions",
    "remit/search":             "/remit/search",
    "remit/{messageId}":        "/remit/{messageId}",

    # ───20) Reference ─────────────────────────────────────────────────────────────
    "reference/bmunits/all":            "/reference/bmunits/all",
    "reference/fueltypes/all":          "/reference/fueltypes/all",
    "reference/interconnectors/all":    "/reference/interconnectors/all",
    "reference/remit/assets/all":       "/reference/remit/assets/all",
    "reference/remit/fueltypes/all":    "/reference/remit/fueltypes/all",
    "reference/remit/participants/all": "/reference/remit/participants/all",

    # ───21) SAA Datasets ──────────────────────────────────────────────────────────
    "saa/datasets/total-exempt-volume/{settlementDate}": "/saa/datasets/total-exempt-volume/{settlementDate}",

    # ───22) SO-SO Prices ──────────────────────────────────────────────────────────
    "soso/prices": "/soso/prices",

    # ───23) Surplus Forecast ──────────────────────────────────────────────────────
    "forecast/surplus/daily":               "/forecast/surplus/daily",
    "forecast/surplus/daily/evolution":     "/forecast/surplus/daily/evolution",
    "forecast/surplus/daily/history":       "/forecast/surplus/daily/history",
    "forecast/surplus/weekly":              "/forecast/surplus/weekly",
    "forecast/surplus/weekly/evolution":    "/forecast/surplus/weekly/evolution",
    "forecast/surplus/weekly/history":      "/forecast/surplus/weekly/history",

    # ───24) System ─────────────────────────────────────────────────────────────────
    "system/demand-control-instructions": "/system/demand-control-instructions",
    "system/frequency":                   "/system/frequency",
    "system/frequency/stream":            "/system/frequency/stream",
    "system/warnings":                    "/system/warnings",

    # ───25) System Forecast ────────────────────────────────────────────────────────
    "forecast/system/loss-of-load":       "/forecast/system/loss-of-load",

    # ───26) Temperature ───────────────────────────────────────────────────────────
    "temperature": "/temperature"
}


class ElexonApiClient:
    """
    A fully‐loaded client that can call any BMRS endpoint listed in ENDPOINTS.
    To invoke, use `call_endpoint(key, **kwargs)`, where `key` matches a dictionary entry
    and `kwargs` fill in the URI template’s placeholders or query parameters.
    """

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or config.ELEXON_API_KEY
        if not self.api_key:
            raise ValueError("Elexon API key must be provided (argument or in config).")
        self.base_url = "https://data.elexon.co.uk/bmrs/api/v1"

        # Optionally, set up caching directories if you want raw/pickle or parquet caching:
        self.raw_dir = Path("data/raw")
        self.proc_dir = Path("data/processed")
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.proc_dir.mkdir(parents=True, exist_ok=True)

    def _get(self, path: str, params: Dict[str, Any]) -> pd.DataFrame:
        """
        Internal helper to do a GET at self.base_url + path, with query params=params
        and header {"apiKey": self.api_key}. Returns DataFrame from JSON payload.
        - If the JSON response is a dict containing "data", extract that.
        - If the JSON response is a list, treat it directly as the data list.
        """
        url = f"{self.base_url}{path}"
        headers = {"apiKey": self.api_key}
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            payload = response.json()

            # If payload is a list, treat that as the data directly
            if isinstance(payload, list):
                data = payload

            # If payload is a dict, look for "data" key
            elif isinstance(payload, dict):
                data = payload.get("data", [])
                # Some endpoints may nest further; but we assume "data" is correct

            else:
                data = []

            # Build DataFrame
            if isinstance(data, list):
                return pd.DataFrame(data)
            elif isinstance(data, dict):
                return pd.DataFrame([data])
            else:
                return pd.DataFrame()

        except requests.RequestException as e:
            print(f"Error fetching {url} with params={params}: {e}")
            return pd.DataFrame()

    def call_endpoint(
        self,
        key: str,
        path_params: Optional[Dict[str, Any]] = None,
        query_params: Optional[Dict[str, Any]] = None,
    ) -> pd.DataFrame:
        """
        Generic caller for any key in ENDPOINTS.
        - Handles both payloads that are top-level lists and payloads that are dicts with "data".
        """
        if key not in ENDPOINTS:
            raise KeyError(f"Endpoint '{key}' not found in ENDPOINTS.")

        uri_template = ENDPOINTS[key]
        path_params = path_params or {}
        try:
            path = uri_template.format(**path_params)
        except KeyError as e:
            missing = e.args[0]
            raise ValueError(f"Missing path parameter '{missing}' for endpoint '{key}'") from e

        return self._get(path, params=query_params or {})

    # ────────────────────────────────────────────────────────────────────────────────
    # For convenience, you can still define “wrapper” methods for the common patterns:
    # ────────────────────────────────────────────────────────────────────────────────

    def get_dataset(self, dataset: str) -> pd.DataFrame:
        """
        Convenience wrapper for GET /datasets/{dataset}
        """
        return self.call_endpoint(f"datasets/{dataset}")

    def get_dataset_stream(
        self,
        dataset: str,
        from_: Optional[str] = None,
        to: Optional[str] = None,
        publishDateTimeFrom: Optional[str] = None,
        publishDateTimeTo: Optional[str] = None,
        settlementDate: Optional[str] = None,
        settlementPeriod: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Convenience wrapper for GET /datasets/{dataset}/stream
        Pass exactly those query_params that apply.
        """
        # The key in our ENDPOINTS dict is literally "datasets/{dataset}/stream"
        endpoint_key = f"datasets/{dataset}/stream"
        # Collect only non‐None values into query_params
        qp: Dict[str, Any] = {}
        if from_:
            qp["from"] = from_
        if to:
            qp["to"] = to
        if publishDateTimeFrom:
            qp["publishDateTimeFrom"] = publishDateTimeFrom
        if publishDateTimeTo:
            qp["publishDateTimeTo"] = publishDateTimeTo
        if settlementDate:
            qp["settlementDate"] = settlementDate
        if settlementPeriod is not None:
            qp["settlementPeriod"] = settlementPeriod

        return self.call_endpoint(endpoint_key, path_params={"dataset": dataset}, query_params=qp)

 # ────────────────────────────────────────────────────────────────────────────
    # 1) Explicit Wrappers for Bid-Offer Acceptances
    # ────────────────────────────────────────────────────────────────────────────

    def get_acceptance_by_number(self, acceptanceNumber: str) -> pd.DataFrame:
        """
        GET /balancing/acceptances/{acceptanceNumber}
        """
        endpoint_key = "balancing/acceptances/{acceptanceNumber}"
        return self.call_endpoint(endpoint_key, path_params={"acceptanceNumber": acceptanceNumber})

    def get_acceptances(
        self,
        bmUnit: Optional[str] = None,
        from_: Optional[str] = None,
        to: Optional[str] = None
    ) -> pd.DataFrame:
        """
        GET /balancing/acceptances?bmUnit={bmUnit}&from={from_}&to={to}
        """
        endpoint_key = "balancing/acceptances"
        qp: Dict[str, Any] = {}
        if bmUnit:
            qp["bmUnit"] = bmUnit
        if from_:
            qp["from"] = from_
        if to:
            qp["to"] = to
        return self.call_endpoint(endpoint_key, query_params=qp)

    def get_acceptances_all_latest(self) -> pd.DataFrame:
        """
        GET /balancing/acceptances/all/latest
        """
        endpoint_key = "balancing/acceptances/all/latest"
        return self.call_endpoint(endpoint_key)

    def get_acceptances_all(
        self,
        settlementDate: str,
        settlementPeriod: Optional[int] = None
    ) -> pd.DataFrame:
        """
        GET /balancing/acceptances/all?settlementDate={settlementDate}&settlementPeriod={settlementPeriod}
        """
        endpoint_key = "balancing/acceptances/all"
        qp = {"settlementDate": settlementDate}
        if settlementPeriod is not None:
            qp["settlementPeriod"] = str(settlementPeriod)
        return self.call_endpoint(endpoint_key, query_params=qp)

    # ────────────────────────────────────────────────────────────────────────────
    # 2) Explicit Wrappers for Bid-Offer
    # ────────────────────────────────────────────────────────────────────────────

    def get_bid_offer(
        self,
        bmUnit: str,
        from_: Optional[str] = None,
        to: Optional[str] = None
    ) -> pd.DataFrame:
        """
        GET /balancing/bid-offer?bmUnit={bmUnit}&from={from_}&to={to}
        """
        endpoint_key = "balancing/bid-offer"
        qp = {"bmUnit": bmUnit}
        if from_:
            qp["from"] = from_
        if to:
            qp["to"] = to
        return self.call_endpoint(endpoint_key, query_params=qp)

    def get_bid_offer_all(
        self,
        settlementDate: str,
        settlementPeriod: Optional[int] = None
    ) -> pd.DataFrame:
        """
        GET /balancing/bid-offer/all?settlementDate={settlementDate}&settlementPeriod={settlementPeriod}
        """
        endpoint_key = "balancing/bid-offer/all"
        qp = {"settlementDate": settlementDate}
        if settlementPeriod is not None:
            qp["settlementPeriod"] = str(settlementPeriod)
        return self.call_endpoint(endpoint_key, query_params=qp)

    # ────────────────────────────────────────────────────────────────────────────
    # 3) Explicit Wrappers for Balancing Mechanism Dynamic
    # ────────────────────────────────────────────────────────────────────────────

    def get_balancing_dynamic(
        self,
        bmUnit: str,
        snapshotAt: str
    ) -> pd.DataFrame:
        """
        GET /balancing/dynamic?bmUnit={bmUnit}&snapshotAt={snapshotAt}
        """
        endpoint_key = "balancing/dynamic"
        qp = {"bmUnit": bmUnit, "snapshotAt": snapshotAt}
        return self.call_endpoint(endpoint_key, query_params=qp)

    def get_balancing_dynamic_all(
        self,
        settlementDate: str,
        settlementPeriod: int
    ) -> pd.DataFrame:
        """
        GET /balancing/dynamic/all?settlementDate={settlementDate}&settlementPeriod={settlementPeriod}
        """
        endpoint_key = "balancing/dynamic/all"
        qp = {"settlementDate": settlementDate, "settlementPeriod": str(settlementPeriod)}
        return self.call_endpoint(endpoint_key, query_params=qp)

    def get_balancing_dynamic_rates_all(
        self,
        settlementDate: str,
        settlementPeriod: int
    ) -> pd.DataFrame:
        """
        GET /balancing/dynamic/rates/all?settlementDate={settlementDate}&settlementPeriod={settlementPeriod}
        """
        endpoint_key = "balancing/dynamic/rates/all"
        qp = {"settlementDate": settlementDate, "settlementPeriod": str(settlementPeriod)}
        return self.call_endpoint(endpoint_key, query_params=qp)

    def get_balancing_dynamic_rates(
        self,
        bmUnit: str,
        snapshotAt: str
    ) -> pd.DataFrame:
        """
        GET /balancing/dynamic/rates?bmUnit={bmUnit}&snapshotAt={snapshotAt}
        """
        endpoint_key = "balancing/dynamic/rates"
        qp = {"bmUnit": bmUnit, "snapshotAt": snapshotAt}
        return self.call_endpoint(endpoint_key, query_params=qp)

    # ────────────────────────────────────────────────────────────────────────────
    # 4) Explicit Wrappers for Balancing Mechanism Physical
    # ────────────────────────────────────────────────────────────────────────────

    def get_balancing_physical_all(
        self,
        dataset: str,
        settlementDate: str,
        settlementPeriod: int
    ) -> pd.DataFrame:
        """
        GET /balancing/physical/all?dataset={dataset}&settlementDate={settlementDate}&settlementPeriod={settlementPeriod}
        """
        endpoint_key = "balancing/physical/all"
        qp = {"dataset": dataset, "settlementDate": settlementDate, "settlementPeriod": str(settlementPeriod)}
        return self.call_endpoint(endpoint_key, query_params=qp)

    def get_balancing_physical(
        self,
        bmUnit: str,
        from_: Optional[str] = None,
        to: Optional[str] = None
    ) -> pd.DataFrame:
        """
        GET /balancing/physical?bmUnit={bmUnit}&from={from_}&to={to}
        """
        endpoint_key = "balancing/physical"
        qp: Dict[str, Any] = {"bmUnit": bmUnit}
        if from_:
            qp["from"] = from_
        if to:
            qp["to"] = to
        return self.call_endpoint(endpoint_key, query_params=qp)

    # ────────────────────────────────────────────────────────────────────────────
    # 5) Explicit Wrappers for Balancing Services Adjustment - Disaggregated
    # ────────────────────────────────────────────────────────────────────────────

    def get_disbsad_details(
        self,
        settlementDate: str,
        settlementPeriod: int
    ) -> pd.DataFrame:
        """
        GET /balancing/nonbm/disbsad/details?settlementDate={settlementDate}&settlementPeriod={settlementPeriod}
        """
        endpoint_key = "balancing/nonbm/disbsad/details"
        qp = {"settlementDate": settlementDate, "settlementPeriod": str(settlementPeriod)}
        return self.call_endpoint(endpoint_key, query_params=qp)

    def get_disbsad_summary(
        self,
        from_: str,
        to: str
    ) -> pd.DataFrame:
        """
        GET /balancing/nonbm/disbsad/summary?from={from_}&to={to}
        """
        endpoint_key = "balancing/nonbm/disbsad/summary"
        qp = {"from": from_, "to": to}
        return self.call_endpoint(endpoint_key, query_params=qp)

    # ────────────────────────────────────────────────────────────────────────────
    # 6) Explicit Wrappers for Balancing Services Adjustment - Net
    # ────────────────────────────────────────────────────────────────────────────

    def get_netbsad_events(self, count: int) -> pd.DataFrame:
        """
        GET /balancing/nonbm/netbsad/events?count={count}
        """
        endpoint_key = "balancing/nonbm/netbsad/events"
        qp = {"count": str(count)}
        return self.call_endpoint(endpoint_key, query_params=qp)

    def get_netbsad(self, from_: str, to: str) -> pd.DataFrame:
        """
        GET /balancing/nonbm/netbsad?from={from_}&to={to}
        """
        endpoint_key = "balancing/nonbm/netbsad"
        qp = {"from": from_, "to": to}
        return self.call_endpoint(endpoint_key, query_params=qp)

    # ────────────────────────────────────────────────────────────────────────────
    # 7) Explicit Wrappers for Demand
    # ────────────────────────────────────────────────────────────────────────────

    def get_demand_actual_total(self) -> pd.DataFrame:
        """
        GET /demand/actual/total
        """
        endpoint_key = "demand/actual/total"
        return self.call_endpoint(endpoint_key)

    def get_demand_outturn(self) -> pd.DataFrame:
        """
        GET /demand/outturn
        """
        endpoint_key = "demand/outturn"
        return self.call_endpoint(endpoint_key)

    def get_demand_outturn_daily(self) -> pd.DataFrame:
        """
        GET /demand/outturn/daily
        """
        endpoint_key = "demand/outturn/daily"
        return self.call_endpoint(endpoint_key)

    def get_demand_outturn_daily_stream(self) -> pd.DataFrame:
        """
        GET /demand/outturn/daily/stream
        """
        endpoint_key = "demand/outturn/daily/stream"
        return self.call_endpoint(endpoint_key)

    def get_demand_outturn_stream(self) -> pd.DataFrame:
        """
        GET /demand/outturn/stream
        """
        endpoint_key = "demand/outturn/stream"
        return self.call_endpoint(endpoint_key)

    def get_demand_outturn_summary(self) -> pd.DataFrame:
        """
        GET /demand/outturn/summary
        """
        endpoint_key = "demand/outturn/summary"
        return self.call_endpoint(endpoint_key)

    def get_demand_peak(self) -> pd.DataFrame:
        """
        GET /demand/peak
        """
        endpoint_key = "demand/peak"
        return self.call_endpoint(endpoint_key)

    def get_demand_peak_indicative(self) -> pd.DataFrame:
        """
        GET /demand/peak/indicative
        """
        endpoint_key = "demand/peak/indicative"
        return self.call_endpoint(endpoint_key)

    def get_demand_peak_indicative_operational(self, triadSeason: str) -> pd.DataFrame:
        """
        GET /demand/peak/indicative/operational/{triadSeason}
        """
        endpoint_key = "demand/peak/indicative/operational/{triadSeason}"
        return self.call_endpoint(endpoint_key, path_params={"triadSeason": triadSeason})

    def get_demand_peak_indicative_settlement(self, triadSeason: str) -> pd.DataFrame:
        """
        GET /demand/peak/indicative/settlement/{triadSeason}
        """
        endpoint_key = "demand/peak/indicative/settlement/{triadSeason}"
        return self.call_endpoint(endpoint_key, path_params={"triadSeason": triadSeason})

    def get_demand_peak_triad(self) -> pd.DataFrame:
        """
        GET /demand/peak/triad
        """
        endpoint_key = "demand/peak/triad"
        return self.call_endpoint(endpoint_key)

    # ────────────────────────────────────────────────────────────────────────────
    # 8) Explicit Wrappers for Demand Forecast
    # ────────────────────────────────────────────────────────────────────────────

    def get_forecast_demand_daily(self) -> pd.DataFrame:
        """
        GET /forecast/demand/daily
        """
        endpoint_key = "forecast/demand/daily"
        return self.call_endpoint(endpoint_key)

    def get_forecast_demand_daily_evolution(self) -> pd.DataFrame:
        """
        GET /forecast/demand/daily/evolution
        """
        endpoint_key = "forecast/demand/daily/evolution"
        return self.call_endpoint(endpoint_key)

    def get_forecast_demand_daily_history(self) -> pd.DataFrame:
        """
        GET /forecast/demand/daily/history
        """
        endpoint_key = "forecast/demand/daily/history"
        return self.call_endpoint(endpoint_key)

    def get_forecast_demand_day_ahead(self) -> pd.DataFrame:
        """
        GET /forecast/demand/day-ahead
        """
        endpoint_key = "forecast/demand/day-ahead"
        return self.call_endpoint(endpoint_key)

    def get_forecast_demand_day_ahead_earliest(self) -> pd.DataFrame:
        """
        GET /forecast/demand/day-ahead/earliest
        """
        endpoint_key = "forecast/demand/day-ahead/earliest"
        return self.call_endpoint(endpoint_key)

    def get_forecast_demand_day_ahead_earliest_stream(self) -> pd.DataFrame:
        """
        GET /forecast/demand/day-ahead/earliest/stream
        """
        endpoint_key = "forecast/demand/day-ahead/earliest/stream"
        return self.call_endpoint(endpoint_key)

    def get_forecast_demand_day_ahead_evolution(self) -> pd.DataFrame:
        """
        GET /forecast/demand/day-ahead/evolution
        """
        endpoint_key = "forecast/demand/day-ahead/evolution"
        return self.call_endpoint(endpoint_key)

    def get_forecast_demand_day_ahead_history(self) -> pd.DataFrame:
        """
        GET /forecast/demand/day-ahead/history
        """
        endpoint_key = "forecast/demand/day-ahead/history"
        return self.call_endpoint(endpoint_key)

    def get_forecast_demand_day_ahead_latest(self) -> pd.DataFrame:
        """
        GET /forecast/demand/day-ahead/latest
        """
        endpoint_key = "forecast/demand/day-ahead/latest"
        return self.call_endpoint(endpoint_key)

    def get_forecast_demand_day_ahead_latest_stream(self) -> pd.DataFrame:
        """
        GET /forecast/demand/day-ahead/latest/stream
        """
        endpoint_key = "forecast/demand/day-ahead/latest/stream"
        return self.call_endpoint(endpoint_key)

    def get_forecast_demand_day_ahead_peak(self) -> pd.DataFrame:
        """
        GET /forecast/demand/day-ahead/peak
        """
        endpoint_key = "forecast/demand/day-ahead/peak"
        return self.call_endpoint(endpoint_key)

    def get_forecast_demand_total_day_ahead(self) -> pd.DataFrame:
        """
        GET /forecast/demand/total/day-ahead
        """
        endpoint_key = "forecast/demand/total/day-ahead"
        return self.call_endpoint(endpoint_key)

    def get_forecast_demand_total_week_ahead(self) -> pd.DataFrame:
        """
        GET /forecast/demand/total/week-ahead
        """
        endpoint_key = "forecast/demand/total/week-ahead"
        return self.call_endpoint(endpoint_key)

    def get_forecast_demand_total_week_ahead_latest(self) -> pd.DataFrame:
        """
        GET /forecast/demand/total/week-ahead/latest
        """
        endpoint_key = "forecast/demand/total/week-ahead/latest"
        return self.call_endpoint(endpoint_key)

    def get_forecast_demand_weekly(self) -> pd.DataFrame:
        """
        GET /forecast/demand/weekly
        """
        endpoint_key = "forecast/demand/weekly"
        return self.call_endpoint(endpoint_key)

    def get_forecast_demand_weekly_evolution(self) -> pd.DataFrame:
        """
        GET /forecast/demand/weekly/evolution
        """
        endpoint_key = "forecast/demand/weekly/evolution"
        return self.call_endpoint(endpoint_key)

    def get_forecast_demand_weekly_history(self) -> pd.DataFrame:
        """
        GET /forecast/demand/weekly/history
        """
        endpoint_key = "forecast/demand/weekly/history"
        return self.call_endpoint(endpoint_key)

    # ────────────────────────────────────────────────────────────────────────────
    # 9) Explicit Wrappers for Generation
    # ────────────────────────────────────────────────────────────────────────────

    def get_generation_actual_per_type(self) -> pd.DataFrame:
        """
        GET /generation/actual/per-type
        """
        endpoint_key = "generation/actual/per-type"
        return self.call_endpoint(endpoint_key)

    def get_generation_actual_per_type_day_total(self) -> pd.DataFrame:
        """
        GET /generation/actual/per-type/day-total
        """
        endpoint_key = "generation/actual/per-type/day-total"
        return self.call_endpoint(endpoint_key)

    def get_generation_actual_per_type_wind_and_solar(self) -> pd.DataFrame:
        """
        GET /generation/actual/per-type/wind-and-solar
        """
        endpoint_key = "generation/actual/per-type/wind-and-solar"
        return self.call_endpoint(endpoint_key)

    def get_generation_outturn(self) -> pd.DataFrame:
        """
        GET /generation/outturn
        """
        endpoint_key = "generation/outturn"
        return self.call_endpoint(endpoint_key)

    def get_generation_outturn_current(self) -> pd.DataFrame:
        """
        GET /generation/outturn/current
        """
        endpoint_key = "generation/outturn/current"
        return self.call_endpoint(endpoint_key)

    def get_generation_outturn_interconnectors(self) -> pd.DataFrame:
        """
        GET /generation/outturn/interconnectors
        """
        endpoint_key = "generation/outturn/interconnectors"
        return self.call_endpoint(endpoint_key)

    def get_generation_outturn_summary(self) -> pd.DataFrame:
        """
        GET /generation/outturn/summary
        """
        endpoint_key = "generation/outturn/summary"
        return self.call_endpoint(endpoint_key)

    # ────────────────────────────────────────────────────────────────────────────
    # 10) Explicit Wrappers for Generation Forecast
    # ────────────────────────────────────────────────────────────────────────────

    def get_forecast_availability_daily(self) -> pd.DataFrame:
        """
        GET /forecast/availability/daily
        """
        endpoint_key = "forecast/availability/daily"
        return self.call_endpoint(endpoint_key)

    def get_forecast_availability_daily_evolution(self) -> pd.DataFrame:
        """
        GET /forecast/availability/daily/evolution
        """
        endpoint_key = "forecast/availability/daily/evolution"
        return self.call_endpoint(endpoint_key)

    def get_forecast_availability_daily_history(self) -> pd.DataFrame:
        """
        GET /forecast/availability/daily/history
        """
        endpoint_key = "forecast/availability/daily/history"
        return self.call_endpoint(endpoint_key)

    def get_forecast_availability_weekly(self) -> pd.DataFrame:
        """
        GET /forecast/availability/weekly
        """
        endpoint_key = "forecast/availability/weekly"
        return self.call_endpoint(endpoint_key)

    def get_forecast_availability_weekly_evolution(self) -> pd.DataFrame:
        """
        GET /forecast/availability/weekly/evolution
        """
        endpoint_key = "forecast/availability/weekly/evolution"
        return self.call_endpoint(endpoint_key)

    def get_forecast_availability_weekly_history(self) -> pd.DataFrame:
        """
        GET /forecast/availability/weekly/history
        """
        endpoint_key = "forecast/availability/weekly/history"
        return self.call_endpoint(endpoint_key)

    def get_forecast_generation_day_ahead(self) -> pd.DataFrame:
        """
        GET /forecast/generation/day-ahead
        """
        endpoint_key = "forecast/generation/day-ahead"
        return self.call_endpoint(endpoint_key)

    def get_forecast_generation_wind(self) -> pd.DataFrame:
        """
        GET /forecast/generation/wind
        """
        endpoint_key = "forecast/generation/wind"
        return self.call_endpoint(endpoint_key)

    def get_forecast_generation_wind_and_solar_day_ahead(self) -> pd.DataFrame:
        """
        GET /forecast/generation/wind-and-solar/day-ahead
        """
        endpoint_key = "forecast/generation/wind-and-solar/day-ahead"
        return self.call_endpoint(endpoint_key)

    def get_forecast_generation_wind_earliest(self) -> pd.DataFrame:
        """
        GET /forecast/generation/wind/earliest
        """
        endpoint_key = "forecast/generation/wind/earliest"
        return self.call_endpoint(endpoint_key)

    def get_forecast_generation_wind_earliest_stream(self) -> pd.DataFrame:
        """
        GET /forecast/generation/wind/earliest/stream
        """
        endpoint_key = "forecast/generation/wind/earliest/stream"
        return self.call_endpoint(endpoint_key)

    def get_forecast_generation_wind_evolution(self) -> pd.DataFrame:
        """
        GET /forecast/generation/wind/evolution
        """
        endpoint_key = "forecast/generation/wind/evolution"
        return self.call_endpoint(endpoint_key)

    def get_forecast_generation_wind_history(self) -> pd.DataFrame:
        """
        GET /forecast/generation/wind/history
        """
        endpoint_key = "forecast/generation/wind/history"
        return self.call_endpoint(endpoint_key)

    def get_forecast_generation_wind_latest(self) -> pd.DataFrame:
        """
        GET /forecast/generation/wind/latest
        """
        endpoint_key = "forecast/generation/wind/latest"
        return self.call_endpoint(endpoint_key)

    def get_forecast_generation_wind_latest_stream(self) -> pd.DataFrame:
        """
        GET /forecast/generation/wind/latest/stream
        """
        endpoint_key = "forecast/generation/wind/latest/stream"
        return self.call_endpoint(endpoint_key)

    def get_forecast_generation_wind_peak(self) -> pd.DataFrame:
        """
        GET /forecast/generation/wind/peak
        """
        endpoint_key = "forecast/generation/wind/peak"
        return self.call_endpoint(endpoint_key)

    # ────────────────────────────────────────────────────────────────────────────
    # 11) Explicit Wrappers for Health Check
    # ────────────────────────────────────────────────────────────────────────────

    def get_health(self) -> pd.DataFrame:
        """
        GET /health
        """
        endpoint_key = "health"
        return self.call_endpoint(endpoint_key)

    # ────────────────────────────────────────────────────────────────────────────
    # 12) Explicit Wrappers for Indicated Forecast
    # ────────────────────────────────────────────────────────────────────────────

    def get_forecast_indicated_day_ahead(self) -> pd.DataFrame:
        """
        GET /forecast/indicated/day-ahead
        """
        endpoint_key = "forecast/indicated/day-ahead"
        return self.call_endpoint(endpoint_key)

    def get_forecast_indicated_day_ahead_evolution(self) -> pd.DataFrame:
        """
        GET /forecast/indicated/day-ahead/evolution
        """
        endpoint_key = "forecast/indicated/day-ahead/evolution"
        return self.call_endpoint(endpoint_key)

    def get_forecast_indicated_day_ahead_history(self) -> pd.DataFrame:
        """
        GET /forecast/indicated/day-ahead/history
        """
        endpoint_key = "forecast/indicated/day-ahead/history"
        return self.call_endpoint(endpoint_key)

    # ────────────────────────────────────────────────────────────────────────────
    # 13) Explicit Wrappers for Indicative Imbalance Settlement
    # ────────────────────────────────────────────────────────────────────────────

    def get_settlement_acceptance_volumes(
        self,
        bidOffer: str,
        settlementDate: str
    ) -> pd.DataFrame:
        endpoint_key = "balancing/settlement/acceptance/volumes/all/{bidOffer}/{settlementDate}"
        return self.call_endpoint(endpoint_key, path_params={"bidOffer": bidOffer, "settlementDate": settlementDate})

    def get_settlement_acceptance_volumes_sp(
        self,
        bidOffer: str,
        settlementDate: str,
        settlementPeriod: int
    ) -> pd.DataFrame:
        endpoint_key = "balancing/settlement/acceptance/volumes/all/{bidOffer}/{settlementDate}/{settlementPeriod}"
        return self.call_endpoint(endpoint_key, path_params={"bidOffer": bidOffer, "settlementDate": settlementDate, "settlementPeriod": str(settlementPeriod)})

    def get_settlement_acceptances_all(
        self,
        settlementDate: str,
        settlementPeriod: int
    ) -> pd.DataFrame:
        endpoint_key = "balancing/settlement/acceptances/all/{settlementDate}/{settlementPeriod}"
        return self.call_endpoint(endpoint_key, path_params={"settlementDate": settlementDate, "settlementPeriod": str(settlementPeriod)})

    def get_settlement_default_notices(self) -> pd.DataFrame:
        endpoint_key = "balancing/settlement/default-notices"
        return self.call_endpoint(endpoint_key)

    def get_settlement_indicative_cashflows(
        self,
        bidOffer: str,
        settlementDate: str
    ) -> pd.DataFrame:
        endpoint_key = "balancing/settlement/indicative/cashflows/all/{bidOffer}/{settlementDate}"
        return self.call_endpoint(endpoint_key, path_params={"bidOffer": bidOffer, "settlementDate": settlementDate})

    def get_settlement_indicative_cashflows_sp(
        self,
        bidOffer: str,
        settlementDate: str,
        settlementPeriod: int
    ) -> pd.DataFrame:
        endpoint_key = "balancing/settlement/indicative/cashflows/all/{bidOffer}/{settlementDate}/{settlementPeriod}"
        return self.call_endpoint(endpoint_key, path_params={"bidOffer": bidOffer, "settlementDate": settlementDate, "settlementPeriod": str(settlementPeriod)})

    def get_settlement_indicative_volumes(
        self,
        bidOffer: str,
        settlementDate: str
    ) -> pd.DataFrame:
        endpoint_key = "balancing/settlement/indicative/volumes/all/{bidOffer}/{settlementDate}"
        return self.call_endpoint(endpoint_key, path_params={"bidOffer": bidOffer, "settlementDate": settlementDate})

    def get_settlement_indicative_volumes_sp(
        self,
        bidOffer: str,
        settlementDate: str,
        settlementPeriod: int
    ) -> pd.DataFrame:
        endpoint_key = "balancing/settlement/indicative/volumes/all/{bidOffer}/{settlementDate}/{settlementPeriod}"
        return self.call_endpoint(endpoint_key, path_params={"bidOffer": bidOffer, "settlementDate": settlementDate, "settlementPeriod": str(settlementPeriod)})

    def get_settlement_market_depth(
        self,
        settlementDate: str
    ) -> pd.DataFrame:
        endpoint_key = "balancing/settlement/market-depth/{settlementDate}"
        return self.call_endpoint(endpoint_key, path_params={"settlementDate": settlementDate})

    def get_settlement_market_depth_sp(
        self,
        settlementDate: str,
        settlementPeriod: int
    ) -> pd.DataFrame:
        endpoint_key = "balancing/settlement/market-depth/{settlementDate}/{settlementPeriod}"
        return self.call_endpoint(endpoint_key, path_params={"settlementDate": settlementDate, "settlementPeriod": str(settlementPeriod)})

    def get_settlement_messages(
        self,
        settlementDate: str
    ) -> pd.DataFrame:
        endpoint_key = "balancing/settlement/messages/{settlementDate}"
        return self.call_endpoint(endpoint_key, path_params={"settlementDate": settlementDate})

    def get_settlement_messages_sp(
        self,
        settlementDate: str,
        settlementPeriod: int
    ) -> pd.DataFrame:
        endpoint_key = "balancing/settlement/messages/{settlementDate}/{settlementPeriod}"
        return self.call_endpoint(endpoint_key, path_params={"settlementDate": settlementDate, "settlementPeriod": str(settlementPeriod)})

    def get_settlement_stack_all(
        self,
        bidOffer: str,
        settlementDate: str,
        settlementPeriod: int
    ) -> pd.DataFrame:
        endpoint_key = "balancing/settlement/stack/all/{bidOffer}/{settlementDate}/{settlementPeriod}"
        return self.call_endpoint(endpoint_key, path_params={"bidOffer": bidOffer, "settlementDate": settlementDate, "settlementPeriod": str(settlementPeriod)})

    def get_settlement_summary(
        self,
        settlementDate: str,
        settlementPeriod: int
    ) -> pd.DataFrame:
        endpoint_key = "balancing/settlement/summary/{settlementDate}/{settlementPeriod}"
        return self.call_endpoint(endpoint_key, path_params={"settlementDate": settlementDate, "settlementPeriod": str(settlementPeriod)})

    def get_settlement_system_prices(
        self,
        settlementDate: str
    ) -> pd.DataFrame:
        endpoint_key = "balancing/settlement/system-prices/{settlementDate}"
        return self.call_endpoint(endpoint_key, path_params={"settlementDate": settlementDate})

    def get_settlement_system_prices_sp(
        self,
        settlementDate: str,
        settlementPeriod: int
    ) -> pd.DataFrame:
        endpoint_key = "balancing/settlement/system-prices/{settlementDate}/{settlementPeriod}"
        return self.call_endpoint(endpoint_key, path_params={"settlementDate": settlementDate, "settlementPeriod": str(settlementPeriod)})