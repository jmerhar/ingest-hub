"""A stand-in for Google's token endpoint and the Google Health API, with synthetic data points
in the API's shapes."""

from __future__ import annotations

import json
import re
import urllib.parse
from dataclasses import dataclass, field
from typing import Any

from jigkit.testing.http import Received

CLIENT_ID = "synthetic-client.apps.example"
CLIENT_SECRET = "synthetic-secret"
REFRESH = "synthetic-refresh"
CODE = "synthetic-code"
SCOPES = (
    "https://www.googleapis.com/auth/googlehealth.sleep.readonly "
    "https://www.googleapis.com/auth/googlehealth.health_metrics_and_measurements.readonly "
    "https://www.googleapis.com/auth/googlehealth.activity_and_fitness.readonly"
)
RANGE = re.compile(r'([a-z0-9_.]+) >= "([^"]+)" AND [a-z0-9_.]+ < "([^"]+)"')


def watch(**extra: Any) -> dict:
    return {"device": {"displayName": "Pixel Watch 2"}, "platform": "FITBIT", **extra}


def date(text: str) -> dict:
    year, month, day = (int(p) for p in text.split("-"))
    return {"year": year, "month": month, "day": day}


def sample(time: str, offset: str = "3600s") -> dict:
    return {"physicalTime": time, "utcOffset": offset}


def interval(start: str, end: str, offset: str = "3600s") -> dict:
    return {"startTime": start, "startUtcOffset": offset, "endTime": end, "endUtcOffset": offset}


def stats(rate: float, deviation: float, snr: float) -> dict:
    return {"breathsPerMinute": rate, "standardDeviation": deviation, "signalToNoise": snr}


def sleep(
    sleep_id: str,
    start: str,
    end: str,
    minutes: int,
    *,
    kind: str = "STAGES",
    processed: bool = True,
    stages: list[dict] | None = None,
) -> dict:
    return {
        "name": f"users/1/dataTypes/sleep/dataPoints/{sleep_id}",
        "dataSource": watch(recordingMethod="DERIVED"),
        "sleep": {
            "interval": interval(start, end),
            "type": kind,
            "stages": stages or [],
            "metadata": {"processed": processed},
            "summary": {"minutesInSleepPeriod": str(minutes), "minutesAsleep": str(minutes - 30)},
        },
    }


def stage(kind: str, start: str, end: str) -> dict:
    return {"startTime": start, "startUtcOffset": "3600s", "endTime": end, "type": kind}


#: A night of 10 Nov 2026 (+01:00), the days around it, and a few samples.
DATA: dict[str, list[dict]] = {
    "daily-heart-rate-variability": [
        {
            "dataSource": watch(),
            "dailyHeartRateVariability": {
                "date": date("2026-11-11"),
                "averageHeartRateVariabilityMilliseconds": 35.5,
                "nonRemHeartRateBeatsPerMinute": "58",
                "entropy": 2.2,
                "deepSleepRootMeanSquareOfSuccessiveDifferencesMilliseconds": 0,
            },
        }
    ],
    "daily-resting-heart-rate": [
        {
            "dataSource": watch(),
            "dailyRestingHeartRate": {"date": date("2026-11-11"), "beatsPerMinute": "58"},
        }
    ],
    "daily-vo2-max": [
        {
            "dataSource": watch(),
            "dailyVo2Max": {
                "date": date("2026-11-11"),
                "vo2Max": 48.5,
                "estimated": True,
                "cardioFitnessLevel": "GOOD",
            },
        }
    ],
    "oxygen-saturation": [
        {
            "dataSource": watch(),
            "oxygenSaturation": {"sampleTime": sample("2026-11-11T01:08:00Z"), "percentage": 97.3},
        },
        {
            "dataSource": watch(),
            "oxygenSaturation": {"sampleTime": sample("2026-11-11T01:09:00Z"), "percentage": 0},
        },
        {
            "dataSource": watch(),
            "oxygenSaturation": {"sampleTime": {"physicalTime": "soon"}, "percentage": 96},
        },
    ],
    "respiratory-rate-sleep-summary": [
        {
            "dataSource": watch(),
            "respiratoryRateSleepSummary": {
                "sampleTime": sample("2026-11-11T06:00:00Z"),
                "deepSleepStats": stats(13.0, 1.2, 4.7),
                "fullSleepStats": stats(13.5, 1.0, 5.0),
            },
        }
    ],
    "weight": [
        {
            "dataSource": {"platform": "FITBIT_WEB_API"},
            "weight": {"sampleTime": sample("2026-11-10T07:44:16Z"), "weightGrams": 72500},
        }
    ],
    "steps": [
        {
            "dataSource": watch(),
            "steps": {
                "interval": interval("2026-11-11T08:02:00Z", "2026-11-11T08:03:00Z"),
                "count": "6",
            },
        },
        {
            "dataSource": {"device": {"displayName": "MobileTrack"}, "platform": "FITBIT"},
            "steps": {
                "interval": interval("2026-11-11T08:02:00Z", "2026-11-11T08:03:00Z"),
                "count": "9",
            },
        },
        {
            "dataSource": {},
            "steps": {
                "interval": interval("2026-11-11T08:03:00Z", "2026-11-11T08:04:00Z"),
                "count": "0",
            },
        },
    ],
    "sleep": [
        sleep(
            "100",
            "2026-11-10T22:30:00Z",
            "2026-11-11T06:00:00Z",
            450,
            stages=[
                stage("LIGHT", "2026-11-10T22:30:00Z", "2026-11-10T22:43:30Z"),
                stage("DEEP", "2026-11-10T22:43:30Z", "2026-11-10T23:30:00Z"),
                stage("REM", "later", "2026-11-10T23:40:00Z"),
            ],
        ),
        sleep("101", "2026-11-11T09:00:00Z", "2026-11-11T09:40:00Z", 40, kind="CLASSIC"),
        sleep("102", "2026-11-11T11:30:00Z", "2026-11-11T11:45:00Z", 15, processed=False),
    ],
    "heart-rate": [
        {
            "dataSource": watch(),
            "heartRate": {"sampleTime": sample("2026-11-11T01:00:05Z"), "beatsPerMinute": "50"},
        },
        {
            "dataSource": watch(),
            "heartRate": {"sampleTime": sample("2026-11-11T01:00:35Z"), "beatsPerMinute": "54"},
        },
        {
            "dataSource": watch(),
            "heartRate": {"sampleTime": sample("2026-11-11T01:01:10Z"), "beatsPerMinute": "60"},
        },
        {"dataSource": watch(), "heartRate": {"sampleTime": sample("2026-11-11T01:01:20Z")}},
    ],
}


def _moment(data_type: str, point: dict) -> str:
    """What a filter compares for a data point: its date, sample time, interval start, or a
    sleep's end."""
    body = next(v for k, v in point.items() if k not in ("name", "dataSource"))
    if "date" in body:
        d = body["date"]
        return f"{d['year']:04d}-{d['month']:02d}-{d['day']:02d}"
    if "sampleTime" in body:
        return body["sampleTime"].get("physicalTime", "")
    if data_type == "sleep":
        return body["interval"]["endTime"]
    return body["interval"]["startTime"]


@dataclass
class FakeGoogle:
    """Answers like Google: tokens for the synthetic client, and the data points in ``data``
    whose time falls in a request's filter, ``page`` at a time."""

    data: dict[str, list[dict]] = field(
        default_factory=lambda: {k: list(v) for k, v in DATA.items()}
    )
    page: int = 2
    #: Answers given before the normal ones, as (status, body), for any API request.
    failures: list[tuple[int, str]] = field(default_factory=list)
    refresh_ok: bool = True
    linked: bool = True
    granted: str = SCOPES
    filters: list[tuple[str, str]] = field(default_factory=list)
    tokens_issued: int = 0

    def __call__(self, request: Received) -> tuple[int, str]:
        if request.path == "/token":
            return self._token(urllib.parse.parse_qs(request.body))
        if self.failures:
            return self.failures.pop(0)
        if request.headers.get("authorization") != f"Bearer access-{self.tokens_issued}":
            return 401, json.dumps({"error": {"code": 401, "status": "UNAUTHENTICATED"}})
        if request.path == "/v4/users/me/identity":
            if not self.linked:
                return 400, json.dumps(
                    {
                        "error": {
                            "code": 400,
                            "message": "The account is not linked to Google Health.",
                            "details": [{"reason": "ACCOUNT_NOT_LINKED"}],
                        }
                    }
                )
            return 200, json.dumps({"healthUserId": "1"})
        match = re.fullmatch(r"/v4/users/me/dataTypes/([a-z0-9-]+)/dataPoints", request.path)
        if not match:
            return 404, "{}"
        data_type = match.group(1)
        found = RANGE.fullmatch(request.query.get("filter", ""))
        assert found, request.query
        self.filters.append((data_type, request.query["filter"]))
        low, high = found.group(2), found.group(3)
        points = [p for p in self.data.get(data_type, []) if low <= _moment(data_type, p) < high]
        start = int(request.query.get("pageToken", "0"))
        body: dict[str, Any] = {"dataPoints": points[start : start + self.page]}
        if start + self.page < len(points):
            body["nextPageToken"] = str(start + self.page)
        return 200, json.dumps(body)

    def _token(self, form: dict[str, list[str]]) -> tuple[int, str]:
        value = {k: v[0] for k, v in form.items()}
        if value.get("client_id") != CLIENT_ID or value.get("client_secret") != CLIENT_SECRET:
            return 401, json.dumps({"error": "invalid_client"})
        if value.get("grant_type") == "authorization_code":
            if value.get("code") != CODE or not value.get("code_verifier"):
                return 400, json.dumps({"error": "invalid_grant"})
            return 200, json.dumps({"refresh_token": REFRESH, "scope": self.granted})
        if value.get("refresh_token") != REFRESH or not self.refresh_ok:
            return 400, json.dumps({"error": "invalid_grant", "error_description": "expired"})
        self.tokens_issued += 1
        return 200, json.dumps({"access_token": f"access-{self.tokens_issued}", "expires_in": 3600})
