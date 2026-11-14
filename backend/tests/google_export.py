"""Synthetic Google Takeout exports for the tests, in the layout Google gives them: the folder and
file names, headers, timestamp formats and the ``-1``, ``0.0`` and ``NaN`` the export uses for
values it does not have. Every value and identifier is made up."""

from __future__ import annotations

import zipfile
from pathlib import Path

ROOT = "Takeout/Google Health/"
ACTIVITY = ROOT + "Physical Activity_GoogleData/"
SLEEP = ROOT + "Health Fitness Data_GoogleData/"


def csv(header: str, *rows: str) -> bytes:
    return ("\n".join((header, *rows)) + "\n").encode()


SLEEPS_HEADER = (
    "sleep_id,sleep_type,minutes_in_sleep_period,minutes_after_wake_up,minutes_to_fall_asleep,"
    "minutes_asleep,minutes_awake,minutes_longest_awakening,minutes_to_persistent_sleep,"
    "start_utc_offset,sleep_start,end_utc_offset,sleep_end,data_source,algorithm_version,"
    "sleep_created,sleep_last_updated"
)
STAGES_HEADER = (
    "sleep_id,sleep_stage_id,sleep_stage_type,start_utc_offset,sleep_stage_start,end_utc_offset,"
    "sleep_stage_end,data_source,algorithm_version,sleep_stage_created,sleep_stage_last_updated"
)
SCORES_HEADER = (
    "sleep_id,sleep_score_id,data_source,score_utc_offset,score_time,overall_score,duration_score,"
    "composition_score,revitalization_score,sleep_time_minutes,deep_sleep_minutes,"
    "rem_sleep_percent,resting_heart_rate,sleep_goal_minutes,waso_count_long_wakes,"
    "waso_count_all_wake_time,restlessness_normalized,hr_below_resting_hr,sleep_score_created,"
    "sleep_score_last_updated"
)
RR_HEADER = (
    "timestamp,deep sleep stats - milli breaths per minute,deep sleep stats - standard deviation "
    "milli breaths per minute,deep sleep stats - signal to noise,light sleep stats - milli breaths "
    "per minute,light sleep stats - standard deviation milli breaths per minute,light sleep stats "
    "- signal to noise,rem sleep stats - milli breaths per minute,rem sleep stats - standard "
    "deviation milli breaths per minute,rem sleep stats - signal to noise,full sleep stats - milli "
    "breaths per minute,full sleep stats - standard deviation milli breaths per minute,full sleep "
    "stats - signal to noise,data source"
)
STRESS_HEADER = (
    "DATE,UPDATED_AT,STRESS_SCORE,SLEEP_POINTS,MAX_SLEEP_POINTS,RESPONSIVENESS_POINTS,"
    "MAX_RESPONSIVENESS_POINTS,EXERTION_POINTS,MAX_EXERTION_POINTS,STATUS,CALCULATION_FAILED"
)
T = "2026-11-11T09:00:00Z"

#: Night of 10 Nov 2026 (+01:00): the main sleep 23:30-07:00 local, a longer recording of the
#: same sleep by an older algorithm, and a nap at 10:00 the next morning, before noon, so of the
#: same night. Night of 22 Jun 2025: a sleep whose times the export gives in local time labelled
#: +00:00 (02:25-09:00), while its stages carry the true +02:00. Night of 13 Jul 2026 (+02:00):
#: one sleep 23:00-06:30 local.
SLEEPS = csv(
    SLEEPS_HEADER,
    f"100,STAGES,450,0,0,420,30,12,0,+01:00,2026-11-10T22:30:00Z,+01:00,2026-11-11T06:00:00Z,"
    f"DERIVED,sleep_session_v2,{T},{T}",
    f"101,STAGES,460,0,0,430,30,12,0,+01:00,2026-11-10T22:30:00Z,+01:00,2026-11-11T06:10:00Z,"
    f"DERIVED,sleep_session_v1,{T},{T}",
    f"102,CLASSIC,40,0,0,35,5,3,0,+01:00,2026-11-11T09:00:00Z,+01:00,2026-11-11T09:40:00Z,"
    f"DERIVED,sleep_session_v2,{T},{T}",
    f"200,STAGES,450,0,0,430,20,8,0,+02:00,2026-07-13T21:00:00Z,+02:00,2026-07-14T04:30:00Z,"
    f"MANUAL,sleep_session_v1,{T},{T}",
    f"300,STAGES,395,0,0,380,15,5,0,+00:00,2025-06-23T02:25:00Z,+00:00,2025-06-23T09:00:00Z,"
    f"DERIVED,sleep_session_v1,{T},{T}",
    "bad,STAGES,1,0,0,1,0,0,0,+01:00,not a time,+01:00,2026-11-11T06:00:00Z,DERIVED,v,,",
)
STAGES = csv(
    STAGES_HEADER,
    f"100,1,LIGHT,+01:00,2026-11-10T22:30:00Z,+01:00,2026-11-10T22:43:30Z,DERIVED,v2,{T},{T}",
    f"100,2,DEEP,+01:00,2026-11-10T22:43:30Z,+01:00,2026-11-10T23:30:00Z,DERIVED,v2,{T},{T}",
    f"101,3,LIGHT,+01:00,2026-11-10T22:30:00Z,+01:00,2026-11-10T22:40:00Z,DERIVED,v1,{T},{T}",
    f"999,4,AWAKE,+01:00,2026-11-12T23:10:00Z,+01:00,2026-11-12T23:12:00Z,DERIVED,v2,{T},{T}",
    f"300,5,LIGHT,+02:00,2025-06-23T00:25:00Z,+02:00,2025-06-23T00:40:00Z,DERIVED,none,{T},{T}",
    f"300,6,DEEP,+02:00,2025-06-23T00:40:00Z,+02:00,2025-06-23T01:10:00Z,DERIVED,none,{T},{T}",
)
SCORES = csv(
    SCORES_HEADER,
    f"100,1,DERIVED,+01:00,2026-11-11T06:00:00Z,87.5,-1,-1,-1,420,52,21.2,58,480,8.5,26,0.15,"
    f"0.91,{T},{T}",
    f"101,2,DERIVED,+01:00,2026-11-11T05:50:00Z,73.4,-1,-1,-1,410,50,20.0,58,480,8,25,0.2,0.9,"
    f"{T},{T}",
    f"200,3,MANUAL,+02:00,2026-07-14T04:30:00Z,74.4,77.5,81.6,61.1,430,55,18.0,60,480,1.7,10,"
    f"0.3,0.97,{T},{T}",
    f"300,4,DERIVED,+00:00,2025-06-23T09:00:00Z,81.0,-1,-1,-1,380,60,20.0,55,480,2,12,0.2,0.95,"
    f"{T},{T}",
)
STRESS = csv(
    STRESS_HEADER,
    "2026-11-11T00:00:00,2026-11-11T09:26:50.516,63,25,30,19,30,19,40,READY,false",
    "2026-11-12T00:00:00,2026-11-12T09:20:09.423,77,0,0,0,0,0,0,READY_NOT_PREMIUM,false",
    "2026-11-13T00:00:00,2026-11-13T09:00:00.000,0,0,0,0,0,0,0,NO_DATA,true",
    "2026-11-14T00:00:00,2026-11-14T09:00:00.000,70,0,0,0,0,0,0,READY,true",
)


def activity(folder: str, name: str, content: bytes) -> tuple[str, bytes]:
    return f"{ACTIVITY}{folder}/{name}", content


def export_files() -> dict[str, bytes]:
    """Every file of a synthetic export, by its path in the zip."""
    files = dict(
        [
            activity(
                "Daily Resting Heart Rate",
                "daily_resting_heart_rate.csv",
                csv(
                    "timestamp,beats per minute,data source",
                    "2026-11-11T00:00:00Z,58.252,Google Health App",
                    "2026-11-12T00:00:00Z,59.0,Google Health App",
                    "2026-11-13T00:00:00Z,N/A,Google Health App",
                ),
            ),
            activity(
                "Daily Heart Rate Variability",
                "daily_heart_rate_variability.csv",
                csv(
                    "timestamp,average heart rate variability milliseconds,non rem heart rate "
                    "beats per minute,entropy,deep sleep root mean square of successive "
                    "differences milliseconds,data source",
                    "2026-11-11T00:00:00Z,35.579,57.988,2.247,0.0,Google Health App",
                ),
            ),
            activity(
                "Daily Sleep Temperature Derivations",
                "daily_sleep_temperature_derivations.csv",
                csv(
                    "timestamp,nightly temperature celsius,baseline temperature celsius,relative "
                    "nightly stddev 30d celsius,data source",
                    "2026-11-11T00:00:00Z,32.63077481840193,NaN,NaN,Google Health App",
                ),
            ),
            activity(
                "Daily Readiness",
                "daily_readiness.csv",
                csv(
                    "timestamp,score,type,readiness level,sleep readiness,heart rate variability "
                    "readiness,resting heart rate readiness,data source",
                    "2026-11-11,53,MEDIUM,TYPE_UNSPECIFIED,VERY_HIGH,MEDIUM,LOW,Google Health App",
                ),
            ),
            activity(
                "Cardio Acute Chronic Workload Ratio",
                "cardio_acute_chronic_workload_ratio.csv",
                csv(
                    "timestamp,ratio,label,data source",
                    "2026-11-11,1.0465,OPTIMAL_TRAINING,Google Health App",
                ),
            ),
            activity(
                "Respiratory Rate Sleep Summary",
                "respiratory_rate_sleep_summary_2026-11-01.csv",
                csv(
                    RR_HEADER,
                    "2026-11-11T06:00:00Z,13.0,1.2,4.763,12.4,1.0,6.48,-1.0,0.0,1.784,13.0,1.2,"
                    "4.763,Google Health App",
                ),
            ),
            activity(
                "Oxygen Saturation",
                "oxygen_saturation_2026-11-01.csv",
                csv(
                    "timestamp,oxygen saturation percentage,data source",
                    "2026-11-11T01:08:00Z,97.3,Pixel Watch 2",
                    "2026-11-11T01:09:00Z,0.0,Pixel Watch 2",
                ),
            ),
            activity(
                "Steps",
                "steps_2026-11-01.csv",
                csv(
                    "timestamp,steps,data source",
                    "2026-11-11T08:02:00Z,6,Pixel Watch 2",
                    "2026-11-11T08:02:00Z,9,MobileTrack",
                    "2026-11-11T08:03:00.250Z,0,Phone Health Connect",
                    "2026-11-11T08:04:00.25Z,4,Pixel Watch 2",
                    "yesterday,4,Pixel Watch 2",
                ),
            ),
            activity(
                "Weight",
                "weight.csv",
                csv(
                    "timestamp,weight grams,data source",
                    "2026-11-09T07:44:16Z,72500,Withings Fitbit 3P Web API",
                ),
            ),
            activity(
                "Heart Rate",
                "heart_rate_2026-11-11.csv",
                csv(
                    "timestamp,beats per minute,data source",
                    "2026-11-11T01:00:05Z,50.0,Pixel Watch 2",
                    "2026-11-11T01:00:35Z,54.0,Pixel Watch 2",
                    "2026-11-11T01:00:50Z,0.0,Pixel Watch 2",
                    "2026-11-11T01:01:10Z,60.0,Pixel Watch 2",
                    "2026-11-11T01:01:10Z,not a number,Pixel Watch 2",
                ),
            ),
            activity(
                "Calories",
                "calories_2026-11-01.csv",
                csv("timestamp,calories,data source", "2026-11-11T08:02:00Z,1.18,Pixel Watch 2"),
            ),
            activity("Steps", "steps_readme.txt", b"Steps per minute.\n"),
        ]
    )
    files[f"{SLEEP}UserSleeps_2026-02-24.csv"] = SLEEPS
    files[f"{SLEEP}UserSleepStages_2026-02-24.csv"] = STAGES
    files[f"{SLEEP}UserSleepScores_2026-02-24.csv"] = SCORES
    files[f"{ROOT}Stress Score/Stress Score.csv"] = STRESS
    files[f"{ROOT}User Profile_GoogleData/user_profile.csv"] = csv(
        "fitbit_id,email,timezone", "1,someone@example.com,Europe/Amsterdam"
    )
    return files


def write_zip(path: Path, files: dict[str, bytes]) -> Path:
    """Zip files by their paths in the zip."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, data in files.items():
            archive.writestr(name, data)
    return path


def write_split(inbox: Path, files: dict[str, bytes], stem: str = "takeout-1") -> list[Path]:
    """An export split over two zips as Takeout splits one: the sleep sessions in the second,
    everything else, their stages and scores included, in the first."""
    second = {n: d for n, d in files.items() if "/UserSleeps_" in n}
    first = {n: d for n, d in files.items() if n not in second}
    return [
        write_zip(inbox / f"{stem}-001.zip", first),
        write_zip(inbox / f"{stem}-002.zip", second),
    ]
