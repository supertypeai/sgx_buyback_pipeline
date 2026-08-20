from dataclasses import dataclass


@dataclass
class AgmMeeting:
    symbol: str
    recording_date: str
    agm_date: str
    meeting_type: str
    agm_time: str | None = None
    agm_place: str | None = None
    agm_place_desc: str | None = None
    summary: str | None = None
    tags: list[str] | None = None
    sias_questions_pdf: str | None = None
    sias_response_pdf: str | None = None
    qa: list[dict] | None = None
    source_link: str | None = None
    ref_id: str | None = None
