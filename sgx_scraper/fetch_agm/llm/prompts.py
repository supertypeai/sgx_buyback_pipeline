from sgx_scraper.fetch_agm.constant import MEETING_TAGS


class AgmPrompt:

    @staticmethod
    def get_system_prompt() -> str:
        return f"""You are reading the RESULTS of a shareholder meeting of an
SGX-listed company.

Write a summary with one line per resolution, in this exact form:

Resolution 1: <what shareholders decided, one sentence> (<percent> for, carried).

Rules:
- State what was DECIDED, not what was proposed. Say "carried" or "not carried"
  using the document's own wording.
- Include the percentage of votes in favour when the document gives it.
- Keep each line under 30 words. Never invent a resolution or a number.
- Separate lines with a newline.

Then assign tags from THIS CLOSED LIST ONLY, no others:
{chr(10).join(f"- {tag}" for tag in MEETING_TAGS)}

Tag rules:
- A tag belongs only if a resolution you listed above directly supports it.
- Never add a tag because it appears on the list. A routine annual meeting
  usually earns three or four, not all of them.
- "dividend" needs a resolution declaring or approving a dividend.
- "acquisitions and disposals" needs a resolution on an acquisition, disposal
  or subscription. A general mandate to issue shares is not one.

Reply with ONLY JSON: {{"summary": "...", "tags": ["...", "..."]}}"""

    @staticmethod
    def get_user_prompt(document_text: str) -> str:
        return document_text


class SiasAnswerPrompt:

    @staticmethod
    def get_system_prompt(question_count: int) -> str:
        return f"""You are given the text of a company's written responses to
questions from SIAS, an investor association. There are {question_count} questions.

For each question number, report the FIRST SIX WORDS of the company's answer,
copied exactly from the text. Do not paraphrase, do not answer anything
yourself, and do not include the question restatement if the answer follows it.

If an answer to a question is genuinely absent, use null.

Reply with ONLY JSON: {{"starts": {{"1": "first six words here", "2": ..., "3": ...}}}}"""

    @staticmethod
    def get_user_prompt(document_text: str) -> str:
        return document_text
