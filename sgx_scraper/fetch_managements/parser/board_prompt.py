from pydantic import Field, BaseModel


class BoardPageNumber(BaseModel):
    page_start: int | None = Field(
        default=None,
        description=(
            "The printed report page where the Board of Directors biographies, "
            "CVs, resumes, profiles, or equivalent director-information section "
            "starts. Return null when no matching section is found."
        ),
    )
    page_end: int | None = Field(
        default=None,
        description=(
            "The last printed report page belonging to the matching section. "
            "This must be one page before the next separate section begins. "
            "Return null when no matching section is found or when the end page "
            "cannot be determined from the table of contents."
        ),
    )
    explanation: str = Field(
        description=(
            "explain why you assign the value of page start and page end"
        )
    )


class BoardMemberDirectorItems(BaseModel):
    name: str = Field(
        description=(
            "Full name of a current member of the reporting company's Board of "
            "Directors, as stated in the provided annual report text."
        )
    )
    position: str | None = Field(
        description=(
            "Current board designation or position of the director at the reporting "
            "company, such as 'Chairman, Non-Executive and Independent Director' "
            "or 'Executive Director'. Return None when the board membership is clear "
            "but an explicit position cannot be determined from the provided text."
        )
    )
    age: int | None = Field(
        description=(
            "Director's age as explicitly stated in the annual report. "
            "Return only the stated age as an integer. Do not calculate age from "
            "a birth date, birth year, annual-report year, or any other information. "
            "Return None when age is not explicitly stated or cannot be reliably "
            "associated with this director."
        )
    ) 
    start_date: str | None = Field(
        description=(
            "Explicitly stated start or appointment date for this director's "
            "current board appointment or board position at the reporting company. "
            "Examples include 'Date of Appointment', 'Appointed as Director on', "
            "'Director since', or 'Joined the Board'. Return a full date as "
            "YYYY-MM-DD, a month-year date as YYYY-MM, and a year-only date as YYYY. "
            "Do not invent a missing day or month. Do not use company joining dates, management-role "
            "appointment dates, committee appointment dates, re-election dates when "
            "an original appointment date is available, external directorship dates, "
            "or inferred/calculated dates. Return None when no reliable board "
            "appointment/start date is explicitly stated."
        )
    )


class BoardMemberDirector(BaseModel):
    bod_payload: list[BoardMemberDirectorItems] = Field(
        description=(
            "Current members of the reporting company's Board of Directors extracted "
            "from the provided annual report section. Excludes management-only personnel, "
            "board committee memberships, external mandates, and former directors."
        )
    )


class PromptCollections:
    @staticmethod
    def get_system_board_page_prompt(is_fallback: bool = False):
        if is_fallback:
            section_selection_rules = """
                FALLBACK SECTION SELECTION

                Prefer a separate director biography, profile, CV, resume, or
                equivalent person-level section over a general "Board of Directors"
                section. Examples include:
                - "Board Profiles"
                - "Directors' Biographies"
                - "Information on Directors"

                Do not select a general "Board of Directors" section during this
                fallback pass.

                Do not select a disclosure about directors seeking re-election.
                It may cover only a subset of the current Board.
            """
        else:
            section_selection_rules = """
                PRIMARY SECTION SELECTION

                Select the general "Board of Directors" section when it exists.
                Do not replace it with a separate director biography, profile, CV,
                resume, or "Information on Directors" section.

                Only when no general "Board of Directors" section exists, select
                the best available director section.
            """

        return (
            """
            You identify the page range containing person-level information about
            the Board of Directors from an annual report table of contents.

            The goal is to locate the section most likely to contain detailed
            information about individual directors, such as their name, position,
            age, appointment/start date, professional background, qualifications,
            career history, or biography.

            """
            + section_selection_rules
            + """

            Do NOT choose sections that only discuss:
            - board composition;
            - corporate governance;
            - board meetings or attendance;
            - board responsibilities or practices;
            - committee structure;
            - a simple list of director names without person-level profile content.

            PAGE NUMBER RULES

            Use the printed report page numbers shown in the table of contents,
            not PDF page indexes.

            page_start is the printed page number associated with the selected
            section.

            page_end is exclusive of the next separate section. Find the next
            heading at the same or higher table-of-contents level and subtract one
            from its printed page number.

            Example:
            - selected section starts on page 7
            - next separate section starts on page 11

            Return:
            page_start = 7
            page_end = 10

            Do not include page 11.

            If there is no matching section, return null for both fields.

            During the fallback pass, return null for both fields when a complete
            page range cannot be determined.

            Return only the requested structured output.
            Never invent page numbers.
            """
        )

    @staticmethod
    def get_user_board_page_prompt(is_fallback: bool = False):
        if is_fallback:
            section_instruction = """
                This is the fallback pass. Prefer a separate director biography,
                profile, CV, resume, or "Information on Directors" section.

                Do not use a general "Board of Directors" section or a disclosure
                about directors seeking re-election. If a complete page range cannot
                be determined, return null for both fields.
            """
        else:
            section_instruction = """
                This is the primary pass. Select the general "Board of Directors"
                section when it exists.
            """

        return (
            """
            Find the page range containing person-level information about the
            company's Board of Directors.

            """
            + section_instruction
            + """

            Table of contents:
            {table_of_contents}

            Note:
                A contents entry may place its printed page number immediately
                before or above the section heading, or immediately after or below
                it.

                Associate an adjacent number with the heading only when both belong
                to the same visual column.

                Do not use numbers from a neighbouring column.

            Return the data in the following JSON schema:
            {format_instructions}
            """
        )

    @staticmethod
    def get_system_board_page_vision_prompt():
        return """
            You extract a printed page range from an annual report's table of contents image.

            Find the section containing person-level information about the Board of Directors,
            such as director profiles, biographies, qualifications, or appointment details.

            Use only printed report page numbers visibly associated with the section headings.
            Do not use PDF page indexes, dates, years, table values, or numbers from another
            visual column.

            page_start is the first printed page of the selected section.
            page_end is one page before the next separate section begins.

            Do not guess. Return null when the section or its page number cannot be determined.
            Return only the requested structured output.
        """

    @staticmethod
    def get_user_board_page_vision_prompt():
        return """ 
            Inspect the attached table of contents image.

            Identify the Board of Directors or director-profile section and return its printed
            page range. Use the next separate section to determine page_end.

            If the page numbers or section relationship are unclear, return null instead of
            guessing. Briefly explain the page selection.
            
            Return the data in the following JSON schema:
            {format_instructions}
        """

    @staticmethod
    def get_system_management():
        return """
            You extract CURRENT members of the reporting company's Board of Directors
            from annual-report text taken from a Board of Directors, director profile,
            biography, CV, resume, or equivalent section.

            Return only information explicitly supported by the provided text.

            For each current director, extract:
            - name
            - position
            - age
            - start_date

            RULES

            1. Extract only current members of the reporting company's Board of Directors.

            2. Preserve the director's current board designation as stated in the report.

            If the profile heading contains both a board designation and a current
            executive role, preserve both when they clearly belong to the same person.

            Example:
            "Executive Director and Chief Executive Officer"
            should remain:
            "Executive Director and Chief Executive Officer"

            3. If a person is clearly presented as a current director but no more
            specific board designation is given, set `position` to "Director".

            4. Do not use board committee roles such as Audit Committee Chairman,
            Nominating Committee Member, or Remuneration Committee Member as the
            person's main board position.

            5. Do not use directorships or positions held at OTHER companies as the
            person's position at the reporting company.

            6. AGE

            Extract `age` only when the person's age is explicitly stated.

            Examples:
            - "Age: 58"
            - "Aged 63"

            Do not calculate age from:
            - date of birth
            - birth year
            - annual-report year
            - employment dates
            - any other information

            Return null when age is not explicitly stated.

            7. START DATE

            `start_date` is the explicitly stated date when the person joined or
            was appointed to the Board of the reporting company.

            Valid wording may include:
            - "Date of Appointment"
            - "Date of appointment as Director"
            - "Appointed as Director"
            - "Director since"
            - "Joined the Board"
            - "First appointed to the Board"
            - equivalent wording that clearly refers to the person's board appointment

            Do not use:
            - date the person joined the company as an employee
            - CEO, CFO, COO, or other management appointment dates unless the text
                explicitly states that the same date is also the board appointment date
            - board committee appointment dates
            - re-election dates when an original appointment date is available
            - external directorship dates
            - employment-history dates
            - calculated or inferred dates

            Normalize dates using the available precision:
            - full day, month, and year: YYYY-MM-DD
            - month and year only: YYYY-MM
            - year only: YYYY

            Do not invent a missing day or month.

            Examples:
            "Director since 2018"
            -> start_date = "2018"

            "Appointed as Director in May 2019"
            -> start_date = "2019-05"

            "Date of Appointment: 15 March 2021"
            -> start_date = "2021-03-15"

            Return null when no reliable board appointment or start date is stated.

            8. NAME FORMATTING

            Preserve normal mixed-case spelling from the report.

            If PDF extraction makes the entire name uppercase or lowercase,
            convert it to natural display capitalization.

            Preserve:
            - initials
            - accents
            - apostrophes
            - hyphens
            - name order
            - non-Latin characters

            9. Exclude:
            - former directors
            - retired directors
            - directors-designate who have not yet taken office
            - management-only personnel
            - external board appointments

            10. Deduplicate the same director when they appear multiple times.

            11. PDF text may contain broken lines, flattened columns, or mixed profile
                text. Associate information with a director only when the relationship
                is clear. Do not guess.

            12. If no current directors can be reliably identified, return an empty
                `bod_payload`.

            Your output must follow the supplied structured response schema.
        """

    @staticmethod
    def get_user_management():
        return """
            Reporting company:
            {company_name}

            Extract the current members of the Board of Directors from the annual-report
            text below.

            For each director, extract their:
            - name, remove Mr/Ms if present 
            - current position
            - age, when explicitly available
            - board start date, when explicitly available, 
            in this format: 'day month year' for example '5 August 2005'

            Annual report text:
            {pdf_text}

            Return the output in the following JSON:
            {format_instructions}
        """
