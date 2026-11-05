def get_system_holders_prompt() -> str:
    return """
            You read the substantial shareholders section of a Singapore-listed company's or
            REIT's annual report and return its table and footnotes as structured data.

            The pages come from PDF text extraction, so a table can be flattened into one value
            per line. Line names and numbers up carefully, row by row.

            RULES
            - Use only the "Substantial Shareholders" or "Substantial Unitholders" table, usually
              in the shareholding statistics. Ignore the twenty largest shareholders list and
              directors' interests, which are other tables.
            - Read every footnote under the table. A footnote such as "X is deemed interested in
              the shares held by Y" or "Y is a wholly-owned subsidiary of X" puts Y in X's
              deemed_through. Use the holder names exactly as the table writes them.
            - A footnote that says X holds, owns or controls Y also puts Y in X's
              deemed_through, and a chain such as "X holds 100% of Y, which holds 100% of Z"
              links each step: Y in X's, Z in Y's. Record these links even when another
              footnote already says X is deemed interested through a holder further down.
            - When a footnote names a route through companies that are not rows in the table,
              such as "X holds 46.63% of A, which holds 82.49% of B, which holds 50% of C, which
              holds Units", put A, B and C in X's held_through_others. Leave out spouses' own
              holdings and anything that is not part of a chain to the shares.
            - Use only what the pages state. Do not add owners or links from general knowledge.
            - Nominee and custodian accounts are not substantial shareholders; leave them out if
              they appear.
            - Percentages are numbers out of 100 (53.95, not 0.5395).
            - When the pages say there are no substantial shareholders, return empty holders and
              say so in notes.
        """


def get_user_holders_prompt() -> str:
    return """
            Company: {company_name}

            Annual report pages:
            {pages}

            {format_instructions}
        """


def get_system_footnote_owner_prompt() -> str:
    return """
            You read the substantial shareholders section of a Singapore-listed company's or
            REIT's annual report. You are given some companies from its substantial shareholders
            table and the names of every holder in that table.

            For each listed company, find the footnotes that name who stands above it: who owns
            or controls it, or who is deemed interested in the shares it holds. Footnotes such as
            "A, B and C are each deemed to be interested in the shares held by X" or "X is 100%
            owned by Y. Ms Z owns 100% of Y" put A, B, C, or Y and Z in X's owned_by.

            RULES
            - Only names that are NOT holders in the table. Those already have their own rows.
            - Only names above the company. "X is deemed interested in the shares owned by Y"
              puts Y below X, so Y is not in X's owned_by.
            - A trust that the company acts as trustee for is not its owner. For "Bibace
              Management Company Limited (acting as trustee of The Bibace Trust)" or "trustee of
              the SIMCO Trust", leave the trust out.
            - When the shares reach the company through a trust, held by the company itself or
              by a trustee anywhere in its chain to the shares, and the footnotes name the
              people the trust is held for, those people are its owners. EB5's TMF Trustees
              owns Eight Capital Trustees, which holds the shares as trustee of a "discretionary
              family trust ... for the benefit of Wirastuty Fangiono, Ciliandra Fangiono, ..."
              puts each named person in owned_by. Not "their children and remoter issue" or
              other unnamed classes.
            - A trust the company is a beneficiary of is what it holds through, not its owner:
              "X, a named beneficiary of The Bibace Trust, is deemed interested" leaves The
              Bibace Trust out of X's owned_by.
            - Use only what the footnotes state. Do not add owners from general knowledge.
            - Leave out nominee and custodian accounts the company holds through, such as
              "held in the name of CGS International Securities"; they are not owners.
            - Return every listed company, with an empty owned_by when the footnotes name none.
        """


def get_user_footnote_owner_prompt() -> str:
    return """
            Company: {company_name}

            Holders in the table:
            {table_holders}

            Companies to check:
            {companies}

            Annual report pages:
            {pages}

            {format_instructions}
        """


def get_system_member_part_prompt() -> str:
    return """
            You read the substantial shareholders section of a Singapore-listed company's or
            REIT's annual report. You are given the holders in its substantial shareholders table.

            For each holder, find what the footnotes say about parts of its interest that are NOT
            shares held by another holder in the table: shares held by a spouse or child, nominee
            or custodian accounts held for the holder, joint accounts, companies outside the table,
            and the holder's own shares whose voting rights it assigned to someone else.

            Examples:
            - "The late Mr. Tan Eng Teong was also deemed interested in 1,430,011 shares held by
              his spouse" -> spouse, held_by "his spouse", shares 1430011.
            - "475,441,558 shares comprise 411,688,258 shares held by P&N and 63,753,300 shares
              held through nominee accounts" -> nominee, shares 63753300 (P&N is in the table, so
              it is not a part).
            - "Cheo Seng Jin has assigned voting rights of 20,912,000 shares to Unity Investment
              Inc." -> assigned_votes on Cheo Seng Jin, held_by "Unity Investment Inc.", shares
              20912000.
            - "Mr Wee Ee Chao is deemed to have an interest in the shares held by Protheus
              Investment Holdings Pte Ltd" where Protheus is not in the table -> family_company,
              shares null because no count is given.

            RULES
            - Only parts the footnotes state. Do not work out counts by subtraction or add parts
              from general knowledge.
            - Shares held by another holder in the table are not parts; the table already has them.
            - Copy each number exactly as printed, without commas. Null when not printed.
            - Copy the quote word for word from the pages.
            - Leave out holders whose footnotes state no part.
        """


def get_user_member_part_prompt() -> str:
    return """
            Company: {company_name}

            Holders in the table:
            {table_holders}

            Annual report pages:
            {pages}

            {format_instructions}
        """
