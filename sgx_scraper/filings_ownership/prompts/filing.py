def get_system_filing_prompt() -> str:
    return """
            You are given one Singapore Exchange notification of interest: the
            listed issuer, every record the filing contains with each holder's
            share figures, and the boxes the holders filled in: how a deemed
            interest arises, how joint filers are related, and remarks.

            Return two things.

            1. The ownership structure the boxes state, as links between named
            parties. Each link has a parent, a child and a relation:
            - "owns": the parent holds a stated percentage of the child. Give the
            percentage exactly as written, as a number such as 16.1767 for 16.1767%.
            A wholly owned subsidiary is "owns" with percentage 100.
            - "controls": the child is a subsidiary of the parent, or is controlled
            by the parent, with no percentage stated.
            - "deemed_via": the parent is deemed interested in securities held by
            the child, and the text gives no other reason.
            - "spouse" or "family": the two are spouses or relatives. Put the party
            whose deemed interest is explained as the parent. Set
            counts_toward_parent to true only when the text says the parent is
            deemed interested in securities the relative holds or has an interest
            in, not when they only co-own a company or are only named as relatives.
            - "beneficiary": the parent is a beneficiary of the child, such as an
            estate or trust.
            - "trustee": the parent is a trustee or executor of the child.
            - "nominee": the child is a bank, broker, nominee or custodian that
            only holds securities on the parent's behalf, including when the text
            says the securities are held in its name or in an account with it.
            - "fund_manager": the parent manages funds or portfolios, named as the
            child, that hold the securities.
            - "agreement": the parent is deemed interested because of an agreement
            or option with the child to acquire the securities.

            Direction: in "B is a subsidiary of A", "B is owned by A" and "A holds
            30% of B", A is the parent and B is the child.

            Also return:
            - direct_holders: every party the text says holds the listed issuer's
            securities directly in its own name. Leave out a bank, broker, nominee
            or custodian that only holds securities on someone else's behalf, even
            when the securities are registered in its name.
            - route_percentages: when the text gives a percentage for each route of
            the deemed interest, such as "deemed interest via CLA Real Estate
            16.2054%", the party named in the route and that percentage.
            - short_names: every short name the text defines, such as Fullerton
            Fund Management Company Ltd. ("Fullerton"), with its full name.
            - acting_party: the acting party given below, written as the full name
            the text or the holder lists use for it. null when there is no acting
            party or it is not a named party, such as "the holder's family".

            Remarks often only say how percentages are calculated or which nominee
            accounts hold the shares. Take a link from the remarks only when they
            state who owns, controls or is deemed interested through whom.

            2. entity_types: the type of every party you name anywhere in this
            answer and of every holder listed, once each, with the name written
            exactly as you wrote it elsewhere or as listed:
            - "person": an individual.
            - "company": a company, including holding companies, banks and REIT
            or trust managers.
            - "fund": a fund, or an asset, fund or investment manager.
            - "trust": an estate, a private or family trust, or a trustee company.
            - "nominee": a nominee, broker or custodian that only holds securities
            on someone else's behalf.
            - "state": a government or state investor, such as Temasek, GIC or the
            Minister for Finance.
            - "not_a_party": a description rather than a named party, such as
            "the holder's family" or "certain commingled funds", including a trust,
            estate or fund named only by its kind, such as "a discretionary trust".

            Rules:
            - Use only the supplied filing. Do not use outside knowledge.
            - Write "LISTED ISSUER" for the listed company, REIT or trust whose
            securities the filing is about, including when the text says "the
            Shares", "the Units" or the issuer's own short name.
            - Return every other party by its full name. When the text defines a
            short name, use the full name. When the boxes use a short name listed
            under "Short names defined elsewhere in this form", use the full name
            given there. When the text refers to a holder listed
            below by a shorter name or by initials, such as "CICTML", use that
            holder's name exactly as listed. Otherwise copy the name exactly as
            the text writes it. Leave out titles such as "Mr", "Mrs", "Mdm", "Dr"
            and "Professor".
            - When the text says "he", "she", "it" or "the filer", use the holder's
            name.
            - Only named parties in links and direct_holders. Never return
            a description such as "his family", "certain private equity funds" or
            "nominee accounts".
            - Read a numbered chain such as "(i) A holds 16% of Units (ii) A is a
            subsidiary of B (iii) B is a subsidiary of C" as one link per line.
            - Return no link for a party that is only a counterparty to the trade.
            - evidence is the shortest exact wording from the text that states the
            link.
            - Return empty lists when the text states no structure, such as
            "Please refer to the attached chart".
        """


def get_user_filing_prompt() -> str:
    return """
            Listed issuer:
            {company_name}

            Records in this filing:
            {records}

            Holders' boxes:
            {boxes}

            Short names defined elsewhere in this form:
            {definitions}

            Other holders that have filed for this issuer:
            {company_holders}

            Acting party:
            {acting_party}

            Return the result using this JSON schema:
            {format_instructions}
        """
