def get_system_trader_prompt() -> str:
    return """
            You are given one Singapore Exchange notification of interest: every
            record the filing contains, with each holder's share figures before and
            after, the reason given for the change, and each holder's boxes
            explaining its deemed interest.

            For each record marked "answer", name the traders: the parties whose
            own dealing changed the holding. A trader is a party that bought, sold,
            subscribed for, received or gave up the securities, or a fund manager
            that dealt for the funds or portfolios it manages. Usually there is one;
            name more than one only when the filing says several parties dealt in
            this change, such as two sellers in one deal.

            Rules:
            - Use only the supplied filing. Do not use outside knowledge.
            - A bank, broker, nominee or custodian that only holds securities on
            someone's behalf is never the trader.
            - A parent or holding company that is deemed interested only because a
            subsidiary dealt is not the trader; the subsidiary is.
            - The holder can be its own trader: a fund manager whose deemed interest
            changed because it dealt for its portfolios traded itself.
            - The share figures show where in the group the holding changed, not who
            dealt: a holder whose deemed interest moved may still have had the
            dealing done by a party below it that did not file. Name a trader from
            the figures alone only when one holder in this filing is the only one
            whose own part of the change is non-zero, its own part being its change
            less the change of the holders it is deemed interested through. With
            only one holder in the filing, the figures cannot show who dealt.
            - When the shares were issued or allotted to a party, such as units
            issued to a REIT manager as fees, that party is the trader.
            - When securities moved between two named parties, as in a transfer or
            sale between them, name the party on the holder's side of the change:
            the one that gave them up when the holder's interest fell, the one that
            received them when it rose.
            - When the change is passive, such as a change in the issuer's total
            share count, or the filing does not say who dealt, return no traders.
            - Return each trader by its full name as the filing writes it, without
            titles such as "Mr" or "Dr". When the boxes use a short name listed
            under "Short names defined elsewhere in this form", use the full name
            given there. Never return a description such as "the Manager's
            nominee" or "certain funds".
            - evidence for "text" is the shortest exact wording from the filing that
            says the traders dealt, copied character for character. A name on its
            own is not evidence.
            - Return one entry per record marked "answer".
        """


def get_user_trader_prompt() -> str:
    return """
            Listed issuer:
            {company_name}

            Records in this filing:
            {records}

            Holders' boxes:
            {boxes}

            Short names defined elsewhere in this form:
            {definitions}

            Return the result using this JSON schema:
            {format_instructions}
        """
