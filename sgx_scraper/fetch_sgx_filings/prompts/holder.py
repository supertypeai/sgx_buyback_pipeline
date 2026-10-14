# Prompts about the holder: the casing of a name filed in capitals, and the
# party that acted when only a deemed interest moved.


def get_system_holder_name_casing_prompt() -> str:
    return """
            You are given the name of a person or organisation written entirely in
            capital letters, taken from a Singapore Exchange filing.

            Rewrite it with standard capitalisation.

            Rules:
            - Change only upper and lower case. Keep every letter, digit, space, and
            punctuation mark exactly where it is. Do not add, remove, reorder,
            abbreviate, expand, or correct anything.
            - Capitalise the words of personal names and ordinary words:
            "WONG FONG FUI" -> "Wong Fong Fui".
            - Keep acronyms, initialisms, and Roman numerals in capitals:
            REIT, DC, SPV, UK, DBS, II, VI.
            - Use the usual form for company suffixes:
            "PTE. LTD." -> "Pte. Ltd.", "SDN. BHD." -> "Sdn. Bhd.", "LIMITED" -> "Limited".
            - Keep titles as words: "DATO' SERI" -> "Dato' Seri", "TAN SRI" -> "Tan Sri".

            Examples:
            "KEPPEL DC REIT MANAGEMENT PTE. LTD" -> "Keppel DC REIT Management Pte. Ltd"
            "NTCP SPV VI" -> "NTCP SPV VI"
            "SINN HIN COMPANY SDN. BHD" -> "Sinn Hin Company Sdn. Bhd"
            "DATO' NOORASHIDAH BINTI AHMAD" -> "Dato' Noorashidah Binti Ahmad"
        """


def get_user_holder_name_casing_prompt() -> str:
    return """
            Name:
            {holder_name}

            Return the result using this JSON schema:
            {format_instructions}
        """


def get_system_acting_party_prompt() -> str:
    return """
            You are given text from a Singapore Exchange notification of a change
            in interest. The current holder's direct holding did not change. Only
            the holder's deemed interest changed, which means another party
            carried out the transaction and the holder is deemed interested in
            that party's securities.

            Return acting_party: the party the filing states carried out the
            transaction, that is the party that acquired, disposed of, received,
            subscribed for, or transferred the securities.

            Rules:
            - Use only the supplied text. Do not use outside knowledge.
            - Copy the party's name exactly as the filing writes it. Leave out a
            quoted short name in brackets after it, such as ("QMH"). Leave out a
            title such as "Mr", "Mrs" or "Ms" before a personal name.
            - When the filing refers to the party by a short name it defines
            elsewhere in the text, such as "Fullerton" for Fullerton Fund
            Management Company Ltd. ("Fullerton"), return the full name. When
            the text never gives the full name, return the name as written.
            - When the party is a relative named only by relationship, such as
            "my spouse" or "his wife", write "the holder's spouse", "the holder's
            son", "the holder's daughter", and so on. When the relative is also
            named, return the name.
            - When several parties carried out the same transaction together,
            return them joined with "and", in the order the filing gives.
            - The acting party is the one whose own holding of the securities
            changed. It is not the counterparty on the other side of the trade,
            and not a broker, bank, nominee, custodian, or trustee that only
            holds the securities on behalf of the party.
            - The transaction type tells you which side to look for: for "buy"
            the party that acquired, for "sell" the party that disposed.
            - A description of why the holder is deemed interested, such as
            "X is deemed interested in the shares held by Y Pte Ltd", names the
            party holding the securities. Return that party only when the text
            ties the reported transaction to it, or when it is the only party
            through which the holder has a deemed interest.
            - Return null when the text does not state who carried out the
            transaction, when it leaves the choice between several parties
            open, or when the current holder carried it out.

            Examples:

            "Quan Min Holdings Pte. Ltd. acquired 4,092,400 ordinary shares for a
            total consideration of S$2,132,958.88. Dr Ng is deemed interested in
            the shares held by Quan Min Holdings Pte. Ltd."
            -> acting_party = "Quan Min Holdings Pte. Ltd."

            "Purchase of 10,000 shares by my spouse via market transaction"
            -> acting_party = "the holder's spouse"

            "Mr Tan is deemed interested in the shares held by A Pte Ltd, B Pte
            Ltd and his spouse." with nothing tying the transaction to one of them
            -> acting_party = null
        """


def get_user_acting_party_prompt() -> str:
    return """
            Current holder:
            {holder_name}

            Transaction type:
            {transaction_type}

            Filing text:
            {filing_text}

            Return the result using this JSON schema:
            {format_instructions}
        """
