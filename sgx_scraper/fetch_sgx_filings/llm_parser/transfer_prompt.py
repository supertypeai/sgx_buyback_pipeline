from pydantic import Field, BaseModel


class TransferParties(BaseModel):
    transferor: str | None = Field(
        default=None,
        description=(
            "The party whose holding is transferred AWAY (the 'from' side), taken "
            "from the circumstance description, e.g. \"CICTML's unitholding\" or a "
            "person's name. null if it cannot be identified from the description."
        ),
    )
    transferee: str | None = Field(
        default=None,
        description=(
            "The party RECEIVING the holding (the 'to' side), taken from the "
            "circumstance description, e.g. 'its key management personnel and "
            "eligible employees'. null if it cannot be identified from the description."
        ),
    )
    reasoning: str = Field(
        description="Briefly explain how you identified the transferor and transferee."
    )


SYSTEM_TRANSFER_PROMPT = """
            You identify the two parties of an off-market transfer reported in an SGX filing.
            Use the circumstance description and the filing party provided: identify who the
            securities moved FROM (the transferor) and who they moved TO (the transferee).
            When the description plainly says securities were gifted or transferred TO a
            recipient but does not name the giver, use the filing party as the transferor.
            Use the wording as it appears in the description as closely as possible and keep
            each side concise. Never invent a party. If either side is not identifiable from
            the description, return null for that side.
        """

USER_TRANSFER_PROMPT = """
            A transfer transaction was reported by the filing party below. From the
            circumstance description, identify the two sides of the transfer.

            - transferor: the party whose holding is transferred away (the 'from' side).
            - transferee: the party receiving the holding (the 'to' side).
            - Name only the party itself. Stop at the recipient and drop trailing
              qualifiers (plan/scheme names, purpose, "pursuant to ..." or "under the ..."
              clauses).
            - If the description says the filing party gave, gifted, or transferred securities
              TO a recipient but omits the giver's name, use the filing party as transferor.
              For example, with filing party "Jane Tan" and description "gift to daughter",
              return transferor "Jane Tan" and transferee "daughter".
            - Do not use the filing party as transferor when the description says securities
              were received FROM someone else or does not establish the direction of transfer.
            - If the description does not clearly identify both sides, return null for the
              side you cannot determine. Do not guess.

            Filing party (holder on the form): {holder_name}

            Circumstance description:
            {circumstances_desc}

            Return the data in the following JSON schema:
            {format_instructions}
        """

SYSTEM_FORM_3_TRANSFER_PROMPT = """
            You determine whether one Form 3 filing proves an off-market transfer.
            Use all shareholder records provided together. Return the transferor and
            transferee only when the filing clearly identifies both parties. Use the
            holder names from the records and never invent a party.
        """

USER_FORM_3_TRANSFER_PROMPT = """
            The following shareholder records came from one Form 3 filing. Their
            circumstances may identify transfer parties in another shareholder's
            Item 6, Item 8, or Item 9 text.

            Determine whether the filing proves one transfer between the holders
            whose direct holdings changed. If it does, return its transferor and
            transferee. Otherwise return null for both parties.

            Shareholder records:
            {records}

            Return the data in the following JSON schema:
            {format_instructions}
        """
