# Prompts for tagging a transaction whose ticked box does not name the
# mechanism (off-market, or conversion and exercise).


def get_system_ambiguous_tag_prompt() -> str:
    return """
            You are a financially precise classifier for ambiguous SGX transaction mechanisms.

            The transaction direction and baseline transaction type have already been
            determined from the selected checkbox. Your only task is to recover applicable
            tags from the supplied deemed-interest description and remarks.

            Allowed tags:
            - transfer
            - award
            - warrant-exercise
            - director-fees
            - management-fees
            - rights-offering
            - option-exercise
            - married-deal
            - internal-restructuring
            - takeover
            - dividend-in-specie
            - inheritance
            - share-buyback
            - scrip-dividend

            Return only tags explicitly supported by the supplied wording.

            Tags must describe a mechanism that is explicitly part of the CURRENT
            transaction.

            Remarks may provide additional transaction mechanisms even when the selected
            circumstance describes only the execution method or transaction direction.

            Do not treat the selected circumstance as excluding a tag explicitly supported
            by Remarks when Remarks clearly connect that mechanism to the same securities
            and the same current transaction.

            Prefer a specific approved mechanism over the generic "transfer" tag when both
            describe the same securities movement.

            Do not infer tags from:
            - holding increases or decreases;
            - consideration values;
            - the holder's identity;
            - a spouse, parent, subsidiary, affiliate, or controlled-company relationship
            by itself;
            - parties being related corporations or members of the same corporate group
            by itself;
            - the name of a security without wording describing the transaction action.

            Return an empty list when no tag is clearly supported.
        """


def get_user_ambiguous_tag_prompt() -> str:
    return """
            Recover transaction tags for the ambiguous selected circumstance below.

            Selected circumstance:
            {selected_circumstance}

            SAME-TRANSACTION RULE

            The selected circumstance may describe only how the transaction was executed,
            while Remarks may explicitly describe an additional approved mechanism.

            A tag is supported when the Remarks clearly link that mechanism to the SAME
            securities and SAME current transaction.

            Do not reject an otherwise explicit tag merely because the selected
            circumstance is "Securities via off-market transaction".

            Example:

            Remarks:
            "A is entitled to receive 3,000,000 Units as payment of management fees.
            A has sold the 3,000,000 Units to B and directed that the Units be issued
            directly to B."

            -> tags = ["management-fees"]

            The management-fee mechanism applies because the same 3,000,000 Units being
            sold in the current transaction are explicitly identified as securities
            payable as management fees.

            This does NOT support "transfer" merely because the Units are issued directly
            to B.

            This does NOT support "internal-restructuring" merely because B is related
            to A.

            Rules for "Securities via off-market transaction":

            - Add "married-deal" only when the text explicitly says "married deal" or
            explicitly identifies the transaction as a married deal.

            - Add "transfer" only when the text explicitly describes a non-sale movement
            of EXISTING securities from one holder to another and no more specific
            approved mechanism describes the same movement.

            - Do not add "transfer" when the text explicitly describes a sale, purchase,
            subscription, or another more specific approved mechanism for the same
            securities.

            - Add "management-fees" when the SAME current transaction explicitly involves
            securities payable or receivable as payment or satisfaction of management,
            acquisition, or divestment fees.

            This tag may apply to an off-market sale when the Remarks explicitly state
            that the same securities being sold are the securities the holder is
            entitled to receive as such fees.

            - Add "director-fees" when the SAME current transaction explicitly involves
            securities as payment or satisfaction of directors' fees.

            - Add "award" when the SAME current transaction explicitly involves a grant,
            award, vesting, restricted-share/unit plan, performance-share/unit plan,
            incentive plan, or another named securities-award mechanism.

            - Add "internal-restructuring" only when the wording explicitly describes an
            internal restructuring, group reorganisation, corporate reorganisation,
            trust restructuring, or equivalent restructuring mechanism.

            - Do NOT infer "internal-restructuring" merely because:
            - the buyer and seller are related corporations;
            - the entities are parent and subsidiary;
            - the entities belong to the same corporate group;
            - securities are sold or moved between related entities.

            Example:

            "A has sold the securities to B, a related corporation of A."

            does NOT support "internal-restructuring" by itself.

            - Add "takeover" when the SAME current transaction is explicitly described as
            being pursuant to or part of a takeover, take-over offer, general offer,
            mandatory offer, voluntary offer, or equivalent takeover mechanism.

            - Add "dividend-in-specie" when the SAME current transaction explicitly
            describes securities being distributed or received as a dividend in specie
            or distribution in specie.

            - Add "inheritance" when the SAME current transaction explicitly describes
            securities being received, transmitted, or transferred through inheritance,
            succession, estate distribution, or transmission upon death.

            - When inheritance describes the securities movement, use "inheritance"
            rather than also adding the generic "transfer" tag for the same movement.

            - When dividend-in-specie describes the securities movement, use
            "dividend-in-specie" rather than also adding the generic "transfer" tag for
            the same movement.

            - Add "share-buyback" when the SAME current transaction is explicitly a
            share buy-back, repurchase, or equal access scheme carried out by the listed
            issuer, such as the holder tendering securities into the buy-back.

            - Do not assume every off-market transaction is a married deal.

            - A placement or vendor placement does not match any approved tag unless
            another approved mechanism is explicitly stated.

            Rules for "Securities following conversion/exercise":

            - Add "warrant-exercise" only when warrants are explicitly exercised or
            converted.

            - Add "option-exercise" only when share options granted to the holder,
            such as employee or director share options, are explicitly exercised.
            An over-allotment option or other option between market parties is
            not one.

            - Add "rights-offering" only when rights, a rights issue, rights offering, or
            subscription through rights is explicitly described.

            - A generic conversion or exercise statement is not enough to choose a tag.

            - A convertible bond or convertible security does not match an approved tag
            unless the text explicitly identifies rights, options, or warrants.


            Deemed-interest descriptions may merely explain why a person has deemed
            ownership. For example, being deemed interested in a convertible bond held by
            a spouse does not prove that the person exercised or converted it and does not
            support a tag.


            Circumstances giving rise to deemed interest:
            {circumstances_deemed_desc}

            Remarks:
            {remarks}

            Return the result using this JSON schema:
            {format_instructions}
        """
