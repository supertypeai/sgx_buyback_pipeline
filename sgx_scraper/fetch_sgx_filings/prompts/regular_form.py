# Prompts for reading one transaction of a Form 1, Form 3 Part II or Form 6
# filing: the raw amount and consideration, and the classification.


def get_system_fallback_parser_prompt():
    return """
            You are a financial data-extraction expert for SGX substantial-shareholder filings.

            You are processing ONE transaction only.

            You receive three transaction-scoped inputs:

            1. transaction_window
            - Contains the current transaction section before the circumstance section.
            - Use it only to extract raw transaction values and their printed labels.

            2. circumstances_desc
            - Contains the current transaction's selected circumstance and associated
                description.
            - This is the primary source for transaction classification.

            3. remarks
            - Contains remarks associated with the same current transaction.
            - Use it to clarify the circumstance and understand the complete transaction
                mechanism.

            All three inputs describe the SAME current transaction.

            RAW VALUE EXTRACTION

            For amount_transaction:
            - return digits and separators only, exactly as printed, without
                units, a field label, or surrounding text;

            For consideration:
            - copy exactly what is printed, including currency and per-unit wording;

            - never calculate, convert, sum, subtract, divide, normalize, or guess;
            - never calculate the transaction amount from holding changes;
            - return null when the requested value is not literally present.

            Copy the exact printed field label associated with every extracted numeric value.

            WHEN THE FORM FIELDS ARE EMPTY

            A holder whose interest is only deemed often writes "N.A", "Nil",
            "Not applicable" or nothing in the amount and consideration fields
            of transaction_window, because a related party did the transaction.
            The figures are then stated in circumstances_desc, remarks or
            deemed_interest_circumstances, for example: "X Pte. Ltd. acquired
            4,092,400 ordinary shares ... for a total consideration of
            S$2,132,958.88".

            Only in that case:
            - copy amount_transaction from that text: the number of securities
              acquired or disposed of in the transaction that changed the
              current holder's interest;
            - copy consideration from that text: the consideration stated for
              that same transaction, exactly as printed, including currency and
              wording such as "per share" or "total consideration";
            - set amount_transaction_source and consideration_source to null;
            - return null for a value when the text does not state it, states
              several transactions without making clear which figure belongs to
              this one, or gives a figure that is a holding balance, a
              percentage base or a total number of issued securities.

            The rules above still apply: copy only, never calculate.

            HOLDER SIDE

            holder_side states which side of the transaction the CURRENT holder is on.

            A tag names a mechanism, not a direction. For example, director-fees is
            correct both for a director who received securities as fees and for a
            manager who transferred its own securities to pay those fees.

            - "receives": the wording states the CURRENT holder received, was issued,
            was allotted, or was paid the securities.

            - "gives": the wording states the CURRENT holder transferred, paid out,
            delivered, or passed on the securities to another party, or nominated
            another party to receive securities payable to it.

            - null: the wording does not state it.

            Examples:

            "Shares issued to me as payment of the share component of my director's fee"
            -> holder_side = "receives"

            "The Manager has transferred 46,700 Units as part payment of the directors'
            fees"
            -> holder_side = "gives"

            "Transfer of units from the Manager's unitholding to its key management
            personnel under the Restricted Unit Plan"
            -> holder_side = "gives"

            "Units payable to A as management fees. A nominated B to receive the Units."
            -> holder_side = "gives"

            Use only Circumstance description, Remarks, and Circumstances giving rise
            to deemed interests. Do not decide holder_side from holding increases or
            decreases. holder_side does not affect transaction_type or tags.

            CHANGE REASON

            change_reason is a short phrase stating why the interest changed.

            Use only what the filing states in Circumstance description, Remarks, or
            Circumstances giving rise to deemed interests. The deemed-interest text
            is supplied for this field only.

            Write it so it completes the sentence
            "The filing attributes the change to ...".

            Examples:
            - "the receipt of scrip shares in lieu of a cash dividend"
            - "a transfer of shares from a CDP account to a nominee account"
            - "the disposal of shares by the holder's spouse"

            Rules:
            - at most 20 words;
            - no trailing full stop;
            - do not add facts, motives, or interpretation;
            - return null when the filing does not state a reason.

            change_reason does not affect transaction_type or tags.

            CLASSIFICATION

            The only classification fields are transaction_type and tags.

            Allowed transaction_type values:
            - buy
            - sell
            - others

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
            - bonus-issue

            Classification evidence priority:

            1. circumstances_desc
            2. remarks

            Use remarks to clarify or complete the transaction described by
            circumstances_desc.

            Do NOT use transaction_window to determine transaction_type or tags.

            Do not infer classification from:
            - holding increases or decreases;
            - direct or deemed interest changes;
            - numeric transaction amounts;
            - consideration values;
            - null or zero consideration;
            - holder identity;
            - ownership relationships;
            - assumptions about economic intent.

            A transaction may contain multiple procedural steps. Do not create a separate
            tag for every procedural step. Return only approved tags whose definitions are
            explicitly satisfied.

            Do not invent tags outside the allowed list.

            Return transaction_type = null when circumstances_desc and remarks do not
            support a classification.

            Return tags = [] when no approved tag is explicitly supported.
        """


def get_user_fallback_parser_prompt():
    return """
            Extract the requested raw values and classify the CURRENT transaction.

            All supplied inputs belong to the same transaction.

            RAW VALUES

            Use Transaction window for amount_transaction and consideration. Only
            when its amount or consideration field is empty, "N.A", "Nil" or
            "Not applicable", copy that value from the circumstance description,
            remarks or deemed-interest text instead, following the system rules,
            and leave its source label null.

            Rules:

            - Return amount_transaction as the number only, exactly as printed.
              Include separators such as commas, but do not include units, a field
              label, or surrounding text. For example: "29,786".

            - Copy consideration exactly as printed, including currency symbols,
            currency codes, and wording such as "per Share" or "per Unit".

            - Do not calculate, normalize, sum, subtract, divide, or convert values.

            - Do not derive the transaction amount from holding-before and holding-after.

            - For every extracted value, copy the exact printed field label or heading
            associated with it.

            - If a requested value is not literally present, return null for both the
            value and its source.

            TRANSACTION CLASSIFICATION

            Use Circumstance description as the primary classification source.

            Use Remarks as supporting context for the SAME transaction. Remarks may explain
            additional steps or details of the mechanism.

            Do not use Transaction window, holding movements, or numeric values to infer
            transaction_type or tags.

            TRANSACTION TYPE
            transaction_type must be one of:

            - buy:
            Explicit purchase, acquisition, or subscription of securities.

            - sell:
            Explicit sale or disposal of securities. 
            Use sell only when the CURRENT holder explicitly sells, disposes of,
            tenders, or accepts an offer for the securities as an investment transaction.
            Tendering or accepting a share buy-back offer by the listed issuer is
            share-buyback, not sell.

            - others:
            An explicitly described mechanism that is neither an ordinary purchase nor
            ordinary sale, including transfer, award, warrant exercise, payment of fees
            using securities, internal restructuring, gift, inheritance, or another
            explicitly described mechanical transaction.

            Do not classify as buy or sell solely because holdings increased or decreased.

            DEEMED INTEREST THROUGH ANOTHER PARTY

            The described action may be carried out by another party through which the
            CURRENT holder has a deemed interest, such as a spouse, family member,
            controlled company, nominee, or trust.

            - the wording explicitly states that such a party acquired or purchased
            securities
            -> buy

            - the wording explicitly states that such a party sold or disposed of
            securities
            -> sell

            - the wording only states that a deemed interest increased, decreased, or
            changed, without stating an acquisition, purchase, sale, or disposal
            -> others

            Apply this identically to every filing with equivalent wording.

            TAG RULES

            - transfer:

            Use only when the transaction explicitly describes movement of EXISTING
            securities from one holder to another without an ordinary sale.

            Examples that support transfer:
            - "A transferred 100,000 shares to B"
            - "transfer of units from A to B"
            - gift of existing securities
            - inheritance of existing securities

            The securities must already exist as securities being moved from one holder
            to another.

            Do NOT use transfer merely because:
            - another entity ultimately receives newly issued securities;
            - a party nominates another entity to receive securities payable to it;
            - a party directs another entity to receive securities;
            - an entitlement to receive securities is redirected or assigned;
            - direct or deemed holdings change;
            - the recipient of newly issued securities differs from the party to whom
                they were originally payable.

            Example:

            "A is entitled to receive newly issued Units but nominates B to receive
            those Units"

            does NOT support the transfer tag because A did not first hold existing
            Units and then transfer those Units to B.

            - award:

            Explicit grant, award, vesting, restricted-share/unit plan, incentive plan,
            or other named securities-award mechanism.

            - warrant-exercise:

            Explicit exercise or conversion of warrants.

            Generic wording such as "conversion/exercise of rights, options, warrants
            or other convertibles" is insufficient unless warrants are specifically
            identified.

            - director-fees:

            Explicit payment or satisfaction of directors' fees using securities.

            - management-fees:

            Explicit payment or satisfaction of management, acquisition, or divestment
            fees using securities.

            - rights-offering:

            Explicit rights issue, rights offering, rights subscription, or exercise
            of rights.

            - option-exercise:

            Explicit exercise of share options granted to the holder, such as
            employee or director share options.

            Do not use it for an over-allotment option (greenshoe), a put or
            call option between investors, or any other option between market
            parties. Those are ordinary purchases or sales and get no
            mechanism tag.

            - married-deal:

            Use only when the transaction explicitly identifies a married deal.

            A generic off-market transaction is not sufficient.

            - internal-restructuring:

            Use only when the transaction explicitly describes an internal
            restructuring, group reorganisation, trust restructuring, or equivalent
            restructuring mechanism.

            Do not infer internal-restructuring merely because entities are related,
            parent/subsidiary, or within the same corporate group.

            - takeover

            Use when the SAME transaction explicitly involves a takeover, take-over offer,
            general offer, mandatory offer, voluntary offer, privatisation, or a scheme of
            arrangement whose stated purpose is the acquisition of existing shareholders'
            or unitholders' securities.

            Do not use takeover merely because:
            - takeover regulations or the Code on Take-overs and Mergers are mentioned;
            - a scheme of arrangement is mentioned;
            - ownership or control changes.

            A scheme of arrangement supports takeover only when its stated purpose is the
            acquisition of existing shareholders' or unitholders' securities.

            The takeover tag does not by itself determine transaction_type.

            Determine transaction_type from the explicitly described action of the
            CURRENT holder:

            - explicit acquisition/purchase by the CURRENT holder
            -> buy

            - explicit sale/disposal, tender, or acceptance of a takeover/general offer
            by the CURRENT holder
            -> sell

            - automatic, compulsory, or scheme-driven transfer resulting from an effective
            or binding takeover, privatisation, or acquisition scheme, without an explicit
            sale/disposal/tender/acceptance action by the CURRENT holder
            -> others
            
            - dividend-in-specie 

            Use when securities are explicitly received, distributed, transferred, or
            allotted as a dividend in specie or distribution in specie.

            This is a non-cash distribution mechanism and should not be classified as
            buy merely because the recipient's holdings increase.

            Do not use dividend-in-specie for:
            - scrip dividend schemes, where new securities are received in lieu of a
            cash dividend (use scrip-dividend);
            - ordinary cash dividends;
            - ordinary purchases of securities using dividend proceeds;
            - distributions where securities are not explicitly identified as being
            distributed in specie.

            - inheritance

            Use when securities are explicitly acquired, received, transmitted, or
            transferred as a result of inheritance, succession, estate distribution,
            or transmission upon death.

            Inheritance is a more specific mechanism than transfer.

            When the same securities movement is explicitly described as inheritance,
            use inheritance rather than adding transfer for that same movement.

            Do not infer inheritance merely because:
            - securities move between family members;
            - the holder is an executor, administrator, trustee, or beneficiary;
            - an estate is mentioned without explicitly linking the securities movement
            to inheritance or succession.

            - scrip-dividend

            Use when the holder receives new securities in lieu of a cash dividend or
            distribution under a scrip dividend scheme, dividend reinvestment plan, or
            distribution reinvestment plan.

            This is not a purchase, even when an issue price is stated.

            - bonus-issue

            Use when the change arises from a bonus issue by the listed issuer: new
            securities issued to existing holders for no consideration, in proportion
            to the securities they already hold, for example "one bonus share for
            every three shares held". It applies to the holder's own bonus securities
            and to those issued to parties through which the holder has a deemed
            interest.

            This is not a purchase, and not an award or part of remuneration. Do not
            use bonus-issue for a bonus paid to a director or employee in securities,
            for a share split or consolidation, or for a scrip dividend (use
            scrip-dividend).

            - share-buyback

            Use when the SAME transaction is explicitly a share buy-back, repurchase,
            or equal access scheme carried out by the listed issuer. This covers both
            a holder tendering securities into the buy-back and a holder whose
            percentage interest changes only because the issuer bought back or
            cancelled shares.

            Do not use share-buyback merely because a buy-back mandate is mentioned
            without being connected to the current transaction.

            SPECIFIC MECHANISM PRECEDENCE

            When an actual securities movement is explicitly described but the movement
            occurs as part of a more specific approved mechanism, use the more specific
            tag rather than adding transfer for the same action.

            Examples:

            "transferred Units to directors as payment of directors' fees"
            -> transaction_type = "others"
            -> tags = ["director-fees"]

            NOT:
            ["director-fees", "transfer"]

            "transferred Units pursuant to an employee award plan"
            -> transaction_type = "others"
            -> tags = ["award"]

            NOT:
            ["award", "transfer"]

            Important distinction:

            Nomination or direction of another party to receive newly issued securities
            is NOT a transfer in the first place.

            Example:

            "Units payable to A as management fees. A nominated B to receive the Units."

            -> transaction_type = "others"
            -> tags = ["management-fees"]

            NOT:
            ["management-fees", "transfer"]

            CLASSIFICATION COMBINATIONS

            - transfer               -> others
            - award                  -> others
            - warrant-exercise       -> others
            - director-fees          -> others
            - management-fees        -> others
            - internal-restructuring -> others
            - dividend-in-specie     -> others
            - inheritance            -> others
            - share-buyback          -> others
            - scrip-dividend         -> others
            - bonus-issue            -> others
            - rights-offering        -> buy
            - option-exercise        -> buy

            married-deal does not determine direction by itself:
            - explicit acquisition/purchase -> buy
            - explicit disposal/sale -> sell
            - no explicit direction -> others 

            takeover does not determine direction by itself:
            - explicit acquisition/purchase -> buy
            - explicit disposal/sale -> sell
            - no explicit direction -> others

            WHOSE ACTION IT IS

            A tag applies only when the CURRENT holder, or a party through which the
            CURRENT holder has a deemed interest, took part in that mechanism.

            When the CURRENT holder's percentage changed only because the listed
            issuer's number of securities in issue changed, the holder did not take
            part. Return tags = [] and transaction_type = "others". This covers:

            - other holders exercising or converting warrants, options, or
            convertibles;
            - a placement, share issue, or allotment the CURRENT holder did not
            subscribe for;
            - a rights issue the CURRENT holder did not take up;
            - a sale or cancellation of treasury shares.

            The only exception is share-buyback, which describes the issuer's own
            action and still applies.

            Ticked circumstance tells you which box the holder ticked on the form.
            When it is a corporate action by the listed issuer, treat the holder as
            not having taken part unless the wording explicitly says the CURRENT
            holder exercised, subscribed, tendered, or received the securities.

            Example:

            Ticked circumstance: "Corporate action by the Listed Issuer which
            Substantial Shareholder/Unitholder did not participate in"
            Circumstance description: "The increase in the total issued share capital
            following the exercise of warrants into new ordinary shares."

            -> transaction_type = "others"
            -> tags = []
            -> change_reason = "an increase in issued share capital following the
            exercise of warrants by other holders"

            NOT:
            tags = ["warrant-exercise"]

            MULTIPLE TAGS

            Multiple tags are allowed only when the SAME transaction explicitly contains
            genuinely independent approved mechanisms.

            Do not assign multiple tags merely because several procedural steps are
            described.

            Do not assign a generic transfer tag to a movement already represented by a
            more specific approved mechanism.

            EVIDENCE

            classification_evidence must be the shortest exact contiguous wording from
            Circumstance description or Remarks that directly supports transaction_type
            and tags.

            - Do not paraphrase.
            - Do not summarize.
            - Do not insert "...".
            - Do not combine separate non-contiguous passages.
            - Do not use numeric holding changes as evidence.

            REASONING

            classification_reasoning must briefly explain:
            - what explicit transaction mechanism is described;
            - why it maps to transaction_type;
            - why each returned tag applies.

            It may mention other procedural steps from the same transaction when needed
            to explain why they do or do not satisfy an approved tag.

            Do not introduce facts absent from Circumstance description or Remarks.

            If classification is unsupported:
            - transaction_type: null
            - tags: []

            Transaction window:
            {window}

            Circumstance description:
            {circumstances_desc}

            Remarks:
            {remarks}

            Circumstances giving rise to deemed interests:
            {deemed_interest_circumstances}

            Ticked circumstance:
            {ticked_circumstance}

            Holder Name:
            {holder_name}

            Return the data using this JSON schema:
            {format_instructions}
        """
