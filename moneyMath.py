import math


def precisionRound(num, precision):
    """A handy way to round a decimal number to any desired number of
    decimal places. Negative precision allows rounding on the
    left side of the decimal place.
    Eg 1. precisionRound(123.456, 2) -> 123.46
    Eg 2. precisionRound(123.456, -1) -> 120
    num -- A floating point number.
    precision -- An integer representing number of decimal places round to.
    """
    factor = pow(10, precision)
    return round(num * factor) / factor


def correct(amt):
    """Whenever mathematical operations such as addition or subtraction
    are preformed on money, the result should be rounded with this
    operation to eliminate any floating point error accumulation.
    General note: For the purposes of this program, rounding occasionally, and
    especially to do comparisons, is sufficient.
    amt -- Decimal amount in dollars and cents. Eg 1.25
    """
    return precisionRound(amt, 2)


def isEqual(a, b):
    """Safely compare monetary quantities; returns true if a == b.
    a -- First amount in dollars and cents.
    b -- Second amount in dollars and cents.
    """
    return correct(a) == correct(b)


def isGt(c, d):
    """Safely compare monetary quantities.
     c -- First amount in dollars and cents.
     d -- Second amount in dollars and cents.
     """
    return correct(c) > correct(d)


def isGtEqual(e, f):
    """Safely compare monetary quantities.
    e -- First amount in dollars and cents.
    f -- Second amount in dollars and cents.
    """
    return correct(e) >= correct(f)


def isLt(g, h):
    """Safely compare monetary quantities.
     g -- First amount in dollars and cents.
     h -- Second amount in dollars and cents.
     """
    return correct(g) < correct(h)


def isLtEqual(i, j):
    """Safely compare monetary quantities.
     i -- First amount in dollars and cents.
     j -- Second amount in dollars and cents.
     """
    return correct(i) <= correct(j)


def getBal(ta_list):
    """Sum up list of all transactions to get current balance.
    fine -- List of transactions
    returns sum of all transactions"""
    initial_value = 0
    for ta in ta_list:
        initial_value += ta["amt"]
    return correct(initial_value)


def getSumPayments(fine_list):
    """Sum of all payment transactions. The following relation holds:
    getSumPayments(fine) + maxCreditToApply(fine) = getBal(fine)
    returns sum of all payment transactions
    """
    filtered_list = [fine for fine in fine_list if fine["type"] == "Payment"]
    return getBal(filtered_list)


def maxCreditToApply(fine_list):
    """Given a list of transactions, the current balance (bal) is the sum of
    the transactions. If bal > 0 then patron owes that amount. If bal < 0,
    should receive back that amount. The maximum amount of credit that can be
    applied (in absolute value, but note that credits, waives and payments are
    always recorded as negative quantities) is then: bal - sum of payments
    (payment amounts being negative). This is equivalent to sum of fines + sum
    of waives + sum of credits. Notice that payments have no effect on the
    amount of credit that can be returned. These seems counterintuative at
    first. However, note that the past payments *do* affect the final balance
    that can show on an account. In fact, the maximum balance by magnitude
    (ignore the sign) in favor of a patron is the sum of past payments made.
    Ideally, once we know that a patron has received their money owed to them,
    we would reflect this as a transaction bringing the final balance back to
    zero.
    returns maximum possible credit that can be applied to fine. Returned as
    a non-negative number.
    """
    filtered_list = [fine for fine in fine_list if fine["type"] != "Payment"]
    return getBal(filtered_list)


def findCreditMatch(creditAmt, fine_list):
    """If one of the transaction lists has a maxCreditToApply() amount equal
    to proposed credit amount (creditAmt), return that index, otherwise
    return -1. If there are multiple matches, the smallest index of the
    matching fines is returned.
    creditAmt --- Amount to be credited, expressed as a positive value.
    returns Index of fine with exact matching max-credit, if exits.
    Otherwise, returns -1.
    """
    found_it = False

    for fine in fine_list:
        index = fine_list.index(fine)
        if isEqual(creditAmt, maxCreditToApply(fine_list[index])):
            found_index = index
            found_it = True
            break
        else:
            pass

    if found_it:
        return found_index
    else:
        return -1


def sortedMaxCredit(fine_list):
    """Return array of maximum credit amounts and corresponding fine index,
    sorted in decending order by maximum credit. The sort is stable, meaning
    that items with equal values will not have their original
    relative positions changed.
    Returns reverse-sorted array of indicies and max-credits.
    """
    creds = []

    for fine in fine_list:
        index = fine_list.index(fine)
        maxCred = maxCreditToApply(fine_list[index])
        creds.append({"ind": index, "maxCred": maxCred})

    sorted_creds = sorted(creds, key=lambda k: k["maxCred"], reverse=True)
    return sorted_creds


def distributeCreditAlma(credit, list_of_fine_lists):
    """Apply credit to an array of related fines. In the case of multiple
    fines, the algorithm first attempts to apply the credit to the first
    fine that has an exact max-credit amount. If there is no exact match,
    then apply maximum credit possible to each fine according to max-credit
    reverse sorted order. Any remaining credit is returned in the remCred
    field. If this amount is > 0, this generally indicates a problem.
    credit -- total amount of credit to account to be possible
    fines -- an array of arrays of transactions.
    returns: remaining credit, individual credit amounts applied, and new
    balances.
    """
    r = {
        "remCred": credit,
        "creds": [0 for fine_list in list_of_fine_lists],
        "newAmts": [getBal(fine_list) for fine_list in list_of_fine_lists],
    }

    if isLtEqual(r["remCred"], 0):
        return r
    else:
        i = findCreditMatch(r["remCred"], list_of_fine_lists)
        if i >= 0:
            credToApply = r["remCred"]
            r["newAmts"][i] = correct(r["newAmts"][i] - credToApply)
            r["creds"][i] = credToApply
            r["remCred"] = 0
            return r
        else:
            maxCreds = sortedMaxCredit(list_of_fine_lists)
            for mc in maxCreds:
                index = maxCreds.index(mc)

                if isGtEqual(r["remCred"], mc["maxCred"]):
                    credToApply = mc["maxCred"]
                else:
                    credToApply = r["remCred"]

                r["newAmts"][i] = correct(r["newAmts"][i] - credToApply)
                r["creds"][i] = credToApply
                r["remCred"] = correct(r["remCred"] - credToApply)

                if isEqual(r["remCred"], 0):
                    return r
                else:
                    pass

    return r


def distributeCreditBurs(credit, targets, allowNegAmts):
    """Distribute credit (or waive or payment) across one or more accounts.
    In the case of payments or waive, it should only be possible to pay up
    to the original amount owing. The algorithm first attempts to apply the
    credit to hit zero meaning it will only recredit the amount already paid.
    We will apply any remaining credit up until origAmt for each account, only
    if allowNegAmts is true. Will return unused credit if everything can not
    be allocated properly. If remCred == 0, then safe to assume success. We
    assume origAmt >= 0. The original credit amount is divided up into the
    output creds array. The newAmts indicate the new total after the credit
    is applied.  When there are exactly two accounts, we will first attempt
    to match up the credit to the account where the curAmt == credit,
    otherwise we will apply credit to first account first.  If credit <= 0
    no credit applied.
    credit -- Amount to credit back, must be a positive number.
    targets -- pairs of curAmt for current balance and origAmt for original
    fine.
    allowNegAmts -- are negative newTargAmts allowed (up to -origAmt)
    returns - remCred: unapplied credit, creds: array of amount cred to each
    account newAmts: array of new balances in each account.
    """
    reverse2 = False

    if len(targets) == 2 and isEqual(credit, targets[1]["curAmt"]):
        # For Sierra fines with two parts, this is a trick to try to match up
        # a waive/credit to the matching part of the fine, if there is an
        # exact match.
        reverse2 = True

    r = {
        "remCred": credit,
        "creds": [0 for target in targets],
        "newAmts": [target["curAmt"] for target in targets],
    }

    if isLtEqual(credit, 0):
        return r
    else:
        if reverse2:
            (r["creds"]).reverse()
            (r["newAmts"]).reverse()

        # First round: bring newAmts down to no more than zero

        for newAmt in r["newAmts"]:
            index = (r["newAmts"]).index(newAmt)
            if isLtEqual(newAmt, 0):
                pass
            else:
                applied = min(r["remCred"], r["newAmts"][index])
                r["creds"][index] = correct(r["creds"][index] + applied)
                r["newAmts"][index] = correct(r["newAmts"][index] - applied)
                r["remCred"] = correct(r["remCred"] - applied)

        if isLtEqual(r["remCred"], 0) or allowNegAmts is False:
            if reverse2:
                (r["creds"]).reverse()
                (r["newAmts"]).reverse()

            return r
        else:
            for newAmt in r["newAmts"]:
                index = (r["newAmts"]).index(newAmt)
                maxRoom = abs(-targets[index]["origAmt"] - r["newAmts"][index])
                applied = min(r["remCred"], maxRoom)
                r["creds"][index] = correct(r["creds"][index] + applied)
                r["newAmts"][index] = correct(r["newAmts"][index] - applied)
                r["remCred"] = correct(r["remCred"] - applied)
            if reverse2:
                r["creds"].reverse()
                r["newAmts"].reverse()
            return r


def ignoreFine(fineId):
    # Old Sierra Fines that don't exist in PeopleSoft.
    # MM: Waived as of July 12, 2017
    FINE_IGNORE = {
        "7813845970003841": True,
        "7813845980003841": True,
        "7813845990003841": True,
        "7813846000003841": True,
        "7813846010003841": True,
        "7813846020003841": True,
        "7813846030003841": True,
        "7813846040003841": True,
        "7813846050003841": True,
        "7813846060003841": True,
        "7821584080003841": True,
        "7817489310003841": True,
        "7820324530003841": True,
        "7813887710003841": True,
        "7821159330003841": True,
        "7822876260003841": True,
        "7814676920003841": True,
        "7814676930003841": True,
        "7814676940003841": True,
        "7814676950003841": True,
        "7814676960003841": True,
        "7814676970003841": True,
        "7808482780003841": True,
        "7809606070003841": True,
        "7809606080003841": True,
        "7816634180003841": True,
        "7815016460003841": True,
        "7810474420003841": True,
        # Old Sierra fines associated with negative initial amounts / credits
        # Most of these have counterparts on the list above.  After these are
        # ignored, it will be saved to waive all fines associated with the
        # negative amount fine.
        # MM: Done on July 12, 2017
        "7821159340003841": True,
        "7820324540003841": True,
        "7820324550003841": True,
        "7818019430003841": True,
        # The following four fines have non-standard Sierra reference numbers such
        # as _09, or a reversed _01 _03.  They will soon be manually waived
        # from PeopleSoft and manually waived in Alma.
        "7818705300003841": True,
        "7816272750003841": True,
        "7810888120003841": True,
        "7810293390003841": True,
        # This is for a fine that had a payment erroneously applied.  As of
        # Sept 20, 2017, we decided to just waive the original fine.
        "9261854950003841": True,
        # Another fine with a payment erroneously applied.  Just ignoring
        # as of Oct 23, 2017
        "9491097110003841": True,
        # select * from fine where "rootAlmaFineId" = '9491097110003841';
        # Jan 21, 2018.  There are 6 new transactions that cause a big problem
        # during the apply new payments method: somehow the root fine is 'new'
        # and hasn't been sent to the Bursar yet which should be impossible.
        # This turned out be caused by human error.
        "10376863930003841": True,
        "10382817500003841": True,
        "10372893110003841": True,
        "10389990880003841": True,
        "10382815720003841": True,
        "10382816690003841": True,
    }

    try:
        return FINE_IGNORE[fineId]
    except:
        return False


# After filtering out the above, we get 3109 initial fines imported into Alma.
# Removing imported Sierra fine that is gone from PeopleSoft.
#
# almaPatronId |    almaFineId    | remAmtAlma
# --------------+------------------+------------
# 1204417898   | 7821159340003841 |        -40
# 1200690339   | 7820324540003841 |         -4
# 1200690339   | 7820324550003841 |        -25
# 1204836733   | 7818019430003841 |      -21.4

# must waive all fines associated with users:
# select "almaPatronId", "almaFineId", "remAmtAlma" from fine
#   where "almaPatronId" IN ('1204417898', '1200690339', '1204836733' );
#
# let eg = {
#   "original_charge": "150",
#   "balance": "0",
#   "strm": "2171",
#   "is_reversed": true,
#   "item": {
#     "ref1_descr": "359952_bQ1460519_01",
#     "session_code": "-",
#     "stdnt_car_nbr": "0",
#     "rate_mult": "1",
#     "tax_cd": "-",
#     "emplid": "1201901471",
#     "cur_rt_type": "OFFIC",
#     "item_balance": "0",
#     "descr": "Tempe Library Book",
#     "sa_id_type": "P",
#     "contract_num": "-",
#     "refund_nbr": "0",
#     "item_nbr_source": "-",
#     "item_amt": "0",
#     "business_unit": "ASU00",
#     "account_term": "2171",
#     "common_id": "1201901471",
#     "tax_authority_cd": "-",
#     "contract_emplid": "-",
#     "receipt_nbr": "0",
#     "sel_group": "-",
#     "item_effective_dt": "2017-05-24",
#     "account_nbr": "ACCOUNT001",
#     "last_activity_date": "2017-05-24",
#     "acad_year": "2017",
#     "item_type": "531000000001",
#     "item_term": "2171",
#     "fee_cd": "-",
#     "refund_ext_org_id": "-",
#     "applied_amt": "0",
#     "currency_cd": "USD",
#     "adm_appl_nbr": "00229969",
#     "orignl_currency_cd": "USD",
#     "rate_div": "1",
#     "encumbered_amt": "0",
#     "item_nbr": "000000000000501",
#     "item_type_cd": "C",
#     "payment_id_nbr": "0",
#     "acad_career": "-",
#     "orignl_item_amt": "150",
#     "ext_org_id": "-",
#     "refund_emplid": "-",
#     "item_status": "A",
#     "class_nbr": "0"
#   },
#   "transactions": [],
#   "current_charge": "0"
# }
