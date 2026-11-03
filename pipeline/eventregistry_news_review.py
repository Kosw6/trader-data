"""Conservative local review hints; neither relevance nor article truth is proven."""
import re

ALIASES = {'recent_NVDA': ['Nvidia', 'NVIDIA', 'NVDA'],
           'recent_AAPL': ['Apple', 'AAPL'], 'recent_TSLA': ['Tesla', 'TSLA']}


def review_article(article):
    cases = article.get('observedCases', [])
    title, excerpt = article['title'], article.get('excerpt', '')
    flags = []
    associations = []
    for case in cases:
        if case not in ALIASES: continue
        title_match = any(re.search(r'(?<!\w)' + re.escape(a) + r'(?!\w)', title, re.I)
                          for a in ALIASES[case])
        associations.append(dict(case=case, titleMention=title_match,
             classification='TITLE_MENTION_CANDIDATE' if title_match else 'PROVIDER_ASSOCIATION_ONLY'))
    if re.search(r'\btokeni[sz]ed\b|\bBinance Futures\b', title + '\n' + excerpt, re.I):
        flags.append('TOKENIZED_PRODUCT_DO_NOT_TREAT_AS_CASH_EQUITY_PRICE')
    if re.search(r'price prediction|price target|\ba buy\b|analysts|scenario|could|likely', title, re.I):
        flags.append('FORECAST_OR_COMMENTARY_CANDIDATE_NOT_OFFICIAL_EVENT')
    year = int(article['date'][:4])
    old_years = [int(x) for x in re.findall(r'\b(?:19|20)\d{2}\b', title) if int(x) < year - 1]
    if old_years:
        flags.append('OLDER_YEAR_IN_TITLE_POSSIBLE_REPUBLICATION_EVENT_DATE_UNCONFIRMED')
    return dict(articleUri=article['uri'], companyAssociations=associations, flags=flags,
                providerPublicationDate=article['date'], eventDate=None,
                articleTruthVerified=False, automaticTradingUseApproved=False)
