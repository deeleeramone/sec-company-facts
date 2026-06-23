from __future__ import annotations

import time
from typing import Any

from sec_app.db.cache import dolt_cached
from sec_app.db.query import _q, _rows, _session

_SIC_CODE_SUBQ = "(SELECT tag_id FROM xbrl_tags WHERE namespace='dei' AND tag='EntitySicCode')"

_SIC_GICS: dict[str, tuple[str, str]] = {
    "12": ("Energy", "Coal Mining"),
    "122": ("Energy", "Bituminous Coal & Lignite Mining"),
    "13": ("Energy", "Oil & Gas Extraction"),
    "131": ("Energy", "Crude Petroleum & Natural Gas"),
    "138": ("Energy", "Oil & Gas Field Services"),
    "29": ("Energy", "Petroleum Refining"),
    "291": ("Energy", "Petroleum Refining"),
    "299": ("Energy", "Miscellaneous Petroleum & Coal Products"),
    "46": ("Energy", "Pipelines"),
    "461": ("Energy", "Pipelines"),
    "517": ("Energy", "Petroleum & Petroleum Products"),
    "08": ("Materials", "Forestry"),
    "10": ("Materials", "Metal Mining"),
    "104": ("Materials", "Gold & Silver Mining"),
    "109": ("Materials", "Miscellaneous Metal Ore Mining"),
    "14": ("Materials", "Nonmetallic Minerals Mining"),
    "24": ("Materials", "Lumber & Wood Products"),
    "242": ("Materials", "Sawmills & Planing Mills"),
    "243": ("Materials", "Millwork, Veneer & Plywood"),
    "245": ("Materials", "Wood Buildings & Mobile Homes"),
    "26": ("Materials", "Paper & Allied Products"),
    "261": ("Materials", "Pulp Mills"),
    "262": ("Materials", "Paper Mills"),
    "263": ("Materials", "Paperboard Mills"),
    "265": ("Materials", "Paperboard Containers & Boxes"),
    "267": ("Materials", "Converted Paper Products"),
    "27": ("Industrials", "Commercial Printing"),
    "271": ("Communication Services", "Publishing - Newspapers"),
    "272": ("Communication Services", "Publishing - Periodicals"),
    "273": ("Communication Services", "Publishing - Books"),
    "274": ("Communication Services", "Publishing - Miscellaneous"),
    "275": ("Industrials", "Commercial Printing"),
    "276": ("Industrials", "Manifold Business Forms"),
    "277": ("Industrials", "Greeting Cards"),
    "278": ("Industrials", "Bookbinding"),
    "279": ("Industrials", "Printing Trade Services"),
    "281": ("Materials", "Industrial Inorganic Chemicals"),
    "2810": ("Materials", "Industrial Inorganic Chemicals"),
    "282": ("Materials", "Plastics Materials & Synthetic Resins"),
    "2820": ("Materials", "Plastics Materials & Synthetic Resins"),
    "2821": ("Materials", "Plastics Materials & Synthetic Resins"),
    "283": ("Health Care", "Drugs"),
    "2833": ("Health Care", "Drugs"),
    "2834": ("Health Care", "Drugs"),
    "2835": ("Health Care", "Drugs"),
    "2836": ("Health Care", "Drugs"),
    "284": ("Consumer Staples", "Soap, Cleaners & Cosmetics"),
    "2840": ("Consumer Staples", "Soap, Cleaners & Cosmetics"),
    "2842": ("Consumer Staples", "Soap, Cleaners & Cosmetics"),
    "2844": ("Consumer Staples", "Soap, Cleaners & Cosmetics"),
    "285": ("Materials", "Paints & Coatings"),
    "2851": ("Materials", "Paints & Coatings"),
    "286": ("Materials", "Industrial Organic Chemicals"),
    "2860": ("Materials", "Industrial Organic Chemicals"),
    "287": ("Materials", "Agricultural Chemicals"),
    "2870": ("Materials", "Agricultural Chemicals"),
    "289": ("Materials", "Miscellaneous Chemical Products"),
    "2890": ("Materials", "Miscellaneous Chemical Products"),
    "2891": ("Materials", "Miscellaneous Chemical Products"),
    "30": ("Materials", "Rubber & Plastics Products"),
    "301": ("Consumer Discretionary", "Tires & Rubber"),
    "302": ("Materials", "Rubber & Plastics Footwear"),
    "305": ("Materials", "Gaskets, Packing & Sealing Devices"),
    "306": ("Materials", "Fabricated Rubber Products"),
    "308": ("Materials", "Miscellaneous Plastics Products"),
    "32": ("Materials", "Stone, Clay & Glass Products"),
    "321": ("Materials", "Flat Glass"),
    "322": ("Materials", "Glass & Glassware"),
    "323": ("Materials", "Glass Products"),
    "324": ("Materials", "Cement, Hydraulic"),
    "326": ("Materials", "Pottery & Related Products"),
    "327": ("Materials", "Concrete, Gypsum & Plaster"),
    "328": ("Materials", "Cut Stone & Stone Products"),
    "329": ("Materials", "Abrasive & Asbestos Products"),
    "33": ("Materials", "Primary Metal Industries"),
    "331": ("Materials", "Steel Works & Rolling Mills"),
    "332": ("Materials", "Iron & Steel Foundries"),
    "333": ("Materials", "Primary Smelting of Nonferrous Metals"),
    "334": ("Materials", "Secondary Smelting of Nonferrous Metals"),
    "335": ("Materials", "Rolling & Drawing of Nonferrous Metals"),
    "336": ("Materials", "Nonferrous Foundries"),
    "339": ("Materials", "Miscellaneous Primary Metal Products"),
    "503": ("Materials", "Lumber & Construction Materials Wholesale"),
    "505": ("Materials", "Metals & Minerals Wholesale"),
    "511": ("Materials", "Paper & Paper Products Wholesale"),
    "516": ("Materials", "Chemicals Wholesale"),
    "52": ("Consumer Discretionary", "Building Materials & Garden Retail"),
    "521": ("Consumer Discretionary", "Lumber & Building Materials Dealers"),
    "07": ("Industrials", "Agricultural Services"),
    "15": ("Industrials", "Building Construction"),
    "152": ("Industrials", "Residential Building Construction"),
    "153": ("Industrials", "Operative Builders"),
    "154": ("Industrials", "Nonresidential Building Construction"),
    "16": ("Industrials", "Heavy Construction"),
    "162": ("Industrials", "Heavy Construction, Except Highway"),
    "17": ("Industrials", "Construction Special Trade Contractors"),
    "173": ("Industrials", "Electrical Work"),
    "351": ("Industrials", "Engines & Turbines"),
    "352": ("Industrials", "Farm & Garden Machinery"),
    "353": ("Industrials", "Construction & Materials Handling Machinery"),
    "354": ("Industrials", "Metalworking Machinery"),
    "355": ("Industrials", "Special Industry Machinery"),
    "356": ("Industrials", "General Industrial Machinery"),
    "358": ("Industrials", "Refrigeration & Service Machinery"),
    "359": ("Industrials", "Miscellaneous Industrial Machinery"),
    "361": ("Industrials", "Electric Transmission & Distribution Equipment"),
    "362": ("Industrials", "Electrical Industrial Apparatus"),
    "364": ("Industrials", "Electric Lighting & Wiring Equipment"),
    "369": ("Industrials", "Miscellaneous Electrical Machinery"),
    "372": ("Industrials", "Aircraft & Parts"),
    "376": ("Industrials", "Guided Missiles & Space Vehicles"),
    "379": ("Industrials", "Miscellaneous Transportation Equipment"),
    "381": ("Industrials", "Navigation & Guidance Instruments"),
    "382": ("Industrials", "Laboratory & Analytical Instruments"),
    "40": ("Industrials", "Railroad Transportation"),
    "401": ("Industrials", "Railroads"),
    "41": ("Industrials", "Local & Suburban Transit"),
    "42": ("Industrials", "Trucking & Warehousing"),
    "421": ("Industrials", "Trucking & Courier Services"),
    "422": ("Industrials", "Public Warehousing & Storage"),
    "44": ("Industrials", "Water Transportation"),
    "441": ("Industrials", "Deep Sea Freight Transportation"),
    "45": ("Industrials", "Air Transportation"),
    "451": ("Industrials", "Air Transportation, Scheduled"),
    "452": ("Industrials", "Air Transportation, Nonscheduled"),
    "458": ("Industrials", "Airports & Terminal Services"),
    "47": ("Industrials", "Transportation Services"),
    "473": ("Industrials", "Freight Transportation Arrangement"),
    "504": ("Industrials", "Professional & Commercial Equipment Wholesale"),
    "506": ("Industrials", "Electrical Goods Wholesale"),
    "507": ("Industrials", "Hardware & Plumbing Wholesale"),
    "508": ("Industrials", "Machinery & Equipment Wholesale"),
    "509": ("Industrials", "Miscellaneous Durable Goods Wholesale"),
    "731": ("Communication Services", "Advertising"),
    "732": ("Industrials", "Consumer Credit Reporting"),
    "733": ("Industrials", "Mailing, Reproduction & Commercial Art"),
    "87": ("Industrials", "Engineering, Accounting & Management Services"),
    "871": ("Industrials", "Engineering & Architectural Services"),
    "873": ("Industrials", "Research, Development & Testing Services"),
    "874": ("Industrials", "Management & Public Relations Services"),
    "22": ("Consumer Discretionary", "Textile Mill Products"),
    "221": ("Consumer Discretionary", "Broadwoven Fabric Mills, Cotton"),
    "222": ("Consumer Discretionary", "Broadwoven Fabric Mills, Manmade Fiber"),
    "225": ("Consumer Discretionary", "Knitting Mills"),
    "227": ("Consumer Discretionary", "Carpets & Rugs"),
    "25": ("Consumer Discretionary", "Furniture & Fixtures"),
    "251": ("Consumer Discretionary", "Household Furniture"),
    "252": ("Consumer Discretionary", "Office Furniture"),
    "253": ("Consumer Discretionary", "Public Building Furniture"),
    "254": ("Consumer Discretionary", "Partitions & Office Fixtures"),
    "259": ("Consumer Discretionary", "Miscellaneous Furniture & Fixtures"),
    "314": ("Consumer Discretionary", "Footwear, Except Rubber"),
    "34": ("Consumer Discretionary", "Fabricated Metal Products"),
    "341": ("Consumer Discretionary", "Metal Cans & Shipping Containers"),
    "342": ("Consumer Discretionary", "Cutlery, Handtools & Hardware"),
    "343": ("Consumer Discretionary", "Heating Equipment & Plumbing Fixtures"),
    "344": ("Consumer Discretionary", "Fabricated Structural Metal Products"),
    "345": ("Consumer Discretionary", "Screw Machine Products & Fasteners"),
    "346": ("Consumer Discretionary", "Metal Forgings & Stampings"),
    "347": ("Consumer Discretionary", "Coating, Engraving & Allied Services"),
    "348": ("Consumer Discretionary", "Ordnance & Accessories"),
    "349": ("Consumer Discretionary", "Miscellaneous Fabricated Metal Products"),
    "363": ("Consumer Discretionary", "Household Appliances"),
    "365": ("Consumer Discretionary", "Household Audio & Video Equipment"),
    "371": ("Consumer Discretionary", "Motor Vehicles & Equipment"),
    "373": ("Industrials", "Ship & Boat Building"),
    "374": ("Industrials", "Railroad Equipment"),
    "375": ("Consumer Discretionary", "Motorcycles, Bicycles & Parts"),
    "501": ("Consumer Discretionary", "Motor Vehicles & Parts Wholesale"),
    "502": ("Consumer Discretionary", "Furniture & Home Furnishings Wholesale"),
    "553": ("Consumer Discretionary", "Auto & Home Supply Stores"),
    "562": ("Consumer Discretionary", "Women's Clothing Stores"),
    "565": ("Consumer Discretionary", "Family Clothing Stores"),
    "566": ("Consumer Discretionary", "Shoe Stores"),
    "571": ("Consumer Discretionary", "Home Furnishings Stores"),
    "70": ("Consumer Discretionary", "Hotels & Lodging"),
    "701": ("Consumer Discretionary", "Hotels & Motels"),
    "751": ("Consumer Discretionary", "Automotive Rental & Leasing"),
    "781": ("Communication Services", "Movies & Entertainment"),
    "782": ("Communication Services", "Movies & Entertainment"),
    "783": ("Communication Services", "Movies & Entertainment"),
    "784": ("Communication Services", "Movies & Entertainment"),
    "794": ("Communication Services", "Sports & Entertainment"),
    "799": ("Consumer Discretionary", "Amusement & Recreation Services"),
    "811": ("Consumer Discretionary", "Legal Services"),
    "82": ("Consumer Discretionary", "Educational Services"),
    "01": ("Consumer Staples", "Agricultural Production - Crops"),
    "02": ("Consumer Staples", "Agricultural Production - Livestock"),
    "09": ("Consumer Staples", "Fishing, Hunting & Trapping"),
    "20": ("Consumer Staples", "Food & Kindred Products"),
    "201": ("Consumer Staples", "Meat Products"),
    "202": ("Consumer Staples", "Dairy Products"),
    "203": ("Consumer Staples", "Canned & Preserved Fruits & Vegetables"),
    "204": ("Consumer Staples", "Grain Mill Products"),
    "205": ("Consumer Staples", "Bakery Products"),
    "206": ("Consumer Staples", "Sugar & Confectionery Products"),
    "207": ("Consumer Staples", "Fats & Oils"),
    "208": ("Consumer Staples", "Beverages"),
    "209": ("Consumer Staples", "Miscellaneous Food Preparations"),
    "21": ("Consumer Staples", "Tobacco Products"),
    "211": ("Consumer Staples", "Cigarettes"),
    "23": ("Consumer Discretionary", "Apparel, Accessories & Luxury Goods"),
    "232": ("Consumer Discretionary", "Apparel - Men's & Boys'"),
    "233": ("Consumer Discretionary", "Apparel - Women's & Misses'"),
    "234": ("Consumer Discretionary", "Apparel - Women's & Children's"),
    "239": ("Consumer Discretionary", "Textile Products"),
    "513": ("Consumer Staples", "Apparel & Piece Goods Wholesale"),
    "514": ("Consumer Staples", "Groceries & Related Products Wholesale"),
    "515": ("Consumer Staples", "Farm-Product Raw Materials Wholesale"),
    "518": ("Consumer Staples", "Beer, Wine & Distilled Beverages Wholesale"),
    "519": ("Consumer Staples", "Miscellaneous Nondurable Goods Wholesale"),
    "53": ("Consumer Staples", "General Merchandise Stores"),
    "531": ("Consumer Staples", "Department Stores"),
    "533": ("Consumer Staples", "Variety Stores"),
    "539": ("Consumer Staples", "Miscellaneous General Merchandise Stores"),
    "54": ("Consumer Staples", "Food Stores"),
    "541": ("Consumer Staples", "Grocery Stores"),
    "283": ("Health Care", "Drugs"),
    "384": ("Health Care", "Surgical & Medical Instruments"),
    "385": ("Health Care", "Ophthalmic Goods"),
    "512": ("Health Care", "Drugs & Druggists' Sundries Wholesale"),
    "80": ("Health Care", "Health Services"),
    "801": ("Health Care", "Offices & Clinics of Medical Doctors"),
    "805": ("Health Care", "Nursing & Personal Care Facilities"),
    "806": ("Health Care", "Hospitals"),
    "807": ("Health Care", "Medical & Dental Laboratories"),
    "808": ("Health Care", "Home Health Care Services"),
    "809": ("Health Care", "Miscellaneous Health Services"),
    "60": ("Financials", "Depository Institutions"),
    "602": ("Financials", "Commercial Banks"),
    "603": ("Financials", "Savings Institutions"),
    "609": ("Financials", "Functions Related to Depository Banking"),
    "61": ("Financials", "Nondepository Credit Institutions"),
    "611": ("Financials", "Federal Credit Agencies"),
    "614": ("Financials", "Personal Credit Institutions"),
    "615": ("Financials", "Business Credit Institutions"),
    "616": ("Financials", "Mortgage Bankers & Brokers"),
    "618": ("Financials", "Asset-Backed Securities"),
    "62": ("Financials", "Security & Commodity Brokers"),
    "621": ("Financials", "Security Brokers & Dealers"),
    "622": ("Financials", "Commodity Contracts Brokers & Dealers"),
    "628": ("Financials", "Services Allied with Securities Exchange"),
    "63": ("Financials", "Insurance Carriers"),
    "631": ("Financials", "Life Insurance"),
    "632": ("Financials", "Accident & Health Insurance"),
    "633": ("Financials", "Fire, Marine & Casualty Insurance"),
    "635": ("Financials", "Surety Insurance"),
    "636": ("Financials", "Title Insurance"),
    "639": ("Financials", "Insurance Carriers, NEC"),
    "64": ("Financials", "Insurance Agents & Brokers"),
    "641": ("Financials", "Insurance Agents & Brokers"),
    "67": ("Financials", "Holding & Other Investment Offices"),
    "677": ("Financials", "Blank Checks"),
    "679": ("Financials", "Miscellaneous Investing"),
    "357": ("Information Technology", "Technology Hardware, Storage & Peripherals"),
    "367": ("Information Technology", "Electronic Equipment, Instruments & Components"),
    "366": ("Information Technology", "Communications Equipment"),
    "48": ("Communication Services", "Communications"),
    "481": ("Communication Services", "Telephone Communications"),
    "482": ("Communication Services", "Telegraph & Message Communications"),
    "483": ("Communication Services", "Radio & TV Broadcasting"),
    "484": ("Communication Services", "Cable & Pay Television"),
    "489": ("Communication Services", "Communications Services, NEC"),
    "49": ("Utilities", "Electric, Gas & Sanitary Services"),
    "491": ("Utilities", "Electric Services"),
    "492": ("Utilities", "Gas Production & Distribution"),
    "493": ("Utilities", "Combination Utility Services"),
    "494": ("Utilities", "Water Supply"),
    "495": ("Utilities", "Sanitary Services"),
    "65": ("Real Estate", "Real Estate"),
    "651": ("Real Estate", "Real Estate Operators & Lessors"),
    "653": ("Real Estate", "Real Estate Agents & Managers"),
    "655": ("Real Estate", "Land Subdividers & Developers"),
    "737": ("Information Technology", "IT Services"),
    "386": ("Information Technology", "Electronic Equipment, Instruments & Components"),
    "591": ("Consumer Staples", "Drug Stores"),
    "284": ("Consumer Staples", "Soap, Cleaners & Cosmetics"),
    "596": ("Consumer Discretionary", "Nonstore Retailers"),
    "835": ("Consumer Discretionary", "Child Day Care Services"),
    "387": ("Consumer Discretionary", "Watches, Clocks & Jewelry"),
    "836": ("Health Care", "Residential Care"),
    "399": ("Materials", "Miscellaneous Manufacturing"),
    "28": ("Materials", "Chemicals & Allied Products"),
    "31": ("Consumer Discretionary", "Leather & Leather Products"),
    "35": ("Industrials", "Industrial & Commercial Machinery"),
    "36": ("Industrials", "Electronic & Electrical Equipment"),
    "37": ("Consumer Discretionary", "Transportation Equipment"),
    "38": ("Industrials", "Measuring & Analyzing Instruments"),
    "39": ("Consumer Discretionary", "Miscellaneous Manufacturing"),
    "50": ("Industrials", "Durable Goods Wholesale"),
    "51": ("Consumer Staples", "Nondurable Goods Wholesale"),
    "55": ("Consumer Discretionary", "Automotive Dealers"),
    "56": ("Consumer Discretionary", "Apparel & Accessory Stores"),
    "57": ("Consumer Discretionary", "Home Furnishings & Electronics Stores"),
    "58": ("Consumer Discretionary", "Eating & Drinking Places"),
    "59": ("Consumer Discretionary", "Miscellaneous Retail"),
    "72": ("Consumer Discretionary", "Personal Services"),
    "73": ("Industrials", "Business Services"),
    "75": ("Consumer Discretionary", "Automotive Services"),
    "76": ("Industrials", "Miscellaneous Repair Services"),
    "79": ("Consumer Discretionary", "Amusement & Recreation Services"),
    "83": ("Health Care", "Social Services"),
    "86": ("Industrials", "Membership Organizations"),
    "89": ("Industrials", "Services, NEC"),
    "7370": ("Information Technology", "IT Services"),
    "7371": ("Information Technology", "IT Services"),
    "7372": ("Information Technology", "Software"),
    "7373": ("Information Technology", "IT Services"),
    "7374": ("Information Technology", "IT Services"),
    "7375": ("Information Technology", "IT Services"),
    "7377": ("Information Technology", "IT Services"),
    "7379": ("Information Technology", "IT Services"),
    "3571": ("Information Technology", "Technology Hardware, Storage & Peripherals"),
    "3572": ("Information Technology", "Technology Hardware, Storage & Peripherals"),
    "3577": ("Information Technology", "Technology Hardware, Storage & Peripherals"),
    "3578": ("Information Technology", "Technology Hardware, Storage & Peripherals"),
    "3576": ("Information Technology", "Communications Equipment"),
    "3674": ("Information Technology", "Semiconductors & Semiconductor Equipment"),
    "3559": ("Information Technology", "Semiconductors & Semiconductor Equipment"),
    "3841": ("Health Care", "Surgical & Medical Instruments"),
    "4813": ("Communication Services", "Telephone Communications"),
    "4911": ("Utilities", "Electric Services"),
    "5812": ("Consumer Discretionary", "Eating & Drinking Places"),
    "6021": ("Financials", "Commercial Banks"),
    "6221": ("Financials", "Security Brokers & Dealers"),
    "6282": ("Financials", "Services Allied with Securities Exchange"),
    "6331": ("Financials", "Fire, Marine & Casualty Insurance"),
    "7389": ("Industrials", "Business Services"),
    "8200": ("Consumer Discretionary", "Educational Services"),
    "8742": ("Industrials", "Management & Public Relations Services"),
    "6798": ("Real Estate", "Equity REITs"),
    "4953": ("Industrials", "Environmental & Facilities Services"),
}

_OVERRIDES: dict[str, tuple[str, str]] = {
    "SHW": ("Materials", "Paints & Coatings"),
    "GOOGL": ("Communication Services", "Interactive Media & Services"),
    "GOOG": ("Communication Services", "Interactive Media & Services"),
    "META": ("Communication Services", "Interactive Media & Services"),
    "PINS": ("Communication Services", "Interactive Media & Services"),
    "SNAP": ("Communication Services", "Interactive Media & Services"),
    "MTCH": ("Communication Services", "Interactive Media & Services"),
    "BMBL": ("Communication Services", "Interactive Media & Services"),
    "DIS": ("Communication Services", "Movies & Entertainment"),
    "WBD": ("Communication Services", "Movies & Entertainment"),
    "V": ("Information Technology", "IT Services"),
    "MA": ("Information Technology", "IT Services"),
    "PYPL": ("Information Technology", "IT Services"),
    "FIS": ("Information Technology", "IT Services"),
    "FI": ("Information Technology", "IT Services"),
    "GPN": ("Information Technology", "IT Services"),
    "NKE": ("Consumer Discretionary", "Apparel, Accessories & Luxury Goods"),
    "CROX": ("Consumer Discretionary", "Apparel, Accessories & Luxury Goods"),
    "DECK": ("Consumer Discretionary", "Apparel, Accessories & Luxury Goods"),
    "ONON": ("Consumer Discretionary", "Apparel, Accessories & Luxury Goods"),
}


def _gics_case(which: str) -> str:
    idx = 0 if which == "sector" else 1
    fallback = "Other / Nonclassifiable" if which == "sector" else "Nonclassifiable"
    four = [(int(k), v[idx]) for k, v in _SIC_GICS.items() if len(k) == 4]
    three = [(int(k), v[idx]) for k, v in _SIC_GICS.items() if len(k) == 3]
    two = [(int(k), v[idx]) for k, v in _SIC_GICS.items() if len(k) == 2]
    lines = ["CASE"]
    for code, val in four:
        lines.append("WHEN c.sic4 = %d THEN %s" % (code, _q(val)))
    for code, val in three:
        lines.append("WHEN FLOOR(c.sic4 / 10) = %d THEN %s" % (code, _q(val)))
    for code, val in two:
        lines.append("WHEN FLOOR(c.sic4 / 100) = %d THEN %s" % (code, _q(val)))
    lines.append("ELSE %s END" % _q(fallback))
    return "\n".join(lines)


def _overrides_cte() -> str:
    sect = "CASE pt.ticker " + " ".join("WHEN %s THEN %s" % (_q(t), _q(v[0])) for t, v in _OVERRIDES.items()) + " END"
    indu = "CASE pt.ticker " + " ".join("WHEN %s THEN %s" % (_q(t), _q(v[1])) for t, v in _OVERRIDES.items()) + " END"
    tin = ",".join(_q(t) for t in _OVERRIDES)
    return f"""overrides AS (
    SELECT cik, MAX(ovr_sector) AS ovr_sector, MAX(ovr_industry) AS ovr_industry FROM (
        SELECT pt.cik, {sect} AS ovr_sector, {indu} AS ovr_industry
        FROM primary_tickers pt WHERE pt.ticker IN ({tin})
    ) z GROUP BY cik
)"""


def _classify_compute_cte() -> str:
    descr_subq = "(SELECT tag_id FROM xbrl_tags WHERE namespace='dei' AND tag='EntitySicDescription')"
    return f"""WITH tradeable AS (
    SELECT DISTINCT cik FROM primary_tickers
),
code AS (
  SELECT cik, sic4 FROM (
    SELECT f.cik,
           CAST(f.val AS UNSIGNED) AS sic4,
           ROW_NUMBER() OVER (
             PARTITION BY f.cik
             ORDER BY f.`end` DESC, f.filed DESC, f.id DESC
           ) AS rn
      FROM facts_enc f
      JOIN tradeable tk ON tk.cik = f.cik
     WHERE f.tag_id = {_SIC_CODE_SUBQ} AND f.val IS NOT NULL
  ) x WHERE rn = 1
),
sic_descr AS (
  SELECT cik, descr FROM (
    SELECT f.cik,
           f.val_text AS descr,
           ROW_NUMBER() OVER (
             PARTITION BY f.cik
             ORDER BY f.`end` DESC, f.filed DESC, f.id DESC
           ) AS rn
      FROM facts_enc f
      JOIN tradeable tk ON tk.cik = f.cik
     WHERE f.tag_id = {descr_subq} AND f.val_text IS NOT NULL
  ) x WHERE rn = 1
),
{_overrides_cte()},
classified AS (
  SELECT c.cik, c.sic4,
         COALESCE(o.ovr_sector, {_gics_case("sector")}) AS sector,
         COALESCE(o.ovr_industry, {_gics_case("industry")}) AS industry,
         COALESCE(d.descr, CONCAT('SIC ', CAST(c.sic4 AS CHAR))) AS sub_industry
  FROM code c
  LEFT JOIN sic_descr d ON d.cik = c.cik
  LEFT JOIN overrides o ON o.cik = c.cik
)"""


_cik_gics_checked_at = 0.0
_cik_gics_present = False


def _has_cik_gics() -> bool:
    global _cik_gics_checked_at, _cik_gics_present
    now = time.monotonic()
    if now - _cik_gics_checked_at < 60.0:
        return _cik_gics_present
    try:
        sess = _session()
        try:
            _cik_gics_present = sess.execute("SHOW TABLES LIKE 'cik_gics'").fetchone() is not None
        finally:
            sess.close()
    except Exception:
        _cik_gics_present = False
    _cik_gics_checked_at = now
    return _cik_gics_present


def _classify_cte() -> str:
    if _has_cik_gics():
        return "WITH classified AS (SELECT cik, sic4, sector, industry, sub_industry FROM cik_gics)"
    return _classify_compute_cte()


@dolt_cached
def cik_in_clause(
    sector: str | None = None,
    industry: str | None = None,
    db_path: str | None = None,
) -> str | None:
    if not sector and not industry:
        return None
    where: list[str] = []
    if sector:
        where.append("sector = %s" % _q(sector))
    if industry:
        where.append("industry = %s" % _q(industry))
    sql = f"{_classify_cte()} SELECT cik FROM classified WHERE " + " AND ".join(where)
    sess = _session(db_path)
    try:
        rows = _rows(sess, sql)
    finally:
        sess.close()
    return ",".join(_q(r[0]) for r in rows)


def _latest_metric_cte(name: str, tag: str, alias: str, frequency: str = "annual") -> str:
    return f"""{name} AS (
  SELECT cik, val AS {alias} FROM (
    SELECT cik, val,
           row_number() OVER (PARTITION BY cik ORDER BY fiscal_year DESC, period_ending DESC) AS rn
    FROM standardized_statements_enc
    WHERE tag = {_q(tag)} AND frequency = {_q(frequency)} AND val IS NOT NULL
  ) z WHERE rn = 1
)"""


def _revenue_cte() -> str:
    return """rev AS (
  SELECT cik, val AS revenue FROM (
    SELECT cik, val,
           row_number() OVER (
               PARTITION BY cik
               ORDER BY fiscal_year DESC, period_ending DESC,
                        CASE WHEN tag = 'total_revenue' THEN 0 ELSE 1 END
           ) AS rn
    FROM standardized_statements_enc
    WHERE tag IN ('total_revenue', 'operating_revenue')
      AND frequency = 'annual' AND val IS NOT NULL AND val <> 0
  ) z WHERE rn = 1
)"""


def _has_revenue_cte() -> str:
    tags = (
        "'total_revenue', 'operating_revenue', 'total_interest_income', "
        "'premiums_earned', 'revenues_excl_interest_dividends', 'total_noninterest_income'"
    )
    return f"""has_rev AS (
  SELECT DISTINCT cik FROM standardized_statements_enc
  WHERE frequency = 'annual' AND val IS NOT NULL AND val <> 0
    AND period_ending >= CURRENT_DATE - INTERVAL 2 YEAR AND tag IN ({tags})
)"""


def _productive_cte() -> str:
    tags = "'net_ppe', 'net_inventory', 'goodwill', 'intangible_assets'"
    return f"""productive AS (
  SELECT DISTINCT cik FROM standardized_statements_enc
  WHERE frequency = 'annual' AND val IS NOT NULL AND val > 0
    AND period_ending >= CURRENT_DATE - INTERVAL 2 YEAR AND tag IN ({tags})
)"""


@dolt_cached
def list_sectors(db_path: str | None = None) -> list[dict[str, Any]]:
    sql = f"{_classify_cte()} SELECT sector, COUNT(*) AS n FROM classified GROUP BY sector ORDER BY n DESC"
    sess = _session(db_path)
    try:
        rows = _rows(sess, sql)
    finally:
        sess.close()
    return [{"label": f"{sector} ({n})", "value": sector} for sector, n in rows]


@dolt_cached
def list_industries(sector: str | None = None, db_path: str | None = None) -> list[dict[str, Any]]:
    where = "WHERE sector = %s" % _q(sector) if sector else ""
    sql = (
        f"{_classify_cte()} SELECT sector, industry, COUNT(*) AS n "
        f"FROM classified {where} GROUP BY sector, industry ORDER BY n DESC"
    )
    sess = _session(db_path)
    try:
        rows = _rows(sess, sql)
    finally:
        sess.close()
    return [
        {"label": f"{industry} ({n})", "value": industry, "extraInfo": {"sector": sector_}}
        for sector_, industry, n in rows
    ]


@dolt_cached
def sector_industry_aggregates(
    sector: str | None = None,
    industry: str | None = None,
    db_path: str | None = None,
) -> list[dict[str, Any]]:
    if industry:
        group_field = "sub_industry"
        where_sql = "WHERE cl.industry = %s" % _q(industry)
        group_select = "b.sector, b.industry, b.sub_industry"
        group_by = "b.sector, b.industry, b.sub_industry"
        order_by = "companies DESC, industry ASC, sub_industry ASC"
    elif sector:
        group_field = "industry"
        where_sql = "WHERE cl.sector = %s" % _q(sector)
        group_select = "b.sector, b.industry, NULL AS sub_industry"
        group_by = "b.sector, b.industry"
        order_by = "companies DESC, industry ASC"
    else:
        group_field = "sector"
        where_sql = ""
        group_select = "b.sector, NULL AS industry, NULL AS sub_industry"
        group_by = "b.sector"
        order_by = "companies DESC, sector ASC"

    sql = f"""{_classify_cte()},
metrics_ranked AS (
  SELECT cik, tag, val,
         ROW_NUMBER() OVER (PARTITION BY cik, tag ORDER BY period_ending DESC, fiscal_year DESC) rn
    FROM standardized_statements_enc
     WHERE frequency='annual' AND tag IN (
         'total_revenue',
         'operating_revenue',
         'total_cost_of_revenue',
         'operating_cost_of_revenue',
         'net_income',
         'total_assets'
     )
     AND period_ending >= CURRENT_DATE - INTERVAL 2 YEAR
),
metrics_latest AS (
  SELECT cik, tag, val
    FROM metrics_ranked
   WHERE rn = 1
),
metrics AS (
  SELECT cik,
                 COALESCE(
                     MAX(CASE WHEN tag='total_revenue' THEN val ELSE NULL END),
                     MAX(CASE WHEN tag='operating_revenue' THEN val ELSE NULL END)
                 ) AS reported_revenue,
                 COALESCE(
                     MAX(CASE WHEN tag='total_cost_of_revenue' THEN val ELSE NULL END),
                     MAX(CASE WHEN tag='operating_cost_of_revenue' THEN val ELSE NULL END)
                 ) AS cost_of_revenue,
         MAX(CASE WHEN tag='net_income' THEN val ELSE NULL END) net_income,
         MAX(CASE WHEN tag='total_assets' THEN val ELSE NULL END) assets,
                 CASE
                     WHEN COALESCE(
                         MAX(CASE WHEN tag='total_revenue' THEN val ELSE NULL END),
                         MAX(CASE WHEN tag='operating_revenue' THEN val ELSE NULL END)
                     ) > 0
                        AND (
                            COALESCE(
                                MAX(CASE WHEN tag='total_cost_of_revenue' THEN val ELSE NULL END),
                                MAX(CASE WHEN tag='operating_cost_of_revenue' THEN val ELSE NULL END)
                            ) IS NULL
                            OR COALESCE(
                                MAX(CASE WHEN tag='total_cost_of_revenue' THEN val ELSE NULL END),
                                MAX(CASE WHEN tag='operating_cost_of_revenue' THEN val ELSE NULL END)
                            ) <= 0
                            OR COALESCE(
                                MAX(CASE WHEN tag='total_cost_of_revenue' THEN val ELSE NULL END),
                                MAX(CASE WHEN tag='operating_cost_of_revenue' THEN val ELSE NULL END)
                            ) / NULLIF(
                                COALESCE(
                                    MAX(CASE WHEN tag='total_revenue' THEN val ELSE NULL END),
                                    MAX(CASE WHEN tag='operating_revenue' THEN val ELSE NULL END)
                                ),
                                0
                            ) < 0.99
                        )
                     THEN 1 ELSE 0
                 END AS has_revenue,
                 CASE
                     WHEN COALESCE(
                         MAX(CASE WHEN tag='total_revenue' THEN val ELSE NULL END),
                         MAX(CASE WHEN tag='operating_revenue' THEN val ELSE NULL END)
                     ) > 0
                        AND COALESCE(
                            MAX(CASE WHEN tag='total_cost_of_revenue' THEN val ELSE NULL END),
                            MAX(CASE WHEN tag='operating_cost_of_revenue' THEN val ELSE NULL END)
                        ) > 0
                        AND COALESCE(
                            MAX(CASE WHEN tag='total_cost_of_revenue' THEN val ELSE NULL END),
                            MAX(CASE WHEN tag='operating_cost_of_revenue' THEN val ELSE NULL END)
                        ) / NULLIF(
                            COALESCE(
                                MAX(CASE WHEN tag='total_revenue' THEN val ELSE NULL END),
                                MAX(CASE WHEN tag='operating_revenue' THEN val ELSE NULL END)
                            ),
                            0
                        ) >= 0.99
                     THEN 0
                     ELSE COALESCE(
                         MAX(CASE WHEN tag='total_revenue' THEN val ELSE NULL END),
                         MAX(CASE WHEN tag='operating_revenue' THEN val ELSE NULL END),
                         0
                     )
                 END AS revenue
    FROM metrics_latest
   GROUP BY cik
),
base AS (
  SELECT cl.sector,
         cl.industry,
         cl.sub_industry,
                 COALESCE(m.revenue, 0) AS revenue,
         m.net_income,
         COALESCE(m.assets, 0) AS assets,
         m.has_revenue
    FROM classified cl
    LEFT JOIN metrics m ON m.cik = cl.cik
    {where_sql}
)
SELECT {group_select},
       COUNT(*) AS companies,
       ROUND(100.0 * SUM(b.has_revenue) / COUNT(*), 1) AS with_revenue,
       ROUND(
         100.0 * SUM(CASE WHEN b.net_income > 0 THEN 1 ELSE 0 END)
         / NULLIF(SUM(CASE WHEN b.net_income IS NOT NULL THEN 1 ELSE 0 END), 0),
         1
       ) AS profitable,
       ROUND(SUM(CASE WHEN b.revenue > 0 THEN b.revenue ELSE 0 END), 0) AS total_revenue,
       ROUND(SUM(CASE WHEN b.net_income IS NOT NULL THEN b.net_income ELSE 0 END), 0) AS total_net_income,
       ROUND(SUM(CASE WHEN b.assets > 0 THEN b.assets ELSE 0 END), 0) AS total_assets,
       ROUND(
         100.0 * SUM(CASE WHEN b.net_income IS NOT NULL THEN b.net_income ELSE 0 END)
         / NULLIF(SUM(CASE WHEN b.revenue > 0 THEN b.revenue ELSE 0 END), 0),
         2
       ) AS aggregate_net_margin,
       ROUND(
         100.0 * SUM(CASE WHEN b.net_income IS NOT NULL THEN b.net_income ELSE 0 END)
         / NULLIF(SUM(CASE WHEN b.assets > 0 THEN b.assets ELSE 0 END), 0),
         2
       ) AS aggregate_roa
  FROM base b
 GROUP BY {group_by}
 ORDER BY {order_by}"""

    sess = _session(db_path)
    try:
        rows = _rows(sess, sql)
    finally:
        sess.close()

    out: list[dict[str, Any]] = []
    for (
        sec,
        ind,
        sub,
        companies,
        with_revenue,
        profitable,
        total_revenue,
        total_net_income,
        total_assets,
        aggregate_net_margin,
        aggregate_roa,
    ) in rows:
        row: dict[str, Any] = {"Sector": sec}
        if group_field in ("industry", "sub_industry"):
            row["Industry"] = ind
        if group_field == "sub_industry":
            row["Sub-Industry"] = sub
        row["Companies"] = companies
        row["With Revenue"] = with_revenue
        row["Profitable"] = profitable
        row["Total Revenue"] = total_revenue
        row["Total Net Income"] = total_net_income
        row["Total Assets"] = total_assets
        row["Aggregate Net Margin"] = aggregate_net_margin
        row["Aggregate ROA"] = aggregate_roa
        out.append(row)

    return out
