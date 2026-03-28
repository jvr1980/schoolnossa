#!/usr/bin/env python3
"""
Format income-based tuition notes into structured tuition_fees_summary field.

For schools with income-based tuition, this script parses the tuition_notes
and creates a formatted summary suitable for display in the tuition column.
"""

import pandas as pd
import re
from typing import Optional

def extract_fee_summary(notes: str, school_name: str) -> Optional[str]:
    """
    Parse tuition notes and extract a formatted fee summary.
    Returns a compact list of fee tiers.
    """
    if pd.isna(notes) or not notes:
        return None

    notes = str(notes)
    lines = []

    # Handle specific schools that may have "No fee information" but we know their structure
    # Berlin Bilingual School
    if 'Berlin Bilingual' in school_name and 'Phorms' not in school_name:
        return "Affordable bilingual fees | Contact school"

    # Freie Schule Berlin Mahlsdorf
    if 'Mahlsdorf' in school_name:
        return "~€50/mo (3rd child) | State-subsidized (93%)"

    # Phorms Berlin Süd
    if 'Phorms' in school_name and 'Süd' in school_name:
        return "Income-based (~€796-1064/mo)"

    # Ev. Schule Köpenick
    if 'Köpenick' in school_name:
        return "EKBO: ~2.4% of income | Min: €30/mo"

    # Skip if it's a "no fee info" note (after handling specific schools)
    if 'No fee information' in notes or 'Public school' in notes:
        return None

    # Pattern 1: Income brackets with EUR amounts (e.g., "€30k+: €240", "ab 50,000€: 796€/month")
    bracket_pattern = r'(?:€|EUR\s*)?(\d{1,3}(?:,\d{3})*(?:k|\.\d+k)?)\s*(?:€|EUR)?\s*(?:\+|-)?\s*(?:income|Einkommen)?[:\s]+(?:€|EUR\s*)?(\d{1,4}(?:,\d{3})?)\s*(?:€|EUR)?(?:/month|/Monat)?'

    # Pattern 2: Grade-based fees (e.g., "Grades 1-6: €231-900")
    grade_pattern = r'(?:Grades?|Klasse|Grade)\s*(\d+(?:-\d+)?)\s*[:\s]+(?:€|EUR\s*)?(\d{1,4}(?:,\d{3})?)\s*(?:-\s*(?:€|EUR\s*)?(\d{1,4}(?:,\d{3})?))?'

    # Pattern 3: Min-max fees (e.g., "min €30/month", "max €350/month")
    minmax_pattern = r'(min(?:imum)?|max(?:imum)?)[:\s]+(?:€|EUR\s*)?(\d{1,4}(?:,\d{3})?)\s*(?:€|EUR)?(?:/month|/Monat)?'

    # Pattern 4: Percentage of income (e.g., "3.4% of taxable income")
    percent_pattern = r'(\d+(?:\.\d+)?)\s*%\s*(?:of|des)?\s*(?:taxable\s*)?(?:income|Einkommen)'

    # Pattern 5: Fixed monthly amounts (e.g., "€400/month for grades 5/6")
    fixed_pattern = r'(?:€|EUR\s*)?(\d{3,4})\s*(?:€|EUR)?/(?:month|Monat)\s*(?:for\s*)?(?:grades?|Klasse)?\s*(\d+(?:/\d+|-\d+)?)?'

    # Check for specific school patterns based on known structures

    # Berlin Bilingual School (not Phorms Bilingual)
    if 'Berlin Bilingual' in school_name and 'Phorms' not in school_name:
        lines.append("Affordable bilingual fees")
        lines.append("Contact school for rates")

    # Freie Schule Berlin Mahlsdorf
    elif 'Mahlsdorf' in school_name:
        lines.append("~€50/mo (3rd child)")
        lines.append("State-subsidized (93%)")

    # Berlin Cosmopolitan School - has detailed grade brackets
    elif 'Cosmopolitan' in school_name:
        grade_matches = re.findall(r'Grades?\s*(\d+(?:-\d+)?)[:\s]+€?(\d+)-(\d+)', notes)
        if grade_matches:
            for grade, low, high in grade_matches[:4]:
                lines.append(f"Gr.{grade}: €{low}-{high}/mo")

    # Phorms Berlin Süd - same structure as Mitte (check before generic Phorms)
    elif 'Phorms' in school_name and 'Süd' in school_name:
        lines.append("Income-based (~€796-1064/mo)")

    # Phorms - has income brackets
    elif 'Phorms' in school_name:
        bracket_matches = re.findall(r'ab\s*(\d{2,3}),?000\s*€?[:\s]+(\d{3,4})\s*€?/month', notes)
        if bracket_matches:
            for income, fee in bracket_matches[:4]:
                lines.append(f">{income}k: €{fee}/mo")

    # SIS Swiss International
    elif 'SIS' in school_name or 'Swiss' in school_name:
        range_match = re.search(r'(\d{1,2},?\d{3})-(\d{1,2},?\d{3})\s*EUR', notes)
        if range_match:
            lines.append(f"€{range_match.group(1)}-{range_match.group(2)}/mo (income-based)")

    # Freie Schule Anne-Sophie
    elif 'Anne-Sophie' in school_name:
        lines.append("Income-based: €100-1070/mo")
        lines.append("Admin fee: €750 one-time")

    # Catholic schools (Erzbistum Berlin) - percentage based
    elif 'Katholische' in school_name or 'Liebfrauen' in school_name or 'Theresienschule' in school_name:
        percent_match = re.search(r'(\d+(?:\.\d+)?)\s*%', notes)
        minmax = re.findall(r'(min(?:imum)?|max(?:imum)?)[:\s]*(?:€|EUR)?\s*(\d+)', notes, re.I)
        if percent_match:
            lines.append(f"{percent_match.group(1)}% of taxable income")
        if minmax:
            for mm_type, amount in minmax[:2]:
                lines.append(f"{mm_type.capitalize()}: €{amount}/mo")

    # Evangelische Schule Köpenick - EKBO school (check before generic Evangelische)
    elif 'Köpenick' in school_name:
        lines.append("EKBO: ~2.4% of income")
        lines.append("Min: €30/mo | Max: €350/mo")

    # Evangelical schools (EKBO) - percentage based
    elif 'Evangelische' in school_name or 'Ev.' in school_name or 'Grauen Kloster' in school_name:
        percent_match = re.search(r'(\d+(?:\.\d+)?)\s*%', notes)
        minmax = re.findall(r'(min(?:imum)?|max(?:imum)?)[:\s]*(?:€|EUR)?\s*(\d+)', notes, re.I)
        if percent_match:
            lines.append(f"{percent_match.group(1)}% of income")
        if minmax:
            for mm_type, amount in minmax[:2]:
                lines.append(f"{mm_type.capitalize()}: €{amount}/mo")
        # Check for specific min/max in notes
        if 'min €30' in notes.lower() or 'minimum: 30' in notes.lower():
            if not any('Min' in l for l in lines):
                lines.append("Min: €30/mo")
        if '€350' in notes or '350' in notes:
            if not any('Max' in l for l in lines):
                lines.append("Max: €350/mo")

    # Moser-Schule - fixed per grade
    elif 'Moser' in school_name:
        grade_matches = re.findall(r'€(\d{3,4})/month\s*for\s*grades?\s*(\d+(?:/\d+)?)', notes)
        if grade_matches:
            for fee, grades in grade_matches[:4]:
                lines.append(f"Gr.{grades}: €{fee}/mo")

    # Berlin British School
    elif 'British' in school_name:
        if 'Early Years' in notes:
            lines.append("Early Years: ~€6,000/yr")
        if 'Secondary' in notes or '15,000' in notes:
            lines.append("Secondary: up to €15,000+/yr")
        lines.append("Income-based options available")


    # Europa-Gymnasium - individually determined
    elif 'Europa' in school_name:
        lines.append("Individually determined")
        lines.append("Based on family income")

    # Generic fallback - try to extract any EUR amounts
    if not lines:
        # Look for any EUR range
        eur_range = re.search(r'€(\d{2,4})-(\d{2,4})', notes)
        if eur_range:
            lines.append(f"€{eur_range.group(1)}-{eur_range.group(2)}/mo")

        # Look for min/max
        minmax = re.findall(r'(min|max)[:\s]*€?(\d{2,4})', notes, re.I)
        for mm_type, amount in minmax[:2]:
            lines.append(f"{mm_type.capitalize()}: €{amount}/mo")

        # If income-based mentioned but no amounts
        if ('income' in notes.lower() or 'einkommens' in notes.lower()) and not lines:
            lines.append("Income-based (contact school)")

    if lines:
        return " | ".join(lines)
    return None


def create_tuition_display(row: pd.Series) -> str:
    """
    Create a display-friendly tuition string for a school.
    Combines monthly/annual amounts with income-based summaries.
    """
    parts = []

    monthly = row.get('tuition_monthly_eur')
    annual = row.get('tuition_annual_eur')
    notes = row.get('tuition_notes')
    school_name = row.get('schulname', '')

    # If we have specific amounts, use those
    if pd.notna(monthly):
        parts.append(f"€{int(monthly)}/mo")
    elif pd.notna(annual):
        parts.append(f"€{int(annual)}/yr")

    # If no specific amount but has income-based notes, extract summary
    if not parts and pd.notna(notes):
        summary = extract_fee_summary(notes, school_name)
        if summary:
            parts.append(summary)

    # Add scholarship/income-based indicators
    if row.get('scholarship_available') == True:
        parts.append("Scholarships")
    if row.get('income_based_tuition') == True and 'income' not in ' '.join(parts).lower():
        parts.append("Income-based")

    if parts:
        return " | ".join(parts)

    # For public schools
    if row.get('traegerschaft') != 'Privat':
        return "Free (public)"

    return "Contact school"


def main():
    import argparse

    parser = argparse.ArgumentParser(description='Format income-based tuition data')
    parser.add_argument('--input', '-i', default='combined_schools_with_metadata_msa_with_descriptions_v2_v3_v4.csv')
    parser.add_argument('--output', '-o', default='combined_schools_with_metadata_msa_with_descriptions_v2_v3_v4_formatted.csv')

    args = parser.parse_args()

    print(f"Loading {args.input}...")
    df = pd.read_csv(args.input, encoding='utf-8-sig')

    # Create tuition_display column
    print("Creating tuition_display column...")
    df['tuition_display'] = df.apply(create_tuition_display, axis=1)

    # Show results for private schools
    private = df[df['traegerschaft'] == 'Privat']

    print("\n" + "="*80)
    print("PRIVATE SCHOOLS - TUITION DISPLAY")
    print("="*80 + "\n")

    for _, row in private.iterrows():
        name = row['schulname'][:50]
        display = row['tuition_display']
        print(f"{name}")
        print(f"  → {display}")
        print()

    # Save
    df.to_csv(args.output, index=False, encoding='utf-8-sig')
    df.to_excel(args.output.replace('.csv', '.xlsx'), index=False, engine='openpyxl')
    print(f"\nSaved to {args.output}")

    # Stats
    private = df[df['traegerschaft'] == 'Privat']
    with_display = private[private['tuition_display'] != 'Contact school']
    print(f"\nPrivate schools with tuition info: {len(with_display)}/{len(private)}")


if __name__ == "__main__":
    main()
