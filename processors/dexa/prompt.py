from processors.dexa.schema import FIELDNAMES


def build_extraction_prompt() -> str:
    fields_list = "\n".join(f"  - {f}" for f in FIELDNAMES)
    return f"""Extract all data from this GE Lunar DEXA body composition report and return ONLY a JSON object — no prose, no markdown fences, no explanation.

Rules:
- Every key in the JSON must be one of the field names listed below. Do not add extra keys.
- If a field is not present in the report, set its value to null. Never omit a key.
- Numbers must be plain floats or integers — no units, no commas, no percent signs.
- Percentages are stored as floats (e.g. 23.4, not "23.4%").

- Dates use YYYY-MM-DD format.
- From the table near the top of page 1:
    "Birth Date" -> birth_date
    "Patient" -> patient_name
    "Height -> height
    "Weight" -> weight
    "Age" -> age
    "Sex" -> sex
    "Ethnicity" ethnicity
    "Measured" -> measured
    "Analyzed" -> analyzed

  - From the "Composition" table on page 1:
    "Arms"  → arms_*
    "Arm Left"  → arm_left_*
    "Arm Right" → arm_right_*
    "Arms Diff"  → arms_diff_*
    "Legs"  → legs_*
    "Leg Left"  → leg_left_*
    "Leg Right" → leg_right_*
    "Legs Diff"  → legs_diff_*
    "Trunk" → trunk_*
    "Trunk Left"  → trunk_left_*
    "Trunk Right" → trunk_right_*
    "Trunk Diff"  → trunk_diff_* 
    "Android" → android_*
    "Gynoid" → gynoid_*
    "Total" → total_*
    "Total Left"  → total_left_*
    "Total Right" → total_right_*
    "Total Diff"  → total_diff_* 

  - From the "Densitometry" table on page 2:
    "Head" -> bmd_head 
    "Arms"-> bmd_arms
    "Legs" -> bmd_legs
    "Trunk" -> bmd_trunk
    "Ribs" -> bmd_ribs
    "Spine" -> bmd_spine
    "Pelvis" -> bmd_pelvis
    "Total" -> bmd_total_*

  - From only the final row of the "Visceral Adipose Tissue" table on page 3:
    "Fat Mass" -> vat_fat_mass
    "Volume" -> vat_fat_volume

  - From page 4:
    Resting Metabolic Rate (kcal/day) -> resting_metabolic_rate
    Body Mass Index (kg/m²) -> bmi

- Return mass and weight values in pounds, length in inches, volume in cubic inches.
- resting_metabolic_rate is in calories (kcal) per day as a plain integer.
- bmi is in kg/m² as a float.

Required fields (return all of these):
{fields_list}

Return only the JSON object, starting with {{ and ending with }}.
"""
