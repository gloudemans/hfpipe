FIELDNAMES = [
    # Metadata
    "scan_date", "patient_name", "birth_date", "age", "sex", "ethnicity",
    "height", "weight", "measured", "analyzed", "source_file",
    # Composition — Arms
    "arms_fat_mass", "arms_lean_mass", "arms_bmc",
    "arm_left_fat_mass", "arm_left_lean_mass", "arm_left_bmc",
    "arm_right_fat_mass", "arm_right_lean_mass", "arm_right_bmc",
    "arms_diff_fat_mass", "arms_diff_lean_mass", "arms_diff_bmc",
    # Composition — Legs
    "legs_fat_mass", "legs_lean_mass", "legs_bmc",
    "leg_left_fat_mass", "leg_left_lean_mass", "leg_left_bmc",
    "leg_right_fat_mass", "leg_right_lean_mass", "leg_right_bmc",
    "legs_diff_fat_mass", "legs_diff_lean_mass", "legs_diff_bmc",
    # Composition — Trunk
    "trunk_fat_mass", "trunk_lean_mass", "trunk_bmc",
    "trunk_left_fat_mass", "trunk_left_lean_mass", "trunk_left_bmc",
    "trunk_right_fat_mass", "trunk_right_lean_mass", "trunk_right_bmc",
    "trunk_diff_fat_mass", "trunk_diff_lean_mass", "trunk_diff_bmc",
    # Composition — Android / Gynoid
    "android_fat_mass", "android_lean_mass", "android_bmc",
    "gynoid_fat_mass", "gynoid_lean_mass", "gynoid_bmc",
    # Composition — Total (+ z_score for total only)
    "total_fat_mass", "total_lean_mass", "total_bmc", "total_z_score",
    "total_left_fat_mass", "total_left_lean_mass", "total_left_bmc",
    "total_right_fat_mass", "total_right_lean_mass", "total_right_bmc",
    "total_diff_fat_mass", "total_diff_lean_mass", "total_diff_bmc",
    # Densitometry — subregions (BMD value only)
    "bmd_head", "bmd_arms", "bmd_legs", "bmd_trunk",
    "bmd_ribs", "bmd_spine", "bmd_pelvis",
    # Densitometry — total (BMD + T-score + Z-score)
    "bmd_total", "bmd_total_t_score", "bmd_total_z_score",
    # VAT
    "vat_fat_mass", "vat_fat_volume",
    # Page 4
    "resting_metabolic_rate", "bmi",
]

_NULL_SENTINELS = frozenset({"n/a", "na", "", "-", "null", "none", "not available"})
_STRING_FIELDS = frozenset({
    "scan_date", "patient_name", "birth_date", "sex", "ethnicity",
    "measured", "analyzed", "source_file",
})


def parse_and_validate(raw_json: dict) -> dict:
    """Coerce types and ensure all FIELDNAMES keys are present."""
    row = {}
    for key in FIELDNAMES:
        val = raw_json.get(key)
        if val is None:
            row[key] = ""
            continue
        str_val = str(val).strip()
        if str_val.lower() in _NULL_SENTINELS:
            row[key] = ""
            continue
        if key not in _STRING_FIELDS:
            try:
                f = float(str_val.replace(",", ""))
                row[key] = int(f) if f == int(f) and "." not in str_val else f
                continue
            except (ValueError, TypeError):
                pass
        row[key] = str_val
    return row
