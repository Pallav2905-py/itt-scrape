import pandas as pd
import re

file_path = "vc_channel_partners_detailed.csv"
df = pd.read_csv(file_path)

# Negative keywords (only in Company_Name)
negative_keywords = ["croma", "vijay sales", "sony mony", "kohinoor", "reliance digital"]
pattern = re.compile("|".join(negative_keywords), re.IGNORECASE)
df = df[~df["Company_Name"].str.contains(pattern, na=False)]

def is_mobile(num):
    if pd.isna(num):
        return False
    
    raw = str(num).strip()
    digits = re.sub(r"\D", "", raw)  # keep only digits
    
    # Case 1: +91 prefixed mobile
    if digits.startswith("91") and len(digits) == 12 and digits[2] in "6789":
        return True
    
    # Case 2: normal 10-digit mobile
    if len(digits) == 10 and digits[0] in "6789":
        return True
    
    # Case 3: 11-digit starting with 0 (like 09876543210)
    if len(digits) == 11 and digits[0] == "0" and digits[1] in "6789":
        return True
    
    # Case 4: grouping check — mobiles usually split as 5+5
    parts = raw.split()
    if len(parts) == 2 and all(p.isdigit() for p in parts):
        if len(parts[0]) == 5 and len(parts[1]) == 5:
            return True  # mobile
    
    return False

# Separate mobiles and landlines
mobiles_df = df[df["Phone"].apply(is_mobile)]
landlines_df = df[~df["Phone"].apply(is_mobile)]

# Save results
mobiles_df.to_csv("vc_channel_partners_filtered.csv", index=False)
landlines_df.to_csv("vc_channel_partners_landlines.csv", index=False)

print("✅ Filtering complete")
print(" - Mobiles saved to vc_channel_partners_filtered.csv")
print(" - Landlines saved to vc_channel_partners_landlines.csv")
