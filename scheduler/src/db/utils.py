import pandas as pd


def generate_pseudonym(ds: pd.DataFrame):
    row = ds.iloc[0]
    cpr = str(row["PatientID"])[:4]
    full_name = str(row["PatientName"]).split("^")
    name = [name[0] for names in full_name for name in names.split(" ")]
    return cpr + "".join(name)