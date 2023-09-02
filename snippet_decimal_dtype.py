from datetime import date

import apache_beam as beam
from apache_beam.dataframe.transforms import DataframeTransform
from handling_types_in_beam_dataframes.pandas.extensions.decimal import Decimal

def adjust_value(df):
    df["value"] = df["value"] + 1000
    df = df.rename(columns={'value': "result"})

    return df

with beam.Pipeline() as p:
    rows = (
        p
        | beam.Create(
            [
                beam.Row(row_id="1", time=date(2022, 1, 1), value=Decimal("10")),
                beam.Row(row_id="1", time=date(2022, 1, 2), value=Decimal("12")),
                beam.Row(row_id="2", time=date(2022, 1, 1), value=Decimal("103")),
                beam.Row(row_id="2", time=date(2022, 1, 2), value=Decimal("145"))
            ]
        )
    )

    print("Before Applying DataframeTransform:\n")
    print(rows.element_type)
    print()

    rows = rows | DataframeTransform(adjust_value)

    print("After Applying DataframeTransform:\n")
    print(rows.element_type)
    print()

    rows | beam.Map(print)
