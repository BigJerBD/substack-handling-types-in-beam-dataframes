import typing
from datetime import date
from decimal import Decimal

import apache_beam as beam
from apache_beam.dataframe.transforms import DataframeTransform
from apache_beam.typehints.row_type import RowTypeConstraint


class MyRow(typing.NamedTuple):
    row_id: str
    time: date
    result: Decimal

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

    rows = (
        rows
        | DataframeTransform(adjust_value)
        | beam.Map(lambda row: row).with_output_types(
            RowTypeConstraint.from_user_type(MyRow)
        )
    )

    print("After Applying RowTypeConstraint from user type :\n")
    print(rows.element_type)
    print()

    rows = (
        rows
        | beam.Map(lambda row: row).with_output_types(
            RowTypeConstraint.from_fields(
                [
                    ('row_id', str),
                    ('time', date),
                    ('result', Decimal)
                ]
            )
        )
    )

    print("After Applying RowTypeConstraint from field:\n")
    print(rows.element_type)
    print()

    rows | beam.Map(print)
