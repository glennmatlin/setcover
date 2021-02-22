#!/usr/bin/env python
# coding: utf-8

import concurrent.futures
from itertools import repeat
from typing import Iterable, List, Tuple

import pandas as pd
import plotly
import plotly.express as px
import plotly.graph_objects as go
import swifter
from plotly.subplots import make_subplots
from tqdm.auto import tqdm

from setcover.utils import flatten_nest


class Report:
    def __init__(self, title, df, codes, codes_limit=None):
        self.title = title
        self.df = df
        self.codes = codes
        self.codes_limit = codes_limit
        self.registry_n_total = self.df.iloc[0].n_total_test
        self.control_n_total = self.df.iloc[0].n_total_control
        self.metrics_df: pd.DataFrame
        self.plot: plotly.graph_objects.Figure
        self.metrics_df = self.get_metrics_df()
        self.plot = self.visualization()

    def _get_metrics(self, n_codes: int):
        codes_used = self.codes[:n_codes]
        df_ = self.df.loc[codes_used]
        registry_ids = flatten_nest(df_.registry_ids.to_list())
        registry_count = len(registry_ids)
        control_ids = flatten_nest(df_.control_ids.to_list())
        control_count = len(control_ids)
        return n_codes, codes_used[-1], registry_count, control_count

    def get_metrics_df(self) -> pd.DataFrame:
        if self.codes_limit:
            codes_range = list(range(1, self.codes_limit + 1))
        else:
            codes_range = list(range(1, len(self.codes) + 1))
        with concurrent.futures.ProcessPoolExecutor(max_workers=16) as executor:
            pool = list(
                tqdm(
                    executor.map(self._get_metrics, codes_range),
                    total=len(codes_range),
                    desc="Calculating Metrics: Highest Rate Ratio Codes",
                    leave=True,
                )
            )
        metrics_df_ = pd.DataFrame(
            pool,
            columns=[
                "n_codes",
                "code_added",
                "registry_count",
                "control_count",
            ],
        )
        metrics_df_["registry_diff"] = metrics_df_.registry_count.diff()
        metrics_df_["control_diff"] = metrics_df_.control_count.diff()
        metrics_df_["registry_percent"] = (
            metrics_df_.registry_count / self.registry_n_total
        ) * 100
        metrics_df_["control_percent"] = (
            metrics_df_.control_count / self.control_n_total
        ) * 100
        return metrics_df_

    def visualization(self) -> plotly.graph_objects.Figure:
        plot_ = make_subplots(specs=[[{"secondary_y": True}]])
        x_axis = list(range(len(self.metrics_df)))
        # Add traces
        plot_.add_trace(
            go.Scatter(
                x=x_axis,
                y=self.metrics_df.registry_percent.to_list(),
                name="Registry Patients",
            ),
            secondary_y=False,
        )

        plot_.add_trace(
            go.Scatter(
                x=x_axis,
                y=[100 - p for p in self.metrics_df.control_percent.to_list()],
                name="Control Sample",
            ),
            secondary_y=True,
        )

        plot_.update_layout(
            title_text=f"<b>{self.title}</b><br>Registry VS Control – Percentage Covered by Trigger Codes"
        )

        # Set x-axis title
        plot_.update_xaxes(title_text="Number (N) ICD-10 DX Trigger Codes")

        # Set y-axes titles
        plot_.update_yaxes(
            title_text="Percentage of Registry Containing Trigger in DX",
            secondary_y=False,
            range=(0, 100),
        )
        plot_.update_yaxes(
            title_text="Percentage of Control Sample NOT Containing Trigger in DX",
            secondary_y=True,
            range=(0, 100),
        )

        return plot_


def main(title: str, df: pd.DataFrame, codes: List[str], codes_limit: int) -> Report:
    report = Report(title, df, codes, codes_limit)
    return report


if __name__ == "__main__":
    """ ETL """
    etl_bucket = (
        "s3://kh-data/setcover/Alexion/aHUS/etl/20210220/test_rate-000/etl_df.parquet"
    )
    etl_df = pd.read_parquet(etl_bucket)
    etl_df.set_index("code", inplace=True)
    etl_df["registry_ids"] = (
        etl_df["registry_ids"]
        .str.split(",")
        .swifter.apply(lambda x: [e.strip() for e in x])
        .tolist()
    )
    etl_df["control_ids"] = (
        etl_df["control_ids"]
        .str.split(",")
        .swifter.apply(lambda x: [e.strip() for e in x])
        .tolist()
    )

    """ Main Run """
    solution_bucket: str = "s3://"
    plot_title: str = "Test"
    solution_codes = pd.read_csv(solution_bucket).to_csv
    report = Report(title=plot_title, df=etl_df, codes=solution_codes, codes_limit=None)

    # TODO [High] Pickle + save reports
    # TODO [High] Save plots & dataframe
    # report.plot.update_xaxes(range=(0, 250))
