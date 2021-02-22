from typing import List, Iterable

from scipy.stats import chi2_contingency, fisher_exact
from tqdm.auto import tqdm


def flatten_nest(nest: Iterable[Iterable], output="set") -> Iterable:
    if output == "set":
        return set([item for sublist in nest for item in sublist])
    elif output == "list":
        return [item for sublist in nest for item in sublist]
    else:
        raise TypeError("Output must be 'set' or 'list'")


def get_p_values(
    df,
    mode="chi2_contingency",
):

    pval_list: List[float] = []
    for i in tqdm(range(len(df))):
        row = df.iloc[i]
        contingency_table = [
            [row["n_test"], row["n_total_test"] - row["n_test"]],
            [row["n_control"], row["n_total_control"] - row["n_control"]],
        ]

        if mode == "chi2_contingency":
            _, pval, _, _ = chi2_contingency(contingency_table)
        elif mode == "fisher_exact":
            _, pval = fisher_exact(contingency_table)
        else:
            raise ValueError(
                "Stat test mode must be 'chi2_contingency' or 'fisher_exact'"
            )

        pval_list.append(pval)
    return pval_list
