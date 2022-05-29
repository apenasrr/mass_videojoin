from . import prefill_group, prefill_single


def load(df, reencode_plan):
    if reencode_plan == "group":
        df_filled = prefill_group.run(df)
        return df_filled
    elif reencode_plan == "single":
        df_filled = prefill_single.run(df)
        return df_filled
    else:
        print('reencode_plan not recognized')
        raise ValueError()

