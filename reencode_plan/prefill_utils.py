def include_resolution(df):

    df_update = df.copy()
    serie_resolution = (
        df["video_resolution_width"].astype(str)
        + "x"
        + df["video_resolution_height"].astype(str)
    )
    df_update["resolution"] = serie_resolution
    return df_update


def show_reencode_plan(df_filled):

    # step 1 - create aux columns
    df_show_draft1 = df_filled.copy()
    df_show_draft1["minutes"] = df_show_draft1["duration_seconds"] / 60
    mask_to_reencode = ~df_show_draft1["video_resolution_to_change"].isin([""])
    df_show_draft1["mask_to_reencode"] = mask_to_reencode

    # step 2 - slice dataframe.
    #  Columns: ['subfolder_n1', 'minutes_ok', 'minutes_to_reencode']
    df_show_draft2 = df_show_draft1[["subfolder_n1"]].copy()
    df_show_draft2["minutes_ok"] = df_show_draft1.loc[
        ~mask_to_reencode, "minutes"
    ]
    df_show_draft2["minutes_to_reencode"] = df_show_draft1.loc[
        mask_to_reencode, "minutes"
    ]

    # mount df_show
    df_groupby = df_show_draft2.groupby(["subfolder_n1"])
    df_agg = df_groupby[["minutes_ok", "minutes_to_reencode"]].agg(sum)
    df_show = df_agg.reset_index()

    # calculate minutes and percent to reencode
    minutes_ok_sum = df_show["minutes_ok"].sum()
    minutes_to_reencode_sum = df_show["minutes_to_reencode"].sum()
    percent_to_reencode = minutes_to_reencode_sum / minutes_ok_sum

    # round values
    df_show["minutes_ok"] = df_show["minutes_ok"].round(1)
    df_show["minutes_to_reencode"] = df_show["minutes_to_reencode"].round(1)

    # show reencode_plan and minutes to reencode
    print(df_show.to_string(index=False))
    print(
        f"\nThere is {minutes_to_reencode_sum:.1f} minutes "
        + f"({percent_to_reencode*100:.0f}%) to reencode"
    )
