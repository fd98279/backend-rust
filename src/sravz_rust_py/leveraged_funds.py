import logging

import arch
import matplotlib.gridspec as gridspec
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def process(price_df: pd.DataFrame, message_key: str):
    price_col_name = 'AdjustedClose'
    base_etf_or_index_col = 'etf_us_qqq_AdjustedClose'
    col = 'etf_us_qqq_AdjustedClose'
    leverage_factor = {
        'etf_us_qqq_AdjustedClose': 1,
        'etf_us_qld_AdjustedClose': 2,
        'etf_us_tqqq_AdjustedClose': 3

    }
    adjusted_closed_columns = [
        x for x in price_df.columns.tolist() if price_col_name in x]
    sravzid = 'LeveragedFunds'
    price_df = price_df.reset_index()
    price_df.set_index('DateTime', inplace=True)

    print("Entire data start date: %s" %
          price_df.index[0].strftime('%Y-%m-%d'))
    print("Entire data end date: %s" % price_df.index[-1].strftime('%Y-%m-%d'))

    vertical_sections = 11
    widthInch = 10
    heightInch = vertical_sections * 5
    fig = plt.figure(figsize=(widthInch, heightInch))
    gs = gridspec.GridSpec(vertical_sections, 4, wspace=1, hspace=0.5)
    gs.update(left=0.1, right=0.9, top=0.965, bottom=0.03)
    chart_index = 0
    ax_price_plot = plt.subplot(gs[chart_index, :])
    price_df[adjusted_closed_columns].plot(
        label=price_col_name, ax=ax_price_plot)
    ax_price_plot.set_title(
        '{0} {1} Price'.format(sravzid, price_col_name))
    ax_price_plot.legend()

    chart_index = chart_index + 1
    ax_daily_return_plot = plt.subplot(gs[chart_index, :])
    for col in adjusted_closed_columns:
        price_df[f'{col}_Daily_Return'] = price_df[col].pct_change()
    daily_returns_columns = [
        x for x in price_df.columns.tolist() if 'Daily_Return' in x]
    price_df[daily_returns_columns].plot(
        label='Daily_Return', ax=ax_daily_return_plot)
    ax_daily_return_plot.set_title(
        '{0} {1} Daily Return'.format(sravzid, price_col_name))
    ax_daily_return_plot.legend()

    chart_index = chart_index + 1
    ax_daily_return_plot = plt.subplot(gs[chart_index, :])
    for col in adjusted_closed_columns:
        price_df[f'{col}_Cumulative_Returns'] = (
            1 + price_df[f'{col}_Daily_Return']).cumprod() - 1
    cum_returns_columns = [
        x for x in price_df.columns.tolist() if '_Cumulative_Returns' in x]
    price_df[cum_returns_columns].plot(
        label='Cumulative_Returns', ax=ax_daily_return_plot)
    ax_daily_return_plot.set_title(
        '{0} Cummulative Returns'.format(sravzid))
    ax_daily_return_plot.legend()

    chart_index = chart_index + 1
    ax_daily_return_factor_plot = plt.subplot(gs[chart_index, :])
    for col in adjusted_closed_columns:
        price_df[f'{col}_Daily_Return_Factor'] = price_df[f'{col}_Daily_Return'] / \
            price_df['etf_us_qqq_AdjustedClose_Daily_Return']
    daily_returns_factor_columns = [
        x for x in price_df.columns.tolist() if '_Daily_Return_Factor' in x]
    price_df[daily_returns_factor_columns].rolling(window=30).mean().plot(
        label='Daily_Return_Factor', ax=ax_daily_return_factor_plot)
    ax_daily_return_factor_plot.set_title(
        '{0} {1} Daily Return Factor'.format(sravzid, price_col_name))
    ax_daily_return_factor_plot.legend()

    chart_index = chart_index + 1
    ax_beta_slippage_plot = plt.subplot(gs[chart_index, :])
    for col in adjusted_closed_columns:
        price_df[f'{col}_Beta_Slippage'] = price_df[f'{col}_Cumulative_Returns'] - \
            (leverage_factor.get(col) *
             price_df[f'{base_etf_or_index_col}_Cumulative_Returns'])
        # Calculate beta slippage as the difference between cumulative ETF returns and expected returns
    cum_returns_columns = [
        x for x in price_df.columns.tolist() if '_Beta_Slippage' in x]
    price_df[cum_returns_columns].plot(
        label='Beta_Slippage', ax=ax_beta_slippage_plot)
    ax_beta_slippage_plot.set_title(
        '{0} Daily Return Beta Slippage'.format(sravzid))
    ax_beta_slippage_plot.legend()

    chart_index = chart_index + 1
    ax_30_day_rolling_volatility_plot = plt.subplot(gs[chart_index, :])
    for col in adjusted_closed_columns:
        price_df[f'{col}_Rolling_Volatility'] = price_df[f'{col}_Daily_Return'].rolling(
            window=30).std()
        # Calculate beta slippage as the difference between cumulative ETF returns and expected returns
    rolling_30day_vol_columns = [
        x for x in price_df.columns.tolist() if '_Rolling_Volatility' in x]
    price_df[rolling_30day_vol_columns].plot(
        label='30Day_Rolling_Vol', ax=ax_30_day_rolling_volatility_plot)
    ax_30_day_rolling_volatility_plot.set_title(
        '{0} 30 Day Rolling Volatility'.format(sravzid))
    ax_30_day_rolling_volatility_plot.legend()

    chart_index = chart_index + 1
    ax_describe = plt.subplot(gs[chart_index, :])
    ax_describe.axis('off')
    describe_df = price_df[[x for x in price_df.columns.tolist(
    ) if 'Daily_Return' in x]].describe().round(3)
    font_size = 14
    bbox = [0, 0, 1, 1]
    mpl_table = ax_describe.table(
        cellText=describe_df.values, rowLabels=describe_df.index, bbox=bbox, colLabels=["_".join(col.split("_")[:3]) for col in describe_df.columns])
    mpl_table.auto_set_font_size(False)
    mpl_table.set_fontsize(font_size)
    ax_describe.set_title(
        "{0} Daily Returns Summary Statistics".format(sravzid))

    chart_index = chart_index + 1
    ax_rolling_std_plot = plt.subplot(gs[chart_index, :])
    # price_df[col].plot(label=col, ax=ax_rolling_std_plot)
    for col in adjusted_closed_columns:
        (price_df[col].rolling(7).std() / price_df[col].rolling(7).mean() * 100).plot(
            label=f"{col} 7 days RSD", ax=ax_rolling_std_plot)
        (price_df[col].rolling(21).std() / price_df[col].rolling(21).mean() * 100).plot(
            label=f"{col} 21 days RSD", ax=ax_rolling_std_plot)
        (price_df[col].rolling(255).std() / price_df[col].rolling(255).mean() * 100).plot(
            label=f"{col} 255 days RSD", ax=ax_rolling_std_plot)
    ax_rolling_std_plot.set_title(
        '{0} {1} Rolling Relative Std'.format(sravzid, price_col_name))
    ax_rolling_std_plot.legend()

    chart_index = chart_index + 1177
    garch_conditional_vol = plt.subplot(gs[chart_index, :])
    for col in adjusted_closed_columns:
        returns_df = price_df[f'{col}_Daily_Return'].dropna()
        model = arch.arch_model(returns_df, vol='Garch', p=1, q=1)
        # Fit the model
        results = model.fit()
        vol = results.conditional_volatility
        vol = vol * np.sqrt(252)
        garch_conditional_vol.plot(
            results._index.values, vol, label=f'{col}_Daily_Return')
    garch_conditional_vol.set_title("Annualized Conditional Volatility")
    garch_conditional_vol.legend()

    for col in adjusted_closed_columns:
        print(price_df.groupby(price_df.index.year)[
              f'{col}_Cumulative_Returns'].last().pct_change() * 100)

    logging.info(f"Saving file to /tmp/data/{message_key}.png")
    plt.savefig(f"/tmp/data/{message_key}", bbox_inches='tight')
    return f"/tmp/data/{message_key}.png"
