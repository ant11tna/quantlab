import akshare as ak
import pandas as pd


def main() -> None:
    df = ak.fund_etf_hist_sina(symbol="sh510300")
    print(df.head())
    print(len(df))


if __name__ == "__main__":
    main()
