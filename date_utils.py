from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta


def past_n_days(venus_ds, day_cnt=0, output_format="%Y%m%d"):
    ds = datetime.strptime(str(venus_ds), "%Y%m%d") + timedelta(days=day_cnt)
    return ds.strftime(output_format)


def past_n_months(venus_month_ds, month_cnt=0, output_format="%Y%m"):
    month_ds = str(venus_month_ds)[:6]

    ds = datetime.strptime(month_ds, "%Y%m") + relativedelta(months=month_cnt)
    return ds.strftime(output_format)


def get_last_day(venus_ds, output_format="%Y%m%d"):
    ds_str = str(venus_ds)
    ds_format = "%Y%m%d"
    if len(ds_str) == 6:
        ds_format = "%Y%m"  # Format = %YYYYMM%
    ds = datetime.strptime(ds_str, ds_format)
    next_month = ds.replace(day=28) + timedelta(days=4)
    last_day = next_month - timedelta(days=next_month.day)
    return last_day.strftime(output_format)


if __name__ == '__main__':
    pass
