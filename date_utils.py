from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta


def past_n_days(venus_ds, day_cnt=0, output_format="%Y%m%d"):
    ds = datetime.strptime(str(venus_ds), "%Y%m%d") + timedelta(days=day_cnt)
    return ds.strftime(output_format)


def past_n_months(venus_month_ds, month_cnt=0, output_format="%Y%m"):
    month_ds = str(venus_month_ds)[:6]

    ds = datetime.strptime(month_ds, "%Y%m") + relativedelta(months=month_cnt)
    return ds.strftime(output_format)


# 获取每个月的最后一天.
def month_last_date(venus_month_ds, output_format="%Y%m%d"):
    # The day 28 exists in every month. 4 days later, it's always next month
    next_month = datetime.strptime(str(venus_month_ds), "%Y%m").replace(day=28) + timedelta(days=4)
    # subtracting the number of the current day brings us back one month
    rst = next_month - timedelta(days=next_month.day)
    if output_format is None:
        return rst

    return rst.strftime(output_format)


# 获取每个月最后一个周n的YYYYMMDD格式日期
def last_date_of_month(venus_month_ds, day_of_week=0, output_format="%Y%m%d"):
    # day of week = 0 means Sunday.
    last_day_of_month = month_last_date(venus_month_ds, output_format=None)
    ans = last_day_of_month - timedelta((last_day_of_month.isoweekday() - day_of_week + 7) % 7)
    return ans.strftime(output_format)


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
    print(last_date_of_month('202210', 3))
    # now = datetime.date.today()  # 获取今天日期，方便后面加减
    # print(get_past_days(now.isoweekday()))  # 获取当月最后一个周日的日期
