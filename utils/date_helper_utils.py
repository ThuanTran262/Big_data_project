from datetime import datetime, timedelta


def get_next_nday(start_date, n):
    """
    start_date: yyyy-mm-dd
    """
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    date_li = [datetime.strftime(start_date + timedelta(days=i), '%Y-%m-%d') for i in range(n+1)]
    return date_li

def get_days_between(start_date, end_date):
    """
    Get list days between two days
    start_date: 'yyyy-mm-dd'
    end_date: 'yyyy-mm-dd'
    """
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')
    delta = (end_date-start_date).days
    date_li = [datetime.strftime(start_date + timedelta(days=i), '%Y-%m-%d') for i in range(delta+1)]
    return date_li

def get_date_range(date, n_lag, m_lead):
    """
    Get n day before and m day after
    """
    date_li = []
    date_to_time = datetime.strptime(date, '%Y-%m-%d')
    date_before = [datetime.strftime(date_to_time + timedelta(days=-i), '%Y-%m-%d') for i in range(1, n_lag+1)]
    date_after = [datetime.strftime(date_to_time + timedelta(days=i), '%Y-%m-%d') for i in range(1, m_lead+1)]
    date_li.extend(date_after)
    date_li.extend([date])
    date_li.extend(date_before)
    return date_li

def get_lag_nday_excl_weekend(date, n_day, max_day):
    date_li = get_date_range(date, n_lag=max_day, m_lead=0)
    weekdays = [5, 6]
    date_li_excl_weekend = []
    for dt in date_li[1:]:
        dt = datetime.strptime(dt, '%Y-%m-%d')
        if dt.weekday() not in weekdays:
            dt = datetime.strftime(dt, '%Y-%m-%d')
            date_li_excl_weekend.append(dt)
    return date_li_excl_weekend[:n_day]
    