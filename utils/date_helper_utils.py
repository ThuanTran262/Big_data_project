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
    date_li.extend(date_before)
    date_li.extend([date])
    date_li.extend(date_after)
    return date_li