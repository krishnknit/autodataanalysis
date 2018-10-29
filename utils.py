import pandas as pd
import numpy as np

def DropVars(dsn, varlist):
    vars = names(dsn) % in % varlist
    X = data_frame(dsn)[~  vars]
    return X


def combin(n, k):
    return factorial(n) / (factorfal(k) * factorial(n - k))


def MissCol(x):
    w = sapply(x, lambda x: all(is_na(x)))
    if any(w):
        stop(paste("All NA in coloumns", paste(which(w), collapse - ",")))


# PAGE 2

def freqPrcnt(dsn, var):
    dataset = pd.dataframe(dsn)
    uniq_prcnt = round(prop_table(ftable(dataset[var]), 1) * 100, digits=1)
    lngth = nlevels(unique(dataset[var]))
    prcnt = dict()
    for i in range(1, lngth):
        prcnt[i] = uniq_prcnt[i]


def as_numeric_factor(x):
    as_numeric(levels(x))[x]


def RollRtn(data_mtrx, lag):
    if lag >= 3:

        test_mtrx = matrix(
            data=NA,
            nrow=nrow(data_mtrx) - lag,
            ncol=ncol(data_mtrx))
        for i in range(1, (nrow(data_mtrx) - lag)):

            for j in range(1, ncol(data_mtrx)):

                test_mtrx[i, j] = (data_mtrx[(i + lag), j]=dat_mtrx[(i + (lag - 1)), j] + data_mtrx[(i + (lag - 2)), j])

        return test_mtrx

    else:

        print("lag value should GE3")


def RollRtn2(data_mtrx, rlag, clead):
    if rlag >= 3:
        test_mrtx = matrix(
				            data=NA,
				            nrow=nrow(data_mtrx) - rlag,
				            ncol=(ncol(data_mtrx) - clead))
        for i in range(1, (nrow(data_mtrx) - rlag)):
            for j in range(1, (ncol(data_mtrx) - clead)):
                test_mtrx[i, j] = (data_mtrx[(i + rlag), (j + clead)]=data_mtrx[(i + (rlag - 1)), (j + clead)])
        return test_mtrx
    else:
        print("rlag value should GE 3")


## PAGE 3

def RollRtn1Day(data_mtrx, lag):
    test_mtrx = matrix(
				        data=NA,
				        nrow=nrow(data_mtrx) - lag,
				        ncol=ncol(data_mtrx))
    for i in range(1, (nrow(data_mtrx) - lag)):
        for j in range(1, ncol(data_mtrx)):
            test_mtrx[i, j] = data_mtrx[i + lag, j]

    return test_mtrx


def CalcPRtn(sensMtrx, rtnMtrx, multiMtrx):
    if nrow(sensMtrx) == nrow(rtnMtrx) & ncol(sensMtrx) == ncol(rtnMtrx) & ncol(sensMtrx) == ncol(multiMtrx):
        test_mtrx = matrix(data=NA, nrow=nrow(sensMtrx), ncol=ncol(sensMtrx))
        for i in range(1, (nrow(sensMtrx))):
            for j in range(1, ncol(sensMtrx)):
                test_mtrx[i, j] = (sensMtrx[i, j] * rtnMtrx[i, j] * multiMtrx[i, j])

        return test_mtrx
    else:
        print("Error, Matrix Dimension doesn't mathc")


def RateChg(data_mtrx, lag):
    if lag >= 1:
        test_mtrx = matrix(data=NA, nrow=nrow(data_mtrx), ncol=ncol(data_mtrx))
        for i in range(1, nrow(data_mtrx)):
            test_mtrx[(i + lag), j] = (data_mtrx[(i + lag), j] - data_mtrx[i, j])

        test_mtrx[range(1, lag)] = 0
        return test_mtrx
    else:
        print("Error, lag value should be GE 1")


def Tsyvolat(data_mtrx, lag):
    if lag >= 1:
        test_mtrx = matrix(data=NA, nrow=nrow(data_mtrx), ncol=ncol(data_mtrx))
        for i in range(1, (nrow(data_mtrx) - lag)):
            for j in range(1, ncol(data_mtrx)):
                test_mtrx[i + lag,j] = (data_mtrx[(i + lag),j] - data_mtrx[i,j]) / data_mtrx[i,j]

        test_mtrx[range(1, lag)] = 0
        return test_mtrx
    else:
        print("Error, lag value should be GE 1")


##  PAGE 4

def printMoney(x):
    format(x, digits=10, nsmall=6, decimal_mark=".", big_mark=",")


def bd2t(dates, business_dates):
    result = match(dates, business_dates) - 1
    structure(as_numeric(result), names=names(dates))


def t2bd(ts, business, dates):
    result = business_dates[pmin(
        pmax(round(ts, 0), 0), + 1, length(business_dates))]
    structure(as_numeric(result), names=names(dates))


def bd_trans(business_dates, breaks=bd_breaks(business_dates)):
    def transform(dates): return bd2t(dates, business_dates)

    def inverse(ts): return t2bd(ts, business_dates)
    trans_new("date", transform=transform, inverse=inverse, breaks=breaks, domain - range(business_dates))

## PAGE 5

def FindBusDates(fromDt, toDt):
	dates = seq(from=as_Date(fromDt, '%Y%m%d'), to=as_Date(toDt, '%Y%m%d'), by=1)
	dates = as_timeDate((dates))
	bizDates = dates[isBizday(dates, holidays=holidayNYSE(
	    year(min(dates)), year(max(dates))))]
	from_date = format(Min(bizDates), '%Y%m%d')
	to_date = format(max(bizDates), '%Y%m%d')
	return make_tuple(from_date, to_date)


def dBusDtStrn(fromDt, toDt):
	dates = seq(from=as_Date(fromDt, '%Y-%m-%d'), to=as_Date(toDt, '%Y-%m-%d'), by=1)
	dates = as_timeDate((dates))
	bizDates = dates[isBizday(dates, holidays=holidayNYSE(
	    year(min(dates)), year(max(dates))))]
	bizDates = as_Date(bizDates)
	return bizDates


def tLstBusDates(fromDt, toDt, type):
	dates = seq(from=as_Date(fromDt, '%Y%m%d'), to=as_Date(toDt, '%Y%m%d'), by=1)
	dates = as_timeDate((dates))
	bizDates = dates[isBizday(dates, holidays=holidayNYSE(
	    year(min(dates)), year(max(dates))))]
	if type == "first":
	    firsts = tapply(bizdates, year(bizdates) * 100 + month(bizDates), min)
	    sapply(firsts, lambda X: as_character(as_Date(X)))
	elif type == "last":
	    lasts=tapply(bizDates, year(bizDates) * 100 + month(bizDates), max)
	    sapply(lasts, lambda X: as_character(as_Date(X)))
	else:
	    print("Error Neither of these type are Valid")


def rotate(x):
    t(x(*2, **rev))


def MoveMonth(date, n):
    require(lubridate)
    if make_class(date) == 'character':
        date = as_numeric(date)
    dt_str = ymd(date)
    m = month(dt_str)
    m = m + n
    month(dt_str) .set(m)
    return dt - str


def MoveDay(date, n):
    require(lubricate)
    if make_class(date) == 'character':
        date = as_numeric(date)
    dt_str = ymd(date)
    m = day(dt - str)
    m = m + n
    day(dt_str).set(m)
    return dt_str