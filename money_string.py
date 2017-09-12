from collections import OrderedDict


_digital = OrderedDict(
    [('万亿', 10 ** 12),
     ('千亿', 10 ** 11),
     ('百亿', 10 ** 10),
     ('十亿', 10 ** 9),
     ('亿', 10 ** 8),
     ('千万', 10 ** 7),
     ('百万', 10 ** 6),
     ('十万', 10 ** 5),
     ('万', 10 ** 4),
     ('千', 10 ** 3),
     ('百', 10 ** 2),
     ('十', 10 ** 1),
     ('元', 10 ** 0)])


def money_string(ms, unit):
    if isinstance(ms, str):
        num, digit = split_money_string(ms)
        if len(digit):
            for di in _digital.keys():
                if di in digit:
                    return num * (_digital.get(di)/_digital.get(unit))
        else:
            return num
    else:
        return ms


def split_money_string(ms):
    import re
    pattern = "\d*"
    digit = re.sub(pattern, '', ms)
    num = eval(ms.replace(digit, ''))
    return num, digit
