from django.shortcuts import render

import psycopg2
from fuzzywuzzy import fuzz


def index(request):
    return render(request, 'engine/index.html', {})


class Raw:
    def __init__(self, selectRaw='', fromRaw='', whereRaw='', andRaw=''):
        self.selectRaw = selectRaw
        self.fromRaw = fromRaw
        self.whereRaw = whereRaw
        self.andRaw = andRaw


def getSplit(str):
    if 'SELECT' not in str or 'FROM' not in str:
        return render('engine/index.html', {'raw': 'No input or syntax error.', 'input': str})
    splits = str.split('\n')

    hasSelect = True
    hasFrom = True
    hasWhere = False
    hasAnd = False
    previous_command = ''
    strSelect = ''
    strFrom = ''
    strWhere = ''
    strAnd = ''
    for split in splits:
        if '(' in strFrom and 'DISTINCT' in strFrom:
            pass

    for split in splits:
        # print('SPLIT : ', split)
        if 'SELECT' in split:
            strSelect = split
            previous_command = 'SELECT'

        elif 'FROM' in split:
            strFrom = split
            previous_command = 'FROM'

        elif 'WHERE' in split:
            strWhere = split
            hasWhere = True
            previous_command = 'WHERE'

        elif 'AND' in split:
            strAnd = split
            hasAnd = True
            previous_command = 'AND'
        else:
            if previous_command is 'SELECT':
                strSelect = strSelect + split
            elif previous_command is 'FROM':
                strFrom = strFrom + split
            elif previous_command is 'WHERE':
                strWhere = strWhere + split
    return Raw(selectRaw=strSelect, fromRaw=strFrom, whereRaw=strWhere, andRaw=strAnd)


def getCost(exps):
    costs = 0
    for exp in exps:
        exp = ''.join(exp)
        if '..' in exp:
            index = exp.index('..')
            # print('find cost: ', index)
            c = exp[index + 2:index + 20]
            # print(c)
            split = c.split(' row')
            costs = costs + float(split[0])
            # print(costs, ' + ', float(split[0]))
    costs = round(costs, 2)
    # print(costs)
    return costs


def new_split(str):
    str = str.replace('\n', ' ').replace('\r', '').replace('  ', ' ')
    str_json = {}

    select_position = str.find('SELECT')
    if 'EXPLAIN' in str[:select_position]:
        str_json['pre_select'] = str[:select_position]
    current_position = select_position + 6 + 1

    from_position = str.find('FROM', current_position, len(str))
    str_json['select'] = str[current_position:from_position].replace(' ', '')
    current_position = from_position + 4 + 1

    # filter predicate pushing
    if '(' in str[current_position:] and 'WHERE' in str[current_position:]:
        next_begin_parenthetical = str.find('(', current_position, len(str))
        next_where = str.find('WHERE', current_position, len(str))
        if next_begin_parenthetical < next_where:
            count_parentheses = 1
            count_round = 0
            for word in str[next_begin_parenthetical + 1:]:
                count_round += 1
                if word == '(':
                    count_parentheses += 1
                if word == ')':
                    count_parentheses -= 1
                if count_parentheses == 0:
                    str_json['from'] = str[current_position:next_begin_parenthetical].replace(' ', '')
                    str_json['temp_table'] = str[next_begin_parenthetical:next_begin_parenthetical + count_round + 1]
                    if 'DISTINCT' in str_json['temp_table']:
                        str_json['temp_table'] = str_json['temp_table'].replace('DISTINCT', '')
                    current_position = next_begin_parenthetical + count_round + 1
                    break

    if 'WHERE' in str[current_position:]:
        where_position = str.find('WHERE', current_position, len(str))
        if 'from' not in str_json:
            str_json['from'] = str[current_position:where_position].replace(' ', '')
        current_position = where_position + 5 + 1

        str_json['where'] = []
        if 'AND' in str[current_position:]:
            and_position = str.find('AND', current_position, len(str))
            str_json['where'].append(str[current_position:and_position].replace(' ', ''))
            current_position = and_position + 3 + 1

            if 'AND' in str[current_position:]:
                and_position = str.find('AND', current_position, len(str))
                str_json['where'].append(str[current_position:and_position].replace(' ', ''))
                current_position = and_position + 3 + 1

                while True:
                    if 'AND' in str[current_position:]:
                        and_position = str.find('AND', current_position, len(str))
                        str_json['where'].append(str[current_position:and_position].replace(' ', ''))
                        current_position = and_position + 3 + 1
                    else:
                        str_json['where'].append(str[current_position:])
                        break
            else:
                str_json['where'].append(str[current_position:].replace(' ', ''))
        else:
            str_json['where'].append(str[current_position:].replace(' ', ''))
        where_list = []
        for where in str_json['where']:
            if ')' in where and '(' not in where:
                where = where.replace(')', '')
            if 'OR' in where:
                where = where.replace('OR', ' OR ')
                if 'like' in where:
                    where = where.replace('like', ' like ')
            where_list.append(where)
        str_json['where'] = where_list
    else:
        str_json['from'] = str[current_position:].replace(' ', '')

    return str_json


def concatenation_json(str_json):
    full_str = ''
    if 'select' in str_json:
        full_str += 'SELECT ' + str_json['select'] + '\n'
        if 'from' in str_json:
            full_str += ' FROM ' + str_json['from'] + '\n'

            if 'temp_table' in str_json:
                full_str += ' (' + concatenation_json(str_json['temp_table']) + ') AS V ' + '\n'

            if 'where' in str_json:
                pop = str_json['where'].pop(0)
                full_str += ' WHERE ' + pop + '\n'
                for condition in str_json['where']:
                    full_str += ' AND ' + condition
    return full_str.replace('  ', ' ')


def rawQuery(request):
    inputStr = request.POST['textInput']
    inputStr = inputStr.replace(';', '')
    str_json = new_split(inputStr)
    if 'temp_table' in str_json:
        str_json['temp_table'] = str_json['temp_table'].replace('\t', '')
        str_json_temp_table = new_split(str_json['temp_table'])
        str_json['temp_table'] = str_json_temp_table
    str_json2 = new_split(inputStr)
    if 'temp_table' in str_json2:
        str_json2['temp_table'] = str_json2['temp_table'].replace('\t', '')
        str_json_temp_table2 = new_split(str_json2['temp_table'])
        str_json2['temp_table'] = str_json_temp_table2
    costs = -1
    conn = psycopg2.connect(
        host='localhost',
        database='habr',
        # database='dvdrental',
        user='postgres',
        password='2540',
    )
    # if 'EXPLAIN (ANALYZE ON)' in inputStr or 'EXPLAIN (ANALYZE TRUE)' in inputStr or 'EXPLAIN (COSTS ON)' in inputStr or 'EXPLAIN (COSTS TRUE)' in inputStr:
    if 'pre_select' in str_json:
        cur = conn.cursor()
        c = 'EXPLAIN (COSTS TRUE) ' + concatenation_json(str_json2)
        cur.execute(c)
        explain = cur.fetchall()
        costs = getCost(explain)
    else:
        cur = conn.cursor()
        c = 'EXPLAIN (COSTS TRUE) ' + concatenation_json(str_json2)
        cur.execute(c)
        explain = cur.fetchall()
        costs = getCost(explain)
    strRaw = inputStr
    split = getSplit(inputStr)
    # for feature 2 check
    cur.execute('select schemaname as schema_name, matviewname as view_name, matviewowner as owner, ispopulated as '
                'is_populated, definition from pg_matviews order by schema_name, view_name;')
    mat_lists = cur.fetchall()
    materializer_bool = False
    for mat_list in mat_lists:
        split2 = getSplit(mat_list[4])
        slFrom = split.fromRaw.replace(' ', '')
        slFrom2 = split2.fromRaw.replace(' ', '')
        ratio_form = fuzz.ratio(slFrom, slFrom2)
        if ratio_form >= 75:
            slWhere = split.whereRaw + split.andRaw
            slWhere2 = split2.whereRaw.lstrip(' ')
            ratio_where = fuzz.ratio(slWhere, slWhere2)
            if ratio_where >= 75:
                # print('where in => ', ratio_where)
                splitSelectRaw = split.selectRaw
                while splitSelectRaw.find(".") != -1:
                    first_index = splitSelectRaw.find("\"")
                    last_index = splitSelectRaw.find(".")
                    splitSelectRaw = splitSelectRaw[:first_index] + splitSelectRaw[last_index + 1:]

                mRaw = splitSelectRaw + 'FROM ' + mat_list[1]
                cur.execute('EXPLAIN (COSTS TRUE) ' + mRaw)
                explainTemp = cur.fetchall()
                costsTemp = getCost(explainTemp)
                if materializer_bool:
                    strRaw += mRaw + '\n' + 'Costs = ' + str(costsTemp) + '\n\n'
                else:
                    strRaw = mRaw + '\n' + 'Costs = ' + str(costsTemp) + '\n\n'
                    materializer_bool = True

    # for feature 2 check
    cur.execute(inputStr)
    queryOutput = cur.fetchall()

    if split.selectRaw != '' and split.fromRaw != '' and split.whereRaw != '' and not materializer_bool:
        if 'OR' in split.whereRaw:
            temp = split.whereRaw.split('(')
            temp1 = temp[1].split(')')
            inWheres = temp1[0].split(' OR ')
            i = 0
            raws = []
            for inWhere in inWheres:
                raws.append(split.selectRaw + '\n' + split.fromRaw +
                            '\n' + 'WHERE ' + inWhere + '\n')
                if split.andRaw is not '':
                    raws[i] = raws[i] + split.andRaw + '\n'
                i = i + 1
            strRaw = ''
            for raw in raws:
                if raw is raws[-1]:
                    strRaw = strRaw + raw
                else:
                    strRaw = strRaw + raw + 'UNION ALL\n'
            cur.execute('EXPLAIN (COSTS TRUE) ' + strRaw)
            explainTemp = cur.fetchall()
            costsTemp = getCost(explainTemp)
            cur.close()
            conn.close()
            return render(request, 'engine/index.html',
                          {'raw': strRaw + 'Costs = ' + str(costsTemp), 'input': inputStr, 'table': queryOutput,
                           'costs': costs})
        else:
            # Predicate pushing
            max_ratio = -1
            where_index = 0
            for condition in str_json['where']:
                ratio = fuzz.ratio(condition, str_json_temp_table['select'])
                if max_ratio < ratio:
                    max_ratio = ratio
                    where_index = str_json['where'].index(condition)
            where_pop = str_json['where'].pop(where_index)

            where_pop_split = where_pop.split('=')
            where_pop_split[0] = where_pop_split[0].replace(' ', '')
            where_pop_split[1] = where_pop_split[1].replace(' ', '')
            if 'V' in where_pop_split[0]:
                where_pop_split[0] = str_json_temp_table['select']
                where_pop_1 = where_pop_split[1]
                point_index = where_pop_1.find('.')
                str_json_temp_table['from'] += ',' + where_pop_1[:point_index]
            else:
                where_pop_split[1] = str_json_temp_table['select']
                where_pop_0 = where_pop_split[0]
                point_index = where_pop_0.find('.')
                str_json_temp_table['from'] += ',' + where_pop_0[:point_index]
            str_json_temp_table['where'].append(where_pop_split[0] + ' = ' + where_pop_split[1])
            str_json['temp_table'] = str_json_temp_table
            new_str = concatenation_json(str_json)
            cur.execute('EXPLAIN (COSTS TRUE) ' + new_str)
            explainTemp = cur.fetchall()
            costsTemp = getCost(explainTemp)
            cur.close()
            conn.close()
            return render(request, 'engine/index.html',
                          {'raw': new_str + '\nCosts = ' + str(costsTemp), 'input': inputStr, 'table': queryOutput,
                           'costs': costs})
    else:
        cur.close()
        conn.close()
        return render(request, 'engine/index.html',
                      {'raw': strRaw, 'input': inputStr, 'table': queryOutput, 'costs': costs})

# def rawQuery2(request):
#     str = request.POST['textInput']
#     split = getSplit(str)
#
#     if split.selectRaw is not '' and split.fromRaw is not '' and split.whereRaw is not '':
#         conn = psycopg2.connect(
#             host='localhost',
#             database='habr',
#             user='postgres',
#             password='2540',
#         )
#         cur = conn.cursor()
#         cur.execute('select schemaname as schema_name, matviewname as view_name, matviewowner as owner, ispopulated as '
#                     'is_populated, definition from pg_matviews order by schema_name, view_name;')
#         mat_lists = cur.fetchall()
#         print(mat_lists)
#         result = str
#         for mat_list in mat_lists:
#             split2 = getSplit(mat_list[4])
#             # print('split from : ', split.fromRaw.lstrip(' '), 'aah')
#             if split2.fromRaw.lstrip(' ') in split.fromRaw:
#                 if split2.whereRaw.lstrip(' ') in split.whereRaw:
#                     result = split.selectRaw + 'FROM ' + mat_list[1]
#                     break
#         cur.execute(str)
#         all = cur.fetchall()
#         cur.close()
#         conn.close()
#         # strRaw = 'SELECT fname, dname\nFROM cpe_stu\nWHERE gpa>3.25'
#         return render(request, 'engine/index.html', {'raw': result, 'input': str, 'table': all})
#     else:
#         return render(request, 'engine/index.html', {'raw': 'Don\'t has WHERE.', 'input2': str})
