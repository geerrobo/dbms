from django.shortcuts import render


def index(request):
    return render(request, 'engine/index.html', {})


def rawQuery(request):
    str = request.POST['textInput']
    if 'SELECT' not in str or 'FROM' not in str:
        return render(request, 'engine/index.html', {'raw': 'No input or syntax error.', 'input': str})
    splits = str.split('\n')

    hasSelect = True
    hasFrom = True
    hasWhere = False
    hasAnd = False
    for split in splits:
        if 'SELECT' in split:
            strSelect = split

        if 'FROM' in split:
            strFrom = split

        if 'WHERE' in split:
            strWhere = split
            hasWhere = True

        if 'AND' in split:
            strAnd = split
            hasAnd = True

    if hasSelect and hasFrom and hasWhere:
        if 'OR' in strWhere:
            print('HAS OR')
            temp = strWhere.split('(')
            temp1 = temp[1].split(')')
            inWheres = temp1[0].split(' OR ')
            i = 0
            raws = []
            for inWhere in inWheres:
                raws.append(strSelect + '\n' + strFrom +
                            '\n' + 'WHERE ' + inWhere + '\n')
                if hasAnd:
                    raws[i] = raws[i] + strAnd + '\n'
                i = i+1
            strRaw = ''
            for raw in raws:
                if raw is raws[-1]:
                    strRaw = strRaw + raw
                else:
                    strRaw = strRaw + raw + 'UNION ALL\n'
            # print(strSelect)
            return render(request, 'engine/index.html', {'raw': strRaw, 'input': str})
        else:
            print('no or')
            return render(request, 'engine/index.html', {'raw': request.POST['textInput'], 'input': str})
    else:
        return render(request, 'engine/index.html', {'raw': 'Don\'t has WHERE.', 'input': str})
